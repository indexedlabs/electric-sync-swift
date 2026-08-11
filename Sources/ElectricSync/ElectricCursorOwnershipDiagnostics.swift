import Foundation

final class ElectricCursorOwnershipDiagnostics: @unchecked Sendable {
  static let shared = ElectricCursorOwnershipDiagnostics()

  static let collisionTypeAttribute = "electric.cursor_collision_type"
  static let ownerCountAttribute = "electric.cursor_owner_count"
  static let persistedCursorKeyAttribute = "electric.persisted_cursor_key"
  static let writerIsOwnerAttribute = "electric.cursor_writer_is_owner"

  private struct Owner: Sendable {
    let clientId: ObjectIdentifier
    let logger: any LogProvider
  }

  private let lock = NSLock()
  private var ownersByCursorKey: [String: [UUID: Owner]] = [:]
  private var ownershipRevisionByCursorKey: [String: UInt64] = [:]
  private var reportedNonOwningWritersByCursorKey: [String: Set<ObjectIdentifier>] = [:]

  func registerOwner(
    persistedCursorKeys: [String],
    clientId: ObjectIdentifier,
    table: String,
    collectionIdentifier: String,
    logger: any LogProvider,
    tracer: any ElectricSyncTracer,
    runtimeProvider: ElectricSyncRuntimeProvider
  ) -> ElectricCursorOwnerRegistrationResult {
    let registrationId = runtimeProvider.makeUUID()
    let uniqueKeys = Self.orderedUnique(persistedCursorKeys)

    lock.lock()
    var collisionKey: String?
    var collisionOwnerCount = 0
    for key in uniqueKeys {
      var owners = ownersByCursorKey[key] ?? [:]
      owners[registrationId] = Owner(clientId: clientId, logger: logger)
      ownersByCursorKey[key] = owners
      advanceOwnershipRevision(for: key)
      if owners.count > 1, owners.count > collisionOwnerCount {
        collisionOwnerCount = owners.count
        collisionKey = key
      }
    }
    lock.unlock()

    let collisionReport: ElectricCursorOwnershipCollisionReport? =
      if let collisionKey {
        ElectricCursorOwnershipCollisionReport(
          spanName: "electric.stream_owner.collision",
          message: "Electric duplicate stream owners detected for persisted cursor",
          attributes: [
            "stage": "cursor_ownership",
            "table": table,
            "collection": collectionIdentifier,
            Self.collisionTypeAttribute: "duplicate_stream_owners",
            Self.ownerCountAttribute: "\(collisionOwnerCount)",
            Self.persistedCursorKeyAttribute: collisionKey,
            "electric.stream_client_id": String(describing: clientId),
          ],
          logger: logger,
          tracer: tracer,
          emissionGate: nil
        )
      } else {
        nil
      }

    return ElectricCursorOwnerRegistrationResult(
      registration: ElectricCursorOwnerRegistration(
        diagnostics: self,
        persistedCursorKeys: uniqueKeys,
        registrationId: registrationId
      ),
      collisionReport: collisionReport
    )
  }

  private static func orderedUnique(_ keys: [String]) -> [String] {
    var seen = Set<String>()
    var result: [String] = []
    for key in keys where seen.insert(key).inserted {
      result.append(key)
    }
    return result
  }

  func cursorWriteCollisionReport(
    persistedCursorKey: String,
    writerClientId: ObjectIdentifier,
    table: String,
    collectionIdentifier: String,
    tracer: any ElectricSyncTracer
  ) -> ElectricCursorOwnershipCollisionReport? {
    lock.lock()
    let owners = ownersByCursorKey[persistedCursorKey] ?? [:]
    let ownerCount = owners.count
    let writerIsOwner = owners.values.contains { $0.clientId == writerClientId }
    let ownerLogger = owners.values.first?.logger
    let ownershipRevision = ownershipRevisionByCursorKey[persistedCursorKey] ?? 0
    lock.unlock()

    guard ownerCount > 0, !writerIsOwner else { return nil }
    guard let ownerLogger else { return nil }

    return ElectricCursorOwnershipCollisionReport(
      spanName: "electric.cursor_writer.collision",
      message: "Electric cursor state written by non-owning client",
      attributes: [
        "stage": "cursor_ownership",
        "table": table,
        "collection": collectionIdentifier,
        Self.collisionTypeAttribute: "non_owning_cursor_writer",
        Self.ownerCountAttribute: "\(ownerCount)",
        Self.persistedCursorKeyAttribute: persistedCursorKey,
        Self.writerIsOwnerAttribute: "false",
        "electric.cursor_writer_client_id": String(describing: writerClientId),
      ],
      logger: ownerLogger,
      tracer: tracer,
      emissionGate: ElectricCursorCollisionEmissionGate(
        diagnostics: self,
        persistedCursorKey: persistedCursorKey,
        writerClientId: writerClientId,
        ownershipRevision: ownershipRevision
      )
    )
  }

  private func advanceOwnershipRevision(for persistedCursorKey: String) {
    ownershipRevisionByCursorKey[persistedCursorKey, default: 0] &+= 1
    reportedNonOwningWritersByCursorKey.removeValue(forKey: persistedCursorKey)
  }

  func ownerCount(persistedCursorKey: String) -> Int {
    lock.withLock {
      ownersByCursorKey[persistedCursorKey]?.count ?? 0
    }
  }

  fileprivate func claimNonOwningWriterEmission(
    persistedCursorKey: String,
    writerClientId: ObjectIdentifier,
    ownershipRevision: UInt64
  ) -> Bool {
    lock.lock()
    defer { lock.unlock() }
    guard ownershipRevisionByCursorKey[persistedCursorKey] == ownershipRevision else {
      return false
    }
    var reportedWriters = reportedNonOwningWritersByCursorKey[persistedCursorKey] ?? []
    guard reportedWriters.insert(writerClientId).inserted else { return false }
    reportedNonOwningWritersByCursorKey[persistedCursorKey] = reportedWriters
    return true
  }

  fileprivate func unregisterOwner(
    persistedCursorKeys: [String],
    registrationId: UUID
  ) {
    lock.lock()
    defer { lock.unlock() }
    for key in persistedCursorKeys {
      guard var owners = ownersByCursorKey[key] else { continue }
      guard owners.removeValue(forKey: registrationId) != nil else { continue }
      advanceOwnershipRevision(for: key)
      if owners.isEmpty {
        ownersByCursorKey.removeValue(forKey: key)
      } else {
        ownersByCursorKey[key] = owners
      }
    }
  }
}

struct ElectricCursorOwnerRegistrationResult: Sendable {
  let registration: ElectricCursorOwnerRegistration
  let collisionReport: ElectricCursorOwnershipCollisionReport?
}

final class ElectricCursorOwnerRegistration: @unchecked Sendable {
  private let lock = NSLock()
  private var diagnostics: ElectricCursorOwnershipDiagnostics?
  private let persistedCursorKeys: [String]
  private let registrationId: UUID

  init(
    diagnostics: ElectricCursorOwnershipDiagnostics,
    persistedCursorKeys: [String],
    registrationId: UUID
  ) {
    self.diagnostics = diagnostics
    self.persistedCursorKeys = persistedCursorKeys
    self.registrationId = registrationId
  }

  deinit {
    cancel()
  }

  func cancel() {
    lock.lock()
    let diagnostics = diagnostics
    self.diagnostics = nil
    lock.unlock()

    diagnostics?.unregisterOwner(
      persistedCursorKeys: persistedCursorKeys,
      registrationId: registrationId
    )
  }
}

struct ElectricCursorOwnershipCollisionReport: Sendable {
  let spanName: String
  let message: String
  let attributes: [String: String]
  let logger: any LogProvider
  let tracer: any ElectricSyncTracer
  let emissionGate: ElectricCursorCollisionEmissionGate?

  func emit() {
    guard emissionGate?.claim() ?? true else { return }
    logger.log(.warning, message: message, metadata: attributes)
    let span = tracer.startSpan(name: spanName, attributes: attributes)
    span.end(status: .failure)
  }
}

struct ElectricCursorCollisionEmissionGate: Sendable {
  let diagnostics: ElectricCursorOwnershipDiagnostics
  let persistedCursorKey: String
  let writerClientId: ObjectIdentifier
  let ownershipRevision: UInt64

  func claim() -> Bool {
    diagnostics.claimNonOwningWriterEmission(
      persistedCursorKey: persistedCursorKey,
      writerClientId: writerClientId,
      ownershipRevision: ownershipRevision
    )
  }
}
