import Foundation

public final class ElectricCollectionStreamToken: @unchecked Sendable {
  private let lock = NSLock()
  private var isCancelled = false
  private let onCancel: @Sendable () -> Void

  init(onCancel: @escaping @Sendable () -> Void) {
    self.onCancel = onCancel
  }

  deinit {
    cancel()
  }

  public func cancel() {
    lock.lock()
    defer { lock.unlock() }
    guard !isCancelled else { return }
    isCancelled = true
    onCancel()
  }
}

public final class ElectricCollectionStreamManager: @unchecked Sendable {
  public struct Key: Hashable, Sendable {
    public let tableName: String
    public let basePredicate: SQLExpression?
    public let syncMode: ElectricCollectionSyncMode
    public let clientId: ObjectIdentifier?

    public init(
      tableName: String,
      basePredicate: SQLExpression? = nil,
      syncMode: ElectricCollectionSyncMode = .onDemand,
      clientId: ObjectIdentifier? = nil
    ) {
      self.tableName = tableName
      self.basePredicate = basePredicate
      self.syncMode = syncMode
      self.clientId = clientId
    }
  }

  private struct Entry {
    var subscriberCount: Int
    var task: Task<Void, Never>
    var gcTask: Task<Void, Never>?
    var cursorOwnerRegistration: ElectricCursorOwnerRegistration?
  }

  public static let shared = ElectricCollectionStreamManager()
  public static let defaultGCTime: TimeInterval = 5.0

  private let gcTime: TimeInterval
  private let runtimeProvider: ElectricSyncRuntimeProvider
  private let cursorOwnershipDiagnostics: ElectricCursorOwnershipDiagnostics
  private let teardownHandlerLock = NSLock()
  private let queue = DispatchQueue(label: "electric-collection-stream-manager")
  private var entries: [Key: Entry] = [:]
  private var teardownRegistrationID: UUID?
  private var teardownUnregister: (@Sendable (UUID) -> Void)?

  deinit {
    teardownHandlerLock.lock()
    let registrationID = teardownRegistrationID
    let unregister = teardownUnregister
    teardownRegistrationID = nil
    teardownUnregister = nil
    teardownHandlerLock.unlock()
    if let registrationID, let unregister {
      unregister(registrationID)
    }
  }

  public init(
    gcTime: TimeInterval = ElectricCollectionStreamManager.defaultGCTime,
    runtimeProvider: ElectricSyncRuntimeProvider = .live
  ) {
    self.gcTime = gcTime
    self.runtimeProvider = runtimeProvider
    self.cursorOwnershipDiagnostics = .shared
  }

  init(
    gcTime: TimeInterval,
    runtimeProvider: ElectricSyncRuntimeProvider = .live,
    cursorOwnershipDiagnostics: ElectricCursorOwnershipDiagnostics
  ) {
    self.gcTime = gcTime
    self.runtimeProvider = runtimeProvider
    self.cursorOwnershipDiagnostics = cursorOwnershipDiagnostics
  }

  public func installTeardownHandler(
    sessionProvider: ElectricSyncSessionProvider
  ) {
    teardownHandlerLock.lock()
    defer { teardownHandlerLock.unlock() }
    guard teardownRegistrationID == nil else { return }
    teardownUnregister = sessionProvider.unregisterTeardownHandler
    teardownRegistrationID = sessionProvider.registerTeardownHandler { [weak self] in
      self?.cancelAll()
    }
  }

  public func acquire(
    key: Key,
    start: @escaping @Sendable () -> Task<Void, Never>
  ) -> ElectricCollectionStreamToken {
    acquire(
      key: key,
      persistedCursorKeys: [],
      collectionIdentifier: nil,
      logger: NoopLogProvider(),
      tracer: NoopElectricSyncTracer(),
      start: start
    )
  }

  func acquire(
    key: Key,
    persistedCursorKeys: [String],
    collectionIdentifier: String?,
    logger: any LogProvider,
    tracer: any ElectricSyncTracer,
    start: @escaping @Sendable () -> Task<Void, Never>
  ) -> ElectricCollectionStreamToken {
    let collisionReport: ElectricCursorOwnershipCollisionReport? = queue.sync {
      if var entry = entries[key] {
        entry.subscriberCount += 1
        entry.gcTask?.cancel()
        entry.gcTask = nil
        entries[key] = entry
        return nil
      } else {
        let registrationResult = makeCursorOwnerRegistration(
          key: key,
          persistedCursorKeys: persistedCursorKeys,
          collectionIdentifier: collectionIdentifier,
          logger: logger,
          tracer: tracer
        )
        entries[key] = Entry(
          subscriberCount: 1,
          task: start(),
          gcTask: nil,
          cursorOwnerRegistration: registrationResult?.registration
        )
        return registrationResult?.collisionReport
      }
    }
    collisionReport?.emit()

    return ElectricCollectionStreamToken { [weak self] in
      self?.release(key: key)
    }
  }

  private func makeCursorOwnerRegistration(
    key: Key,
    persistedCursorKeys: [String],
    collectionIdentifier: String?,
    logger: any LogProvider,
    tracer: any ElectricSyncTracer
  ) -> ElectricCursorOwnerRegistrationResult? {
    guard let clientId = key.clientId,
      !persistedCursorKeys.isEmpty,
      let collectionIdentifier
    else {
      return nil
    }

    return cursorOwnershipDiagnostics.registerOwner(
      persistedCursorKeys: persistedCursorKeys,
      clientId: clientId,
      table: key.tableName,
      collectionIdentifier: collectionIdentifier,
      logger: logger,
      tracer: tracer,
      runtimeProvider: runtimeProvider
    )
  }

  private func release(key: Key) {
    queue.sync {
      guard var entry = entries[key] else { return }
      entry.subscriberCount -= 1

      if entry.subscriberCount > 0 {
        entries[key] = entry
        return
      }

      // If gcTime is 0, cancel immediately (useful for tests or strict cleanup).
      if gcTime <= 0 {
        entry.task.cancel()
        entry.gcTask?.cancel()
        entries.removeValue(forKey: key)
        return
      }

      entry.gcTask?.cancel()
      entry.gcTask = Task { [weak self] in
        let delaySeconds = self?.gcTime ?? 0
        try? await self?.runtimeProvider.sleep(for: .seconds(delaySeconds))
        guard !Task.isCancelled else { return }
        await Task.yield()
        self?.cancelIfUnused(key: key)
      }
      entries[key] = entry
    }
  }

  private func cancelIfUnused(key: Key) {
    queue.sync {
      guard let entry = entries[key] else { return }
      guard entry.subscriberCount <= 0 else { return }

      entry.task.cancel()
      entry.gcTask?.cancel()
      entries.removeValue(forKey: key)
    }
  }

  public func cancelAll() {
    queue.sync {
      let activeEntries = Array(entries.values)
      entries.removeAll()
      for entry in activeEntries {
        entry.task.cancel()
        entry.gcTask?.cancel()
      }
    }
  }
}
