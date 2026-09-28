import Foundation
import GRDB
import Testing

@testable import ElectricSync

/// OTTO-5318: legacy-bootstrap admission read sync state synchronously on the
/// client actor. With every GRDB reader checked out, that read parked a Swift
/// concurrency thread on GRDB's pool semaphore.
struct LegacyBootstrapAdmissionReadTests {
  private static let identity = ElectricReplicaIdentity(
    modelType: ReplicaTestRecord.self,
    modelIdentifier: ReplicaTestRecord.collectionIdentifier,
    basePredicate: nil
  )

  @Test
  func admissionSuspendsWhileEveryReaderIsCheckedOut() async throws {
    let identity = Self.identity
    let databaseURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("electric-admission-\(UUID().uuidString).sqlite")
    defer {
      for suffix in ["", "-wal", "-shm"] {
        try? FileManager.default.removeItem(atPath: databaseURL.path + suffix)
      }
    }
    var configuration = Configuration()
    configuration.maximumReaderCount = 1
    let database = try DatabasePool(path: databaseURL.path, configuration: configuration)
    let metadata = try PooledSyncStateMetadataProvider(database: database)
    // Legacy evidence without an exact cursor: admission must take the slot.
    try metadata.updateSyncState(
      collectionId: identity.legacyPersistedCursorKey(syncMode: .eager),
      state: .resumable(offset: "legacy-offset"),
      transaction: nil
    )
    let controller = ElectricLegacyBootstrapAdmissionController(enabled: true)
    let client = makeClient(metadata: metadata, controller: controller)

    let heldReader = await HeldReader.checkOut(from: database, releasingAfter: .seconds(8))
    let admission = Task {
      try await client.withLegacyBootstrapAdmission(
        identity: identity,
        stage: "reader_exhaustion_test",
        syncMode: .eager
      ) {
        try await metadata.publish(
          collectionId: identity.persistedCursorKey,
          state: .resumable(offset: "exact-offset")
        )
        return "published"
      }
    }
    try await waitUntil { metadata.readCounts.started > 0 }

    // A read that blocks the actor's thread holds this call until the held
    // reader times out; a suspended read leaves the actor free to serve it.
    let probeStart = ContinuousClock.now
    let probe = try await client.withLegacyBootstrapAdmission(
      identity: nil,
      stage: "actor_probe"
    ) { "probe" }
    let probeDuration = ContinuousClock.now - probeStart
    #expect(probe == "probe")
    #expect(probeDuration < .seconds(4))
    #expect(metadata.readCounts.completed == 0)

    heldReader.release()
    #expect(try await admission.value == "published")
    let metrics = await controller.metricsSnapshot()
    #expect(metrics.completed == 1)
    #expect(metrics.exactCursorAdvanced == 1)
    #expect(await controller.state().inFlight == 0)
    #expect(metadata.readCounts.blocking == 0)
  }

  @Test
  func admissionReleasesTheSlotWhenTheRecheckFails() async throws {
    let identity = Self.identity
    let metadata = RecheckFailingMetadataProvider(
      states: [identity.legacyPersistedCursorKey(syncMode: .eager): .resumable(offset: "legacy")],
      failingKey: identity.persistedCursorKey
    )
    let controller = ElectricLegacyBootstrapAdmissionController(enabled: true)
    let client = makeClient(metadata: metadata, controller: controller)

    await #expect(throws: RecheckFailure.self) {
      try await client.withLegacyBootstrapAdmission(
        identity: identity,
        stage: "recheck_failure_test",
        syncMode: .eager
      ) { "unreachable" }
    }

    #expect(
      await controller.state()
        == ElectricLegacyBootstrapAdmissionState(isEnabled: true, queued: 0, inFlight: 0)
    )
    let metrics = await controller.metricsSnapshot()
    #expect(metrics.admitted == 1)
    #expect(metrics.failed == 1)
  }

  private func makeClient(
    metadata: MetadataProvider,
    controller: ElectricLegacyBootstrapAdmissionController
  ) -> ElectricSyncClientImpl {
    ElectricSyncClientImpl(
      configuration: ElectricSyncClientConfiguration(
        metadataProvider: metadata,
        httpClient: UnusedHTTPClientProvider(),
        isExactCursorCutoverEnabled: true,
        legacyBootstrapAdmissionController: controller
      )
    )
  }
}

extension SyncState {
  fileprivate static func resumable(offset: String) -> SyncState {
    SyncState(
      offset: offset,
      handle: "handle",
      cursor: "cursor",
      isUpToDate: true,
      lastSyncedAt: nil
    )
  }
}

/// Stores sync state in GRDB the way a host app does: a synchronous read takes
/// a pooled reader on the calling thread, while the async read waits for one on
/// GRDB's queue through `asyncRead`.
private final class PooledSyncStateMetadataProvider: MetadataProvider, @unchecked Sendable {
  struct ReadCounts {
    var started = 0
    var completed = 0
    var blocking = 0
  }

  private let database: DatabasePool
  private let lock = NSLock()
  private var counts = ReadCounts()

  init(database: DatabasePool) throws {
    self.database = database
    try database.write { db in
      try db.create(table: "sync_state") { table in
        table.column("id", .text).primaryKey()
        table.column("stateOffset", .text)
        table.column("handle", .text)
        table.column("cursor", .text)
        table.column("isUpToDate", .boolean).notNull()
      }
    }
  }

  var readCounts: ReadCounts {
    lock.withLock { counts }
  }

  func getSyncState(collectionId: String, transaction: Any?) throws -> SyncState? {
    if let db = transaction as? Database {
      return try Self.syncState(collectionId: collectionId, db: db)
    }
    lock.withLock {
      counts.started += 1
      counts.blocking += 1
    }
    defer { lock.withLock { counts.completed += 1 } }
    return try database.read { db in
      try Self.syncState(collectionId: collectionId, db: db)
    }
  }

  func getSyncState(collectionId: String) async throws -> SyncState? {
    lock.withLock { counts.started += 1 }
    defer { lock.withLock { counts.completed += 1 } }
    return try await withCheckedThrowingContinuation { continuation in
      database.asyncRead { result in
        continuation.resume(
          with: Result { try Self.syncState(collectionId: collectionId, db: result.get()) }
        )
      }
    }
  }

  func updateSyncState(collectionId: String, state: SyncState, transaction: Any?) throws {
    if let db = transaction as? Database {
      try Self.save(state, collectionId: collectionId, db: db)
      return
    }
    try database.write { db in
      try Self.save(state, collectionId: collectionId, db: db)
    }
  }

  func publish(collectionId: String, state: SyncState) async throws {
    try await database.write { db in
      try Self.save(state, collectionId: collectionId, db: db)
    }
  }

  private static func syncState(collectionId: String, db: Database) throws -> SyncState? {
    try Row.fetchOne(
      db,
      sql: "SELECT * FROM sync_state WHERE id = ?",
      arguments: [collectionId]
    ).map { row in
      SyncState(
        offset: row["stateOffset"],
        handle: row["handle"],
        cursor: row["cursor"],
        isUpToDate: row["isUpToDate"],
        lastSyncedAt: nil
      )
    }
  }

  private static func save(_ state: SyncState, collectionId: String, db: Database) throws {
    try db.execute(
      sql: """
        INSERT OR REPLACE INTO sync_state (id, stateOffset, handle, cursor, isUpToDate)
        VALUES (?, ?, ?, ?, ?)
        """,
      arguments: [collectionId, state.offset, state.handle, state.cursor, state.isUpToDate]
    )
  }

  func hasFetched(table _: String, predicate _: PredicateHash, transaction _: Any?) throws -> Bool {
    false
  }

  func getFetchedPredicates(table _: String, transaction _: Any?) throws -> [FetchedPredicate] {
    []
  }

  func recordFetch(
    table _: String,
    predicate _: PredicateHash,
    predicateJSON _: String?,
    snapshotBoundary _: PostgresSnapshot?,
    outcome _: SubsetObservationOutcome,
    isComplete _: Bool,
    transaction _: Any?
  ) throws {}

  func getFetchedRanges(table _: String, orderField _: String, transaction _: Any?) throws
    -> [FetchedRange]
  {
    []
  }

  func recordRange(
    table _: String,
    orderField _: String,
    range _: FetchedRange,
    transaction _: Any?
  ) throws {}

  func clearMetadata(table _: String, transaction _: Any?) throws {}
}

private struct RecheckFailure: Error {}

/// Serves fixed states and fails the second read of `failingKey`: the first is
/// the admission check, the second the re-check after the slot is acquired.
private final class RecheckFailingMetadataProvider: MetadataProvider, @unchecked Sendable {
  private let states: [String: SyncState]
  private let failingKey: String
  private let lock = NSLock()
  private var failingKeyReads = 0

  init(states: [String: SyncState], failingKey: String) {
    self.states = states
    self.failingKey = failingKey
  }

  private func read(collectionId: String) throws -> SyncState? {
    try lock.withLock {
      if collectionId == failingKey {
        failingKeyReads += 1
        if failingKeyReads == 2 { throw RecheckFailure() }
      }
      return states[collectionId]
    }
  }

  func getSyncState(collectionId: String, transaction _: Any?) throws -> SyncState? {
    try read(collectionId: collectionId)
  }

  func getSyncState(collectionId: String) async throws -> SyncState? {
    try read(collectionId: collectionId)
  }

  func updateSyncState(collectionId _: String, state _: SyncState, transaction _: Any?) throws {}

  func hasFetched(table _: String, predicate _: PredicateHash, transaction _: Any?) throws -> Bool {
    false
  }

  func getFetchedPredicates(table _: String, transaction _: Any?) throws -> [FetchedPredicate] {
    []
  }

  func recordFetch(
    table _: String,
    predicate _: PredicateHash,
    predicateJSON _: String?,
    snapshotBoundary _: PostgresSnapshot?,
    outcome _: SubsetObservationOutcome,
    isComplete _: Bool,
    transaction _: Any?
  ) throws {}

  func getFetchedRanges(table _: String, orderField _: String, transaction _: Any?) throws
    -> [FetchedRange]
  {
    []
  }

  func recordRange(
    table _: String,
    orderField _: String,
    range _: FetchedRange,
    transaction _: Any?
  ) throws {}

  func clearMetadata(table _: String, transaction _: Any?) throws {}
}

/// Checks out a pooled reader on GRDB's reader queue, never on a Swift
/// concurrency thread, and returns it on `release()` or after the limit, so a
/// regression fails by time instead of hanging the suite.
private final class HeldReader: @unchecked Sendable {
  private let released = DispatchSemaphore(value: 0)

  static func checkOut(
    from database: DatabasePool,
    releasingAfter limit: DispatchTimeInterval
  ) async -> HeldReader {
    let reader = HeldReader()
    await withCheckedContinuation { (checkedOut: CheckedContinuation<Void, Never>) in
      database.asyncRead { _ in
        checkedOut.resume()
        _ = reader.released.wait(timeout: .now() + limit)
      }
    }
    return reader
  }

  func release() {
    released.signal()
  }
}

private struct UnusedHTTPClientProvider: HTTPClientProvider {
  func fetch(_: ElectricShapeRequest) async throws -> [ElectricMessage] {
    Issue.record("Admission tests never fetch")
    return []
  }
}

private func waitUntil(
  timeout: Duration = .seconds(10),
  condition: @escaping @Sendable () -> Bool
) async throws {
  let deadline = ContinuousClock.now + timeout
  while ContinuousClock.now < deadline {
    if condition() { return }
    try await Task.sleep(for: .milliseconds(10))
  }
  Issue.record("Timed out waiting for condition")
}
