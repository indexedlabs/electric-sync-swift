import Foundation
import GRDB
import Testing

@testable import ElectricSync

/// OTTO-5325: metadata calls made outside an owner transaction (stream resume,
/// fetch coverage, batch preflight, legacy adoption, `markFetched`) ran the
/// provider's synchronous requirements on the Swift concurrency pool. With a
/// GRDB pool, each one parked its thread until a reader or the writer was free.
struct MetadataProviderAsyncCallTests {
  private static let identity = ElectricReplicaIdentity(
    modelType: ReplicaTestRecord.self,
    modelIdentifier: ReplicaTestRecord.collectionIdentifier,
    basePredicate: nil
  )

  @Test
  func streamResumeSuspendsWhileEveryReaderIsCheckedOut() async throws {
    let identity = Self.identity
    let store = try TemporaryPool()
    let metadata = try PooledMetadataProvider(database: store.database)
    try metadata.seed(.resumableState(offset: "exact-offset"), at: identity.persistedCursorKey)
    let http = UpToDateHTTPClient()
    let client = makeClient(metadata: metadata, http: http)

    let heldReader = await HeldConnection.reader(of: store.database, releasingAfter: .seconds(8))
    let poll = Task {
      try await client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        syncMode: .eager,
        live: false
      )
    }
    try await waitUntil { metadata.callCounts.readsStarted > 0 }

    // A resume read that blocks the actor's thread holds this call until the
    // reader is released; a suspended read leaves the actor free to serve it.
    #expect(try await probeClientActor(client) < .seconds(4))
    #expect(!heldReader.isReleased)
    #expect(metadata.callCounts.blocking == 0)

    heldReader.release()
    let batch = try #require(try await poll.value)
    #expect(batch.messages.contains { $0.control == .upToDate })
    #expect(http.requestedOffsets == ["exact-offset"])
    #expect(metadata.callCounts.blocking == 0)
  }

  @Test
  func batchPreflightSuspendsWhileEveryReaderIsCheckedOut() async throws {
    let identity = Self.identity
    let store = try TemporaryPool()
    let metadata = try PooledMetadataProvider(database: store.database)
    try metadata.seed(.resumableState(offset: "exact-offset"), at: identity.persistedCursorKey)
    let client = makeClient(metadata: metadata, http: UpToDateHTTPClient())
    let batch = try #require(
      try await client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        syncMode: .eager,
        live: false
      )
    )
    let readsBeforePreflight = metadata.callCounts.readsStarted

    // Preflight runs from the live-stream task, the collection owner and the
    // query coordinator actor, not on the client actor. What a blocking read
    // costs there is a cooperative-pool thread: start more preflights than the
    // pool has threads, then check other async work still runs.
    let preflightCount = ProcessInfo.processInfo.activeProcessorCount * 2
    let heldReader = await HeldConnection.reader(of: store.database, releasingAfter: .seconds(8))
    let preflights = (0..<preflightCount).map { _ in
      Task.detached { try await batch.preflightSupportedEvents() }
    }
    try await waitUntil {
      metadata.callCounts.readsStarted - readsBeforePreflight >= preflightCount
    }

    let probeRanWhileReaderWasHeld = await Task.detached { !heldReader.isReleased }.value
    #expect(probeRanWhileReaderWasHeld)
    #expect(metadata.callCounts.blocking == 0)

    heldReader.release()
    for preflight in preflights {
      try await preflight.value
    }
    #expect(metadata.callCounts.readsStarted - readsBeforePreflight == preflightCount)
    #expect(metadata.callCounts.blocking == 0)
  }

  @Test
  func fetchCoverageSuspendsWhileEveryReaderIsCheckedOut() async throws {
    let store = try TemporaryPool()
    let metadata = try PooledMetadataProvider(database: store.database)
    try metadata.seedFetch(table: ReplicaTestRecord.tableName, predicate: PredicateHash(from: nil))
    let tracker = ElectricFetchTracker(metadataProvider: metadata)

    let heldReader = await HeldConnection.reader(of: store.database, releasingAfter: .seconds(8))
    let plan = Task {
      try await tracker.computeMissing(
        table: ReplicaTestRecord.tableName,
        requested: nil,
        scope: nil,
        orderBy: [],
        limit: nil
      )
    }
    try await waitUntil { metadata.callCounts.readsStarted > 0 }
    #expect(metadata.callCounts.blocking == 0)

    heldReader.release()
    // Only a read of the seeded coverage row can skip the fetch.
    let fetchPlan = try await plan.value
    #expect(!fetchPlan.needsFetch)
    #expect(fetchPlan.reuseExisting)
    #expect(metadata.callCounts.blocking == 0)
  }

  @Test
  func legacyAdoptionSuspendsWhileTheWriterIsBusy() async throws {
    let identity = ElectricReplicaIdentity(
      modelType: LegacyCursorTestRecord.self,
      modelIdentifier: LegacyCursorTestRecord.collectionIdentifier,
      basePredicate: nil
    )
    #expect(!identity.provenLegacyPersistedCursorKeys.isEmpty)
    let store = try TemporaryPool()
    let metadata = try PooledMetadataProvider(database: store.database)
    // No exact cursor and one proven legacy cursor: resume must adopt it.
    try metadata.seed(
      .resumableState(offset: "legacy-offset"),
      at: identity.legacyPersistedCursorKey(syncMode: .eager)
    )
    let http = UpToDateHTTPClient()
    let client = makeClient(metadata: metadata, http: http)

    let heldWriter = await HeldConnection.writer(of: store.database, releasingAfter: .seconds(8))
    let poll = Task {
      try await client.pollStream(
        LegacyCursorTestRecord.self,
        basePredicate: nil,
        syncMode: .eager,
        live: false
      )
    }
    try await waitUntil { metadata.callCounts.writesStarted > 0 }

    #expect(try await probeClientActor(client) < .seconds(4))
    #expect(!heldWriter.isReleased)
    #expect(metadata.callCounts.blocking == 0)

    heldWriter.release()
    _ = try #require(try await poll.value)
    #expect(http.requestedOffsets == ["legacy-offset"])
    let adopted = try metadata.storedSyncState(at: identity.persistedCursorKey)
    #expect(adopted?.offset == "legacy-offset")
    #expect(metadata.callCounts.blocking == 0)
  }

  @Test
  func markFetchedSuspendsWhileTheWriterIsBusy() async throws {
    let store = try TemporaryPool()
    let metadata = try PooledMetadataProvider(database: store.database)
    let client = makeClient(metadata: metadata, http: UpToDateHTTPClient())

    let heldWriter = await HeldConnection.writer(of: store.database, releasingAfter: .seconds(8))
    let mark = Task {
      try await client.markFetched(ReplicaTestRecord.self, where: nil)
    }
    try await waitUntil { metadata.callCounts.writesStarted > 0 }

    #expect(try await probeClientActor(client) < .seconds(4))
    #expect(!heldWriter.isReleased)
    #expect(metadata.callCounts.blocking == 0)

    heldWriter.release()
    try await mark.value
    #expect(
      try metadata.storedFetchHashes(table: ReplicaTestRecord.tableName)
        == [PredicateHash(from: nil).value]
    )
    #expect(metadata.callCounts.blocking == 0)
  }

  /// The ownership read suspends the client actor. A forced bootstrap that
  /// replaces the stream's tracker meanwhile must win: the suspended rebuild
  /// may not re-establish the discarded tracker.
  @Test
  func trackerRebuildRefusesATrackerReplacedDuringItsOwnershipRead() async throws {
    let identity = Self.identity
    let metadata = GatedRebuildMetadataProvider(
      states: [identity.persistedCursorKey: .resumableState(offset: "exact-offset")]
    )
    let client = makeClient(metadata: metadata, http: UpToDateHTTPClient())

    let resumed = Task {
      try await client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        shapeTopology: .staticallySimple,
        syncMode: .eager,
        live: false
      )
    }
    try await waitUntil { metadata.isHoldingFirstRebuildRead }

    let forced = try #require(
      try await client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        shapeTopology: .staticallySimple,
        syncMode: .eager,
        live: false,
        forceFullBootstrap: true
      )
    )
    metadata.openFirstRebuildRead()
    let resumedBatch = try #require(try await resumed.value)

    #expect(resumedBatch.moveOutTracker !== forced.moveOutTracker)
    #expect(!resumedBatch.moveOutTracker.isContinuityEstablished)
    #expect(!forced.moveOutTracker.isContinuityEstablished)

    // Positive control: without a replacement, the same provider admits the
    // rebuild into the stream's current tracker.
    let later = try #require(
      try await client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        shapeTopology: .staticallySimple,
        syncMode: .eager,
        live: false
      )
    )
    #expect(later.moveOutTracker === forced.moveOutTracker)
    #expect(later.moveOutTracker.isContinuityEstablished)
  }

  private func makeClient(
    metadata: MetadataProvider,
    http: HTTPClientProvider
  ) -> ElectricSyncClientImpl {
    ElectricSyncClientImpl(
      configuration: ElectricSyncClientConfiguration(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true
      )
    )
  }

  /// A call that needs nothing but the client actor: it answers at once unless
  /// a synchronous provider call is holding the actor's thread.
  private func probeClientActor(_ client: ElectricSyncClientImpl) async throws -> Duration {
    let start = ContinuousClock.now
    let probe = try await client.withLegacyBootstrapAdmission(
      identity: nil,
      stage: "actor_probe"
    ) { "probe" }
    #expect(probe == "probe")
    return ContinuousClock.now - start
  }
}

private struct LegacyCursorTestRecord: ReplicaLifecycleTestModel {
  let id: String
  let name: String

  static var tableName: String { "legacy_cursor_async_test_records" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(
      endpoint: "/shapes/\(tableName)",
      selectedColumns: ["id", "name"],
      legacyCursorVersion: "1"
    )
  }
}

extension SyncState {
  fileprivate static func resumableState(offset: String) -> SyncState {
    SyncState(
      offset: offset,
      handle: "handle",
      cursor: "cursor",
      isUpToDate: true,
      lastSyncedAt: nil
    )
  }
}

private final class TemporaryPool: Sendable {
  let database: DatabasePool
  private let url: URL

  init() throws {
    url = FileManager.default.temporaryDirectory
      .appendingPathComponent("electric-async-metadata-\(UUID().uuidString).sqlite")
    var configuration = Configuration()
    configuration.maximumReaderCount = 1
    database = try DatabasePool(path: url.path, configuration: configuration)
  }

  deinit {
    for suffix in ["", "-wal", "-shm"] {
      try? FileManager.default.removeItem(atPath: url.path + suffix)
    }
  }
}

/// Stores metadata in GRDB the way a host app does. A synchronous call without
/// a transaction takes a pooled reader or the writer on the calling thread and
/// counts as blocking; the async requirements wait for one on GRDB's queues
/// through `asyncRead` and `asyncWrite`.
private final class PooledMetadataProvider: MetadataProvider, @unchecked Sendable {
  struct CallCounts {
    var readsStarted = 0
    var writesStarted = 0
    var blocking = 0
  }

  private let database: DatabasePool
  private let lock = NSLock()
  private var counts = CallCounts()

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
      try db.create(table: "fetched_predicate") { table in
        table.column("tableName", .text).notNull()
        table.column("predicateHash", .text).notNull()
        table.column("predicateJSON", .text)
        table.primaryKey(["tableName", "predicateHash"])
      }
    }
  }

  var callCounts: CallCounts {
    lock.withLock { counts }
  }

  // MARK: Test setup and inspection (never counted)

  func seed(_ state: SyncState, at collectionId: String) throws {
    try database.write { try Self.save(state, collectionId: collectionId, db: $0) }
  }

  func seedFetch(table: String, predicate: PredicateHash) throws {
    try database.write {
      try Self.saveFetch(table: table, predicate: predicate, predicateJSON: nil, db: $0)
    }
  }

  func storedSyncState(at collectionId: String) throws -> SyncState? {
    try database.read { try Self.syncState(collectionId: collectionId, db: $0) }
  }

  func storedFetchHashes(table: String) throws -> [String] {
    try database.read { db in
      try Self.fetchedPredicates(table: table, db: db).map(\.predicateHash.value)
    }
  }

  // MARK: Synchronous requirements

  func getSyncState(collectionId: String, transaction: Any?) throws -> SyncState? {
    try read(transaction) { try Self.syncState(collectionId: collectionId, db: $0) }
  }

  func updateSyncState(collectionId: String, state: SyncState, transaction: Any?) throws {
    try write(transaction) { try Self.save(state, collectionId: collectionId, db: $0) }
  }

  func adoptSyncState(
    collectionId: String,
    legacyCollectionIds: [String],
    transaction: Any?
  ) throws -> SyncState? {
    try write(transaction) {
      try Self.adopt(collectionId: collectionId, legacyCollectionIds: legacyCollectionIds, db: $0)
    }
  }

  func hasFetched(table: String, predicate: PredicateHash, transaction: Any?) throws -> Bool {
    try read(transaction) { try Self.hasFetched(table: table, predicate: predicate, db: $0) }
  }

  func getFetchedPredicates(table: String, transaction: Any?) throws -> [FetchedPredicate] {
    try read(transaction) { try Self.fetchedPredicates(table: table, db: $0) }
  }

  func recordFetch(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary _: PostgresSnapshot?,
    outcome _: SubsetObservationOutcome,
    isComplete _: Bool,
    transaction: Any?
  ) throws {
    try write(transaction) {
      try Self.saveFetch(table: table, predicate: predicate, predicateJSON: predicateJSON, db: $0)
    }
  }

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

  // MARK: Async requirements

  func getSyncState(collectionId: String) async throws -> SyncState? {
    try await asyncRead { try Self.syncState(collectionId: collectionId, db: $0) }
  }

  func adoptSyncState(collectionId: String, legacyCollectionIds: [String]) async throws
    -> SyncState?
  {
    try await asyncWrite {
      try Self.adopt(collectionId: collectionId, legacyCollectionIds: legacyCollectionIds, db: $0)
    }
  }

  func hasFetched(table: String, predicate: PredicateHash) async throws -> Bool {
    try await asyncRead { try Self.hasFetched(table: table, predicate: predicate, db: $0) }
  }

  func getFetchedPredicates(table: String) async throws -> [FetchedPredicate] {
    try await asyncRead { try Self.fetchedPredicates(table: table, db: $0) }
  }

  func recordFetch(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary _: PostgresSnapshot?,
    outcome _: SubsetObservationOutcome,
    isComplete _: Bool
  ) async throws {
    try await asyncWrite {
      try Self.saveFetch(table: table, predicate: predicate, predicateJSON: predicateJSON, db: $0)
    }
  }

  // MARK: Connection access

  private func read<T>(_ transaction: Any?, _ body: (Database) throws -> T) throws -> T {
    if let db = transaction as? Database { return try body(db) }
    lock.withLock {
      counts.readsStarted += 1
      counts.blocking += 1
    }
    return try database.read(body)
  }

  private func write<T>(_ transaction: Any?, _ body: (Database) throws -> T) throws -> T {
    if let db = transaction as? Database { return try body(db) }
    lock.withLock {
      counts.writesStarted += 1
      counts.blocking += 1
    }
    return try database.write(body)
  }

  private func asyncRead<T: Sendable>(
    _ body: @escaping @Sendable (Database) throws -> T
  ) async throws -> T {
    lock.withLock { counts.readsStarted += 1 }
    return try await withCheckedThrowingContinuation { continuation in
      database.asyncRead { result in
        continuation.resume(with: Result { try body(result.get()) })
      }
    }
  }

  private func asyncWrite<T: Sendable>(
    _ body: @escaping @Sendable (Database) throws -> T
  ) async throws -> T {
    lock.withLock { counts.writesStarted += 1 }
    return try await withCheckedThrowingContinuation { continuation in
      database.asyncWrite(body) { _, result in
        continuation.resume(with: result)
      }
    }
  }

  // MARK: Storage

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

  private static func adopt(
    collectionId: String,
    legacyCollectionIds: [String],
    db: Database
  ) throws -> SyncState? {
    if let current = try syncState(collectionId: collectionId, db: db) {
      return current
    }
    let legacyStates = try legacyCollectionIds.compactMap {
      try syncState(collectionId: $0, db: db)
    }
    guard let first = legacyStates.first, first.canResumeWithoutFullBootstrap else { return nil }
    guard legacyStates.dropFirst().allSatisfy({ $0.hasSameResumeIdentity(as: first) }) else {
      return nil
    }
    try save(first, collectionId: collectionId, db: db)
    return first
  }

  private static func hasFetched(
    table: String,
    predicate: PredicateHash,
    db: Database
  ) throws -> Bool {
    try Bool.fetchOne(
      db,
      sql: "SELECT 1 FROM fetched_predicate WHERE tableName = ? AND predicateHash = ?",
      arguments: [table, predicate.value]
    ) ?? false
  }

  private static func fetchedPredicates(table: String, db: Database) throws -> [FetchedPredicate] {
    try Row.fetchAll(
      db,
      sql: "SELECT * FROM fetched_predicate WHERE tableName = ? ORDER BY predicateHash",
      arguments: [table]
    ).map { row in
      FetchedPredicate(
        predicateHash: PredicateHash(value: row["predicateHash"]),
        predicateJSON: row["predicateJSON"],
        snapshotBoundary: nil,
        outcome: .present,
        isComplete: true,
        fetchedAt: Date()
      )
    }
  }

  private static func saveFetch(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    db: Database
  ) throws {
    try db.execute(
      sql: """
        INSERT OR REPLACE INTO fetched_predicate (tableName, predicateHash, predicateJSON)
        VALUES (?, ?, ?)
        """,
      arguments: [table, predicate.value, predicateJSON]
    )
  }
}

/// Serves fixed sync state, claims durable row ownership, and holds the first
/// tracker-rebuild ownership read until the test opens it.
private final class GatedRebuildMetadataProvider: MetadataProvider, @unchecked Sendable {
  let supportsDurableRowOwnership = true

  private let lock = NSLock()
  private var states: [String: SyncState]
  private var rebuildReads = 0
  private var firstReadGate: CheckedContinuation<Void, Never>?
  private var isFirstReadOpen = false

  init(states: [String: SyncState]) {
    self.states = states
  }

  var isHoldingFirstRebuildRead: Bool {
    lock.withLock { firstReadGate != nil }
  }

  func openFirstRebuildRead() {
    let gate = lock.withLock {
      isFirstReadOpen = true
      defer { firstReadGate = nil }
      return firstReadGate
    }
    gate?.resume()
  }

  func trackerRebuildOwnership(
    table _: String,
    shapeIdentity _: String,
    localTableOwnership _: ElectricLocalTableOwnership
  ) async throws -> [String: [String]]? {
    let isFirstRead = lock.withLock {
      rebuildReads += 1
      return rebuildReads == 1
    }
    if isFirstRead {
      await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
        let isOpen = lock.withLock {
          if !isFirstReadOpen { firstReadGate = continuation }
          return isFirstReadOpen
        }
        if isOpen { continuation.resume() }
      }
    }
    return [:]
  }

  func trackerRebuildOwnership(
    table _: String,
    shapeIdentity _: String,
    transaction _: Any?
  ) throws -> [String: [String]]? {
    lock.withLock { rebuildReads += 1 }
    return [:]
  }

  func getSyncState(collectionId: String, transaction _: Any?) throws -> SyncState? {
    lock.withLock { states[collectionId] }
  }

  func updateSyncState(collectionId: String, state: SyncState, transaction _: Any?) throws {
    lock.withLock { states[collectionId] = state }
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

/// Answers every shape request with an up-to-date control and records the
/// offset each request resumed from.
private final class UpToDateHTTPClient: HTTPClientProvider, @unchecked Sendable {
  private let lock = NSLock()
  private var offsets: [String?] = []

  var requestedOffsets: [String?] {
    lock.withLock { offsets }
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    lock.withLock { offsets.append(request.offset) }
    return [
      ElectricMessage(
        payload: Data(),
        offset: "100_0",
        handle: "handle",
        isUpToDate: true,
        kind: .snapshot,
        control: .upToDate
      )
    ]
  }
}

/// Holds a pooled reader, or the writer, on GRDB's own queue (never on a Swift
/// concurrency thread) until `release()` or the limit, so a regression fails
/// by time instead of hanging the suite.
private final class HeldConnection: @unchecked Sendable {
  private let released = DispatchSemaphore(value: 0)
  private let lock = NSLock()
  private var didRelease = false

  static func reader(
    of database: DatabasePool,
    releasingAfter limit: DispatchTimeInterval
  ) async -> HeldConnection {
    let held = HeldConnection()
    await withCheckedContinuation { (checkedOut: CheckedContinuation<Void, Never>) in
      database.asyncRead { _ in
        checkedOut.resume()
        held.hold(limit)
      }
    }
    return held
  }

  static func writer(
    of database: DatabasePool,
    releasingAfter limit: DispatchTimeInterval
  ) async -> HeldConnection {
    let held = HeldConnection()
    await withCheckedContinuation { (checkedOut: CheckedContinuation<Void, Never>) in
      database.asyncWriteWithoutTransaction { _ in
        checkedOut.resume()
        held.hold(limit)
      }
    }
    return held
  }

  /// True once the connection is back with GRDB.
  var isReleased: Bool {
    lock.withLock { didRelease }
  }

  func release() {
    released.signal()
  }

  private func hold(_ limit: DispatchTimeInterval) {
    _ = released.wait(timeout: .now() + limit)
    lock.withLock { didRelease = true }
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
