import Foundation
import Testing

@testable import ElectricSync

struct SubscribeTruncateTests {
  @Test
  func subscribeEmitsUpToDateOnceAfterCommittedBoundary() async throws {
    let metadataProvider = NoopMetadataProvider()
    let store = ClearCountingStore()
    let commitGate = FirstTransactionCommitGate()
    let eventHandler = RecordingUpToDateEventHandler {
      await commitGate.hasCommittedTransaction()
    }
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [.upToDate(offset: "offset-1")],
        [.snapshot(offset: "offset-2")],
      ]
    )
    let fetchTracker = ElectricFetchTracker(metadataProvider: metadataProvider)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: fetchTracker,
        eventHandler: eventHandler
      )
    )
    let expectedPredicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("scoped"))
    )

    let transactionRunner:
      @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
        try operation(store)
        await commitGate.finishTransaction()
      }

    let collection = ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: TestModel.self,
        syncMode: .onDemand,
        live: false
      ),
      client: client,
      cacheProvider: NoopCacheProvider(),
      transactionRunner: transactionRunner,
      eventHandler: eventHandler,
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      logger: NoopLogProvider()
    )

    let stream = collection.subscribe(
      where: expectedPredicate,
      circuitBreaker: FixedDelayCircuitBreaker(delay: 60)
    )
    let consumer = Task {
      for await _ in stream {}
    }
    defer {
      consumer.cancel()
    }

    await commitGate.waitForFirstApply()
    #expect(await eventHandler.upToDateEvents().isEmpty)

    await commitGate.releaseFirstCommit()
    await eventHandler.waitForUpToDateCount(1)
    await httpClient.waitForRequestCount(3)

    let events = await eventHandler.upToDateEvents()
    #expect(
      events == [
        RecordedUpToDateEvent(
          table: TestModel.tableName,
          predicate: expectedPredicate,
          transactionWasCommitted: true
        )
      ]
    )

    consumer.cancel()
    await consumer.value
  }

  @Test
  func subscribeDoesNotEmitUpToDateWhenBoundaryTransactionFails() async throws {
    let metadataProvider = NoopMetadataProvider()
    let store = ClearCountingStore()
    let transactionRecorder = TransactionAttemptRecorder()
    let eventHandler = RecordingUpToDateEventHandler()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [.upToDate(offset: "offset-failed")]
      ]
    )
    let fetchTracker = ElectricFetchTracker(metadataProvider: metadataProvider)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: fetchTracker,
        eventHandler: eventHandler
      )
    )

    let transactionRunner:
      @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
        try operation(store)
        await transactionRecorder.recordAttempt()
        throw CollectionBoundaryTestError.commitFailed
      }

    let collection = ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: TestModel.self,
        syncMode: .onDemand,
        live: false
      ),
      client: client,
      cacheProvider: NoopCacheProvider(),
      transactionRunner: transactionRunner,
      eventHandler: eventHandler,
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      logger: NoopLogProvider()
    )

    let stream = collection.subscribe(
      circuitBreaker: FixedDelayCircuitBreaker(delay: 0)
    )
    let consumer = Task {
      for await _ in stream {}
    }
    defer {
      consumer.cancel()
    }

    await transactionRecorder.waitForAttemptCount(1)
    await httpClient.waitForRequestCount(2)

    #expect(await transactionRecorder.attemptCount() == 1)
    #expect(await eventHandler.upToDateEvents().isEmpty)

    consumer.cancel()
    await consumer.value
  }

  @Test
  func subscribeDoesNotApplyOrEmitUpToDateForIncompleteBatch() async throws {
    let metadataProvider = NoopMetadataProvider()
    let store = ClearCountingStore()
    let transactionRecorder = TransactionAttemptRecorder()
    let eventHandler = RecordingUpToDateEventHandler()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [.snapshot(offset: "offset-incomplete")]
      ]
    )
    let fetchTracker = ElectricFetchTracker(metadataProvider: metadataProvider)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: fetchTracker,
        eventHandler: eventHandler
      )
    )

    let transactionRunner:
      @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
        try operation(store)
        await transactionRecorder.recordAttempt()
      }

    let collection = ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: TestModel.self,
        syncMode: .onDemand,
        live: false
      ),
      client: client,
      cacheProvider: NoopCacheProvider(),
      transactionRunner: transactionRunner,
      eventHandler: eventHandler,
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      logger: NoopLogProvider()
    )

    let stream = collection.subscribe(
      circuitBreaker: FixedDelayCircuitBreaker(delay: 0)
    )
    let consumer = Task {
      for await _ in stream {}
    }
    defer {
      consumer.cancel()
    }

    await httpClient.waitForRequestCount(2)

    #expect(await transactionRecorder.attemptCount() == 0)
    #expect(await eventHandler.upToDateEvents().isEmpty)

    consumer.cancel()
    await consumer.value
  }

  @Test
  func subscribeEmitsUpToDateForLegacyControlNilBoundary() async throws {
    let metadataProvider = NoopMetadataProvider()
    let store = ClearCountingStore()
    let eventHandler = RecordingUpToDateEventHandler()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [.legacyUpToDate(offset: "legacy-offset-1")],
        [.snapshot(offset: "offset-2")],
      ]
    )
    let fetchTracker = ElectricFetchTracker(metadataProvider: metadataProvider)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: fetchTracker,
        eventHandler: eventHandler
      )
    )
    let collection = ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: TestModel.self,
        syncMode: .onDemand,
        live: false
      ),
      client: client,
      cacheProvider: NoopCacheProvider(),
      transactionRunner: { operation in
        try operation(store)
      },
      eventHandler: eventHandler,
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      logger: NoopLogProvider()
    )

    let stream = collection.subscribe(
      circuitBreaker: FixedDelayCircuitBreaker(delay: 60)
    )
    let consumer = Task {
      for await _ in stream {}
    }
    defer {
      consumer.cancel()
    }

    await httpClient.waitForRequestCount(2)

    #expect(
      await eventHandler.upToDateEvents() == [
        RecordedUpToDateEvent(
          table: TestModel.tableName,
          predicate: nil,
          transactionWasCommitted: false
        )
      ]
    )

    consumer.cancel()
    await consumer.value
  }

  @Test
  func subscribeClearsCacheAndInvokesTruncateHooks() async throws {
    let metadataProvider = NoopMetadataProvider()
    let store = ClearCountingStore()
    let eventHandler = CountingEventHandler()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [.truncate(handle: "next-handle")],
        [.upToDate(offset: "offset-1")],
      ]
    )
    let fetchTracker = ElectricFetchTracker(metadataProvider: metadataProvider)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: fetchTracker,
        eventHandler: eventHandler
      )
    )

    let transactionRunner:
      @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
        try operation(store)
      }

    let configuration = ElectricCollectionConfiguration(
      modelType: TestModel.self,
      syncMode: .onDemand,
      live: false
    )

    let collection = ElectricCollection(
      configuration: configuration,
      client: client,
      cacheProvider: NoopCacheProvider(),
      transactionRunner: transactionRunner,
      eventHandler: eventHandler,
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      logger: NoopLogProvider()
    )

    let breaker = FixedDelayCircuitBreaker(delay: 60)
    let stream = collection.subscribe(circuitBreaker: breaker)
    let consumer = Task {
      for await _ in stream {}
    }

    await httpClient.waitForRequestCount(1)
    await eventHandler.waitForDidTruncateCount(1)

    #expect(store.clearCallCount() == 1)
    #expect(await eventHandler.willTruncateCount() == 1)

    consumer.cancel()
    await consumer.value
  }

  @Test
  func repeatedFencedSubsetQueriesNeverCommitPoisonedEmptyResumeState() async throws {
    let metadata = LoopGuardMetadataProvider()
    let store = LoopGuardStore()
    let commits = LoopGuardCommitRecorder()
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: LoopGuardModel.self,
      basePredicate: nil
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "persisted-offset",
        handle: "persisted-handle",
        cursor: "persisted-cursor",
        isUpToDate: true,
        lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    store.insert("stale-row")

    let sessionController = TestSessionController()
    do {
      let session = try #require(
        sessionController.captureAuthenticatedSession()
      )

      for invocation in 1...2 {
        let subsetRow = "subset-\(invocation)"
        let replacementRow = "replacement-\(invocation)"
        let httpClient = ScriptedHTTPClientProvider(
          responses: [
            loopGuardTaggedBatch(
              rowId: subsetRow,
              offset: "subset-fenced-\(invocation)",
              isSubset: true
            ),
            loopGuardTaggedBatch(
              rowId: replacementRow,
              offset: "owner-reset-\(invocation)"
            ),
            loopGuardTaggedBatch(
              rowId: replacementRow,
              offset: "owner-replacement-\(invocation)"
            ),
            loopGuardTaggedBatch(
              rowId: subsetRow,
              offset: "subset-final-\(invocation)",
              isSubset: true
            ),
          ]
        )
        let client = ElectricSyncClientImpl(
          configuration: .init(
            metadataProvider: metadata,
            httpClient: httpClient,
            isExactCursorCutoverEnabled: true,
            protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
              isTaggedShapeProtocolEnabled: { true }
            )
          )
        )
        let collection = ElectricCollection(
          configuration: ElectricCollectionConfiguration(
            modelType: LoopGuardModel.self,
            syncMode: .progressive,
            live: false
          ),
          client: client,
          cacheProvider: LoopGuardCacheProvider(store: store),
          transactionRunner: { operation in
            try operation(store)
            let state = try metadata.getSyncState(
              collectionId: streamStateKey,
              transaction: nil
            )
            commits.record(rowCount: store.rowCount, state: state)
          }
        )

        let swapsBeforeQuery = store.prepareTruncateSwapCount
        _ = try await collection.query(
          where: SQLExpression("id = '\(subsetRow)'"),
          session: session
        )
        #expect(store.prepareTruncateSwapCount - swapsBeforeQuery == 1)
      }
    }

    #expect(
      commits.snapshots.allSatisfy { snapshot in
        !(snapshot.rowCount == 0 && snapshot.isUpToDate && snapshot.offset != "-1")
      }
    )
  }
}

private final class LoopGuardMetadataProvider: MetadataProvider, @unchecked Sendable {
  private let lock = NSLock()
  private var syncStates: [String: SyncState] = [:]

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

  func getSyncState(collectionId: String, transaction _: Any?) throws -> SyncState? {
    lock.withLock { syncStates[collectionId] }
  }

  func updateSyncState(collectionId: String, state: SyncState, transaction _: Any?) throws {
    lock.withLock { syncStates[collectionId] = state }
  }

  func resetSyncState(collectionId: String, transaction _: Any?) throws {
    lock.withLock { syncStates[collectionId] = .fullBootstrap }
  }
}

private final class LoopGuardStore: @unchecked Sendable {
  private let lock = NSLock()
  private var rowKeys = Set<String>()
  private var truncateSwapCount = 0

  var rowCount: Int {
    lock.withLock { rowKeys.count }
  }

  var prepareTruncateSwapCount: Int {
    lock.withLock { truncateSwapCount }
  }

  func insert(_ rowKey: String) {
    _ = lock.withLock { rowKeys.insert(rowKey) }
  }

  func remove(_ rowKey: String) {
    _ = lock.withLock { rowKeys.remove(rowKey) }
  }

  func removeAll() {
    lock.withLock {
      truncateSwapCount += 1
      rowKeys.removeAll()
    }
  }
}

private final class LoopGuardCommitRecorder: @unchecked Sendable {
  struct Snapshot: Sendable {
    let rowCount: Int
    let offset: String?
    let isUpToDate: Bool
  }

  private let lock = NSLock()
  private var recordedSnapshots: [Snapshot] = []

  var snapshots: [Snapshot] {
    lock.withLock { recordedSnapshots }
  }

  func record(rowCount: Int, state: SyncState?) {
    lock.withLock {
      recordedSnapshots.append(
        Snapshot(
          rowCount: rowCount,
          offset: state?.offset,
          isUpToDate: state?.isUpToDate ?? false
        )
      )
    }
  }
}

private struct LoopGuardCacheProvider: DataCacheProvider {
  let store: LoopGuardStore

  func load<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> [T]
  where T: ElectricCollectionModel {
    guard T.tableName == LoopGuardModel.tableName else { return [] }
    return Array(repeating: LoopGuardModel(), count: store.rowCount) as? [T] ?? []
  }

  func hasData<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> Bool
  where T: ElectricCollectionModel {
    T.tableName == LoopGuardModel.tableName && store.rowCount > 0
  }
}

private struct LoopGuardModel: ElectricCollectionModel {
  static let tableName = "loop_guard"
  static let electricShapeWireIdentity = ElectricShapeWireIdentity(
    endpoint: "/shapes/loop-guard",
    selectedColumns: ["id"]
  )

  static func createShapeRequest(
    where predicate: SQLExpression?,
    orderBy: [OrderBy],
    limit: Int?,
    offset: String?,
    handle: String?,
    cursor: String?,
    live: Bool
  ) -> ElectricShapeRequest {
    ElectricShapeRequest(
      table: tableName,
      predicate: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live
    )
  }

  static func processMessage(_ message: ElectricMessage, transaction: Any?) throws
    -> ProcessedMessage<Self>
  {
    if let key = message.key {
      (transaction as? LoopGuardStore)?.insert(key)
    }
    return ProcessedMessage(
      records: message.key == nil ? [] : [Self()],
      metadata: StoreMetadata(
        offset: message.offset,
        handle: message.handle,
        cursor: message.cursor,
        operation: .insert
      )
    )
  }

  static func truncate(transaction: Any?) throws {
    (transaction as? LoopGuardStore)?.removeAll()
  }

  static func deleteByKey(_ key: String, transaction: Any?) throws {
    (transaction as? LoopGuardStore)?.remove(key)
  }
}

private func loopGuardTaggedBatch(
  rowId: String,
  offset: String,
  isSubset: Bool = false
) -> [ElectricMessage] {
  var messages = [
    ElectricMessage(
      payload: Data("{}".utf8),
      key: rowId,
      offset: offset,
      handle: "handle-\(offset)",
      kind: isSubset ? .snapshot : .mutation,
      tags: ["scope/\(rowId)"],
      isSubsetSnapshot: isSubset
    )
  ]
  if isSubset {
    messages.append(
      ElectricMessage(
        payload: Data(),
        offset: offset,
        handle: "handle-\(offset)",
        kind: .snapshot,
        control: .snapshotEnd,
        isSubsetSnapshot: true
      )
    )
    messages.append(
      ElectricMessage(
        payload: Data(),
        offset: offset,
        handle: "handle-\(offset)",
        kind: .snapshot,
        control: .subsetEnd,
        isSubsetSnapshot: true
      )
    )
  } else {
    messages.append(
      ElectricMessage(
        payload: Data(),
        offset: offset,
        handle: "handle-\(offset)",
        isUpToDate: true,
        kind: .snapshot,
        control: .upToDate
      )
    )
  }
  return messages
}

private struct NoopMetadataProvider: MetadataProvider {
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

  func getSyncState(collectionId _: String, transaction _: Any?) throws -> SyncState? {
    nil
  }

  func updateSyncState(collectionId _: String, state _: SyncState, transaction _: Any?) throws {}
  func resetSyncState(collectionId _: String, transaction _: Any?) throws {}
}

private struct NoopCacheProvider: DataCacheProvider {
  func load<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> [T]
  where T: ElectricCollectionModel {
    []
  }

  func hasData<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> Bool
  where T: ElectricCollectionModel {
    false
  }
}

private struct FixedDelayCircuitBreaker: CircuitBreakerStrategy {
  let delay: TimeInterval

  mutating func preflightDelay(now _: Date) -> TimeInterval? { nil }
  mutating func recordSuccess() {}
  mutating func recordFailure(now _: Date, reason _: String?) -> TimeInterval { delay }
  mutating func reset() {}
}

private actor ScriptedHTTPClientProvider: HTTPClientProvider {
  private var responses: [[ElectricMessage]]
  private var requests: [ElectricShapeRequest] = []
  private var waiting: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

  init(responses: [[ElectricMessage]]) {
    self.responses = responses
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    requests.append(request)

    let requestCount = requests.count
    if !waiting.isEmpty {
      var remaining: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
      for waiter in waiting {
        if requestCount >= waiter.count {
          waiter.continuation.resume()
        } else {
          remaining.append(waiter)
        }
      }
      waiting = remaining
    }

    guard !responses.isEmpty else { return [] }
    return responses.removeFirst()
  }

  func waitForRequestCount(_ count: Int) async {
    if requests.count >= count { return }
    await withCheckedContinuation { continuation in
      waiting.append((count: count, continuation: continuation))
    }
  }
}

private actor CountingEventHandler: ElectricSyncEventHandler {
  private var willCount: Int = 0
  private var didCount: Int = 0
  private var didWaiting: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

  func willReceiveTruncate(table _: String, predicate _: SQLExpression?) async {
    willCount += 1
  }

  func didReceiveTruncate(table _: String, predicate _: SQLExpression?) async {
    didCount += 1
    if !didWaiting.isEmpty {
      var remaining: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
      for waiter in didWaiting {
        if didCount >= waiter.count {
          waiter.continuation.resume()
        } else {
          remaining.append(waiter)
        }
      }
      didWaiting = remaining
    }
  }

  func willTruncateCount() -> Int { willCount }
  func waitForDidTruncateCount(_ count: Int) async {
    if didCount >= count { return }
    await withCheckedContinuation { continuation in
      didWaiting.append((count: count, continuation: continuation))
    }
  }
}

private struct RecordedUpToDateEvent: Equatable, Sendable {
  let table: String
  let predicate: SQLExpression?
  let transactionWasCommitted: Bool
}

private actor RecordingUpToDateEventHandler: ElectricSyncEventHandler {
  private let commitObservation: @Sendable () async -> Bool
  private var events: [RecordedUpToDateEvent] = []
  private var waiting: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

  init(
    commitObservation: @escaping @Sendable () async -> Bool = { false }
  ) {
    self.commitObservation = commitObservation
  }

  func willReceiveTruncate(table _: String, predicate _: SQLExpression?) async {}
  func didReceiveTruncate(table _: String, predicate _: SQLExpression?) async {}

  func didReceiveUpToDate(table: String, predicate: SQLExpression?) async {
    events.append(
      RecordedUpToDateEvent(
        table: table,
        predicate: predicate,
        transactionWasCommitted: await commitObservation()
      )
    )
    if !waiting.isEmpty {
      var remaining: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
      for waiter in waiting {
        if events.count >= waiter.count {
          waiter.continuation.resume()
        } else {
          remaining.append(waiter)
        }
      }
      waiting = remaining
    }
  }

  func upToDateEvents() -> [RecordedUpToDateEvent] {
    events
  }

  func waitForUpToDateCount(_ count: Int) async {
    if events.count >= count { return }
    await withCheckedContinuation { continuation in
      waiting.append((count: count, continuation: continuation))
    }
  }
}

private actor FirstTransactionCommitGate {
  private var transactionCount = 0
  private var committedTransactionCount = 0
  private var firstApplyWaiters: [CheckedContinuation<Void, Never>] = []
  private var firstCommitReleased = false
  private var firstCommitWaiter: CheckedContinuation<Void, Never>?

  func finishTransaction() async {
    transactionCount += 1
    if transactionCount == 1 {
      for waiter in firstApplyWaiters {
        waiter.resume()
      }
      firstApplyWaiters.removeAll()

      if !firstCommitReleased {
        await withCheckedContinuation { continuation in
          firstCommitWaiter = continuation
        }
      }
    }
    committedTransactionCount += 1
  }

  func waitForFirstApply() async {
    if transactionCount >= 1 { return }
    await withCheckedContinuation { continuation in
      firstApplyWaiters.append(continuation)
    }
  }

  func releaseFirstCommit() {
    firstCommitReleased = true
    firstCommitWaiter?.resume()
    firstCommitWaiter = nil
  }

  func hasCommittedTransaction() -> Bool {
    committedTransactionCount > 0
  }
}

private actor TransactionAttemptRecorder {
  private var attempts = 0
  private var waiting: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

  func recordAttempt() {
    attempts += 1
    if !waiting.isEmpty {
      var remaining: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
      for waiter in waiting {
        if attempts >= waiter.count {
          waiter.continuation.resume()
        } else {
          remaining.append(waiter)
        }
      }
      waiting = remaining
    }
  }

  func attemptCount() -> Int {
    attempts
  }

  func waitForAttemptCount(_ count: Int) async {
    if attempts >= count { return }
    await withCheckedContinuation { continuation in
      waiting.append((count: count, continuation: continuation))
    }
  }
}

private enum CollectionBoundaryTestError: Error {
  case commitFailed
}

private struct TestModel: ElectricCollectionModel {
  static var tableName: String { "test_models" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(endpoint: "/shapes/test-models", selectedColumns: ["id"])
  }

  static func createShapeRequest(
    where predicate: SQLExpression?,
    orderBy: [OrderBy],
    limit: Int?,
    offset: String?,
    handle: String?,
    cursor: String?,
    live: Bool
  ) -> ElectricShapeRequest {
    ElectricShapeRequest(
      table: tableName,
      predicate: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live
    )
  }

  static func processMessage(
    _ message: ElectricMessage,
    transaction _: Any?
  ) throws -> ProcessedMessage<TestModel> {
    ProcessedMessage(
      records: [],
      metadata: StoreMetadata(
        offset: message.offset,
        handle: message.handle,
        cursor: message.cursor,
        operation: .insert
      )
    )
  }

  static func truncate(transaction: Any?) throws {
    if let store = transaction as? ClearCountingStore {
      store.clear()
    }
  }
}

extension ElectricMessage {
  fileprivate static func snapshot(offset: String) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: false,
      kind: .snapshot
    )
  }

  fileprivate static func truncate(handle: String?) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: "-1",
      handle: handle,
      cursor: nil,
      isUpToDate: false,
      kind: .truncate
    )
  }

  fileprivate static func upToDate(offset: String) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: true,
      kind: .snapshot,
      control: .upToDate
    )
  }

  fileprivate static func legacyUpToDate(offset: String) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: true,
      kind: .snapshot,
      control: nil
    )
  }
}

private final class ClearCountingStore: @unchecked Sendable {
  private let lock = NSLock()
  private var clears: Int = 0

  func clear() {
    lock.lock()
    clears += 1
    lock.unlock()
  }

  func clearCallCount() -> Int {
    lock.lock()
    defer { lock.unlock() }
    return clears
  }
}
