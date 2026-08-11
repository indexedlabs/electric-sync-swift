import Foundation
import Testing

@testable import ElectricSync

struct ElectricSyncTimingLogTests {
  @Test
  func queryLogsWarningWhenSubsetFetchIsSlow() async throws {
    let baseDate = Date(timeIntervalSince1970: 1_700_000_000)
    let dateGenerator = ScriptedDateGenerator(
      dates: [
        baseDate,
        baseDate,
        baseDate.addingTimeInterval(1),
      ]
    )
    do {
      let metadataProvider = TimingNoopMetadataProvider()
      let httpClient = DelayedScriptedHTTPClientProvider(
        responses: [
          [.upToDate(offset: "offset-1"), .subsetEnd(offset: "offset-1")]
        ],
        delayNanoseconds: 0
      )
      let logger = CapturingLogProvider()

      let client = ElectricSyncClientImpl(
        configuration: .init(
          metadataProvider: metadataProvider,
          httpClient: httpClient,
          fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider),
          runtimeProvider: ElectricSyncRuntimeProvider(
            now: { dateGenerator.next() },
            makeUUID: UUID.init,
            sleep: { duration in try await Task.sleep(for: duration) }
          )
        )
      )

      let transactionRunner:
        @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
          try operation(nil)
        }

      let collection = ElectricCollection(
        configuration: ElectricCollectionConfiguration(
          modelType: TimingTestModel.self,
          syncMode: .onDemand,
          live: false,
          shapeTopology: .staticallySimple
        ),
        client: client,
        cacheProvider: TimingNoopCacheProvider(),
        transactionRunner: transactionRunner,
        backgroundTaskProvider: NoopBackgroundTaskProvider(),
        logger: logger
      )

      _ = try await collection.query()

      let entries = logger.entries()
      #expect(
        entries.contains { entry in
          entry.level == .warning
            && entry.message == "Electric subset fetch was slow"
        }
      )
      #expect(
        entries.contains { entry in
          entry.message == "Electric subset fetch was slow"
            && entry.metadata?["durationMs"] != nil
        }
      )
    }
  }

  @Test
  func queryLogsBatchWarningWhenBoundaryTransactionIsSlow() async throws {
    let baseDate = Date(timeIntervalSince1970: 1_700_000_000)
    let dateGenerator = ScriptedDateGenerator(
      dates: [
        baseDate,
        baseDate,
        baseDate,
        baseDate,
        baseDate.addingTimeInterval(1),
      ]
    )
    do {
      let metadataProvider = TimingNoopMetadataProvider()
      let httpClient = DelayedScriptedHTTPClientProvider(
        responses: [
          [.upToDate(offset: "offset-1"), .subsetEnd(offset: "offset-1")]
        ],
        delayNanoseconds: 0
      )
      let logger = CapturingLogProvider()

      let client = ElectricSyncClientImpl(
        configuration: .init(
          metadataProvider: metadataProvider,
          httpClient: httpClient,
          fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider),
          runtimeProvider: ElectricSyncRuntimeProvider(
            now: { dateGenerator.next() },
            makeUUID: UUID.init,
            sleep: { duration in try await Task.sleep(for: duration) }
          )
        )
      )

      let transactionRunner:
        @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
          try operation(nil)
        }

      let collection = ElectricCollection(
        configuration: ElectricCollectionConfiguration(
          modelType: TimingTestModel.self,
          syncMode: .onDemand,
          live: false,
          shapeTopology: .staticallySimple
        ),
        client: client,
        cacheProvider: TimingNoopCacheProvider(),
        transactionRunner: transactionRunner,
        backgroundTaskProvider: NoopBackgroundTaskProvider(),
        logger: logger
      )

      _ = try await collection.query()

      let entries = logger.entries()
      #expect(
        entries.contains { entry in
          entry.level == .warning
            && entry.message == "Electric sync batch apply was slow (query)"
        }
      )
    }
  }

  @Test
  func subscribeClassifiesVerySlowLongPollWaitAsTransportWaitInfo() async throws {
    let baseDate = Date(timeIntervalSince1970: 1_700_000_000)
    let dateGenerator = ScriptedDateGenerator(
      dates: [
        baseDate,
        baseDate,
        baseDate.addingTimeInterval(20),
      ]
    )

    do {
      let metadataProvider = TimingNoopMetadataProvider()
      let httpClient = DelayedScriptedHTTPClientProvider(
        responses: [
          [.upToDate(offset: "offset-1")]
        ],
        delayNanoseconds: 0
      )
      let logger = CapturingLogProvider()

      let client = ElectricSyncClientImpl(
        configuration: .init(
          metadataProvider: metadataProvider,
          httpClient: httpClient,
          fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider),
          runtimeProvider: ElectricSyncRuntimeProvider(
            now: { dateGenerator.next() },
            makeUUID: UUID.init,
            sleep: { duration in try await Task.sleep(for: duration) }
          )
        )
      )

      let transactionRunner:
        @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
          try operation(nil)
        }

      let collection = ElectricCollection(
        configuration: ElectricCollectionConfiguration(
          modelType: TimingTestModel.self,
          syncMode: .onDemand,
          live: true,
          liveTransport: .longPoll
        ),
        client: client,
        cacheProvider: TimingNoopCacheProvider(),
        transactionRunner: transactionRunner,
        backgroundTaskProvider: NoopBackgroundTaskProvider(),
        logger: logger
      )

      let stream = collection.subscribe(circuitBreaker: TimingFixedDelayCircuitBreaker(delay: 60))
      let consumer = Task {
        for await _ in stream {
          break
        }
      }
      await consumer.value

      let entries = logger.entries()
      #expect(
        entries.contains { entry in
          entry.level == .info
            && entry.message == "Electric poll transport wait"
            && entry.metadata?["timing.classification"] == "transport_wait"
            && entry.metadata?["timing.expected_for_long_poll"] == "true"
            && entry.metadata?["transport.wait.sampled"] == "true"
        }
      )
      #expect(
        entries.contains { entry in
          entry.message == "Electric poll fetch was slow"
        } == false
      )
    }
  }

  @Test
  func subscribeTreatsLongPollTimeoutAsExpectedTransportWait() async throws {
    let metadataProvider = TimingNoopMetadataProvider()
    let httpClient = ScriptedTransportOutcomeHTTPClientProvider(
      outcomes: [
        .error(URLError(.timedOut)),
        .messages([.upToDate(offset: "offset-1")]),
      ]
    )
    let logger = CapturingLogProvider()
    let breakerCounter = TrackingCircuitBreakerCounter()

    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider)
      )
    )

    let transactionRunner:
      @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
        try operation(nil)
      }

    let collection = ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: TimingTestModel.self,
        syncMode: .onDemand,
        live: true,
        liveTransport: .longPoll
      ),
      client: client,
      cacheProvider: TimingNoopCacheProvider(),
      transactionRunner: transactionRunner,
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      logger: logger
    )

    let stream = collection.subscribe(
      circuitBreaker: TrackingCircuitBreaker(counter: breakerCounter)
    )
    let consumer = Task {
      for await _ in stream {
        break
      }
    }
    await consumer.value

    let entries = logger.entries()
    #expect(
      entries.contains { entry in
        entry.message == "Electric poll transport timeout"
          && entry.metadata?["transport.wait.expected"] == "true"
          && entry.metadata?["timing.classification"] == "transport_wait"
      }
    )
    #expect(
      entries.contains { entry in
        entry.level == .error
          && entry.message.starts(with: "Electric sync error for collection")
      } == false
    )
    #expect(breakerCounter.failureCount == 0)
    #expect(await httpClient.fetchCount() >= 2)
  }
}

private struct TimingTestModel: ElectricCollectionModel {
  static var tableName: String { "timing_test_models" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(endpoint: "/shapes/timing-test-models", selectedColumns: ["id"])
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
  ) throws -> ProcessedMessage<TimingTestModel> {
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
}

private actor DelayedScriptedHTTPClientProvider: HTTPClientProvider {
  private var responses: [[ElectricMessage]]
  private let delayNanoseconds: UInt64

  init(responses: [[ElectricMessage]], delayNanoseconds: UInt64) {
    self.responses = responses
    self.delayNanoseconds = delayNanoseconds
  }

  func fetch(_: ElectricShapeRequest) async throws -> [ElectricMessage] {
    if delayNanoseconds > 0 {
      try await Task.sleep(nanoseconds: delayNanoseconds)
    }
    guard !responses.isEmpty else { return [] }
    return responses.removeFirst()
  }
}

private actor ScriptedTransportOutcomeHTTPClientProvider: HTTPClientProvider {
  enum Outcome {
    case messages([ElectricMessage])
    case error(URLError)
  }

  private var outcomes: [Outcome]
  private var requests = 0

  init(outcomes: [Outcome]) {
    self.outcomes = outcomes
  }

  func fetch(_: ElectricShapeRequest) async throws -> [ElectricMessage] {
    requests += 1
    guard !outcomes.isEmpty else {
      return [.upToDate(offset: "fallback")]
    }

    switch outcomes.removeFirst() {
    case .messages(let messages):
      return messages
    case .error(let error):
      throw error
    }
  }

  func fetchCount() -> Int {
    requests
  }
}

private struct TimingFixedDelayCircuitBreaker: CircuitBreakerStrategy {
  let delay: TimeInterval

  mutating func preflightDelay(now _: Date) -> TimeInterval? { nil }
  mutating func recordSuccess() {}
  mutating func recordFailure(now _: Date, reason _: String?) -> TimeInterval { delay }
  mutating func reset() {}
}

private final class ScriptedDateGenerator: @unchecked Sendable {
  private let lock = NSLock()
  private let dates: [Date]
  private var index = 0

  init(dates: [Date]) {
    self.dates = dates
  }

  func next() -> Date {
    lock.lock()
    defer { lock.unlock() }
    guard !dates.isEmpty else { return Date() }
    if index >= dates.count {
      return dates[dates.count - 1]
    }
    let value = dates[index]
    index += 1
    return value
  }
}

private final class TrackingCircuitBreakerCounter: @unchecked Sendable {
  private let lock = NSLock()
  private var failures = 0

  var failureCount: Int {
    lock.lock()
    defer { lock.unlock() }
    return failures
  }

  func recordFailure() {
    lock.lock()
    failures += 1
    lock.unlock()
  }
}

private struct TrackingCircuitBreaker: CircuitBreakerStrategy {
  let counter: TrackingCircuitBreakerCounter

  mutating func preflightDelay(now _: Date) -> TimeInterval? { nil }
  mutating func recordSuccess() {}

  mutating func recordFailure(now _: Date, reason _: String?) -> TimeInterval {
    counter.recordFailure()
    return 0
  }

  mutating func reset() {}
}

private final class CapturingLogProvider: LogProvider, @unchecked Sendable {
  struct Entry: Sendable {
    let level: LogLevel
    let message: String
    let metadata: [String: String]?
  }

  private let lock = NSLock()
  private var recorded: [Entry] = []

  func log(_ level: LogLevel, message: String, metadata: [String: String]?) {
    lock.lock()
    recorded.append(Entry(level: level, message: message, metadata: metadata))
    lock.unlock()
  }

  func entries() -> [Entry] {
    lock.lock()
    defer { lock.unlock() }
    return recorded
  }
}

private struct TimingNoopMetadataProvider: MetadataProvider {
  // Match the durable ownership provider; the protocol default (false)
  // makes request shape scheduling-sensitive (see ElectricSyncClientTests).
  let supportsDurableRowOwnership = true
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

private struct TimingNoopCacheProvider: DataCacheProvider {
  func load<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> [T]
  where T: ElectricCollectionModel {
    []
  }

  func hasData<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> Bool
  where T: ElectricCollectionModel {
    false
  }
}

extension ElectricMessage {
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
}
