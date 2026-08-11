import Foundation
import Testing

@testable import ElectricSync

struct SubscribeBackoffTests {
  @Test
  func subscribeDoesNotHotLoopAfterSyncErrors() async throws {
    let metadataProvider = NoopMetadataProvider()
    let httpClient = ThrowingHTTPClientProvider()
    let fetchTracker = ElectricFetchTracker(metadataProvider: metadataProvider)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: fetchTracker
      )
    )

    let transactionRunner:
      @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
        try operation(nil)
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
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      logger: NoopLogProvider()
    )

    let breaker = FixedDelayCircuitBreaker(delay: 60)
    let stream = collection.subscribe(circuitBreaker: breaker)
    let consumer = Task {
      for await _ in stream {}
    }

    await httpClient.waitForRequestCount(1)
    try await Task.sleep(nanoseconds: 30_000_000)
    #expect(await httpClient.requestCount() == 1)

    consumer.cancel()
    await consumer.value
  }
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

  func clear<T>(_ type: T.Type) async throws where T: ElectricCollectionModel {}
}

private struct FixedDelayCircuitBreaker: CircuitBreakerStrategy {
  let delay: TimeInterval

  mutating func preflightDelay(now _: Date) -> TimeInterval? { nil }
  mutating func recordSuccess() {}
  mutating func recordFailure(now _: Date, reason _: String?) -> TimeInterval { delay }
  mutating func reset() {}
}

private actor ThrowingHTTPClientProvider: HTTPClientProvider {
  private var requests: [ElectricShapeRequest] = []
  private var waiting: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

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

    throw TestError()
  }

  func requestCount() -> Int {
    requests.count
  }

  func waitForRequestCount(_ count: Int) async {
    if requests.count >= count { return }
    await withCheckedContinuation { continuation in
      waiting.append((count: count, continuation: continuation))
    }
  }
}

private struct TestError: Error {}

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
}
