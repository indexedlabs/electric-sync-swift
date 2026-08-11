import Foundation
import Testing

@testable import ElectricSync

struct SSETransportTests {
  @Test
  func liveBatchStreamYieldsBatchesAtUpToDateBoundaries() async throws {
    let metadataProvider = SeededMetadataProvider()
    let httpClient = NoopHTTPClientProvider()
    let httpStreamClient = ScriptedHTTPStreamClientProvider(
      streams: [
        [
          .snapshot(offset: "10_0"),
          .upToDate(offset: "10_0"),
          .snapshot(offset: "11_0"),
          .upToDate(offset: "11_0"),
        ]
      ]
    )

    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: SSETestModel.self,
      basePredicate: nil
    )
    metadataProvider.seedSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "10_0",
        handle: "handle-10_0",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: Date()
      )
    )

    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        httpStreamClient: httpStreamClient,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider),
        isExactCursorCutoverEnabled: true
      )
    )

    let stream = try #require(
      try await client.liveBatchStream(
        SSETestModel.self,
        basePredicate: nil,
        syncMode: .onDemand
      )
    )

    var batches: [[ElectricMessage]] = []
    for try await batch in stream {
      batches.append(batch.messages)
    }

    #expect(batches.count == 2)
    let firstHasUpToDate = batches[0].contains { message in
      message.control == .upToDate && message.offset == "10_0"
    }
    let secondHasUpToDate = batches[1].contains { message in
      message.control == .upToDate && message.offset == "11_0"
    }
    #expect(firstHasUpToDate)
    #expect(secondHasUpToDate)
  }

  @Test
  func subscribeFallsBackToLongPollAfterRepeatedSSEFailures() async throws {
    let metadataProvider = SeededMetadataProvider()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [.upToDate(offset: "10_0")]
      ]
    )
    let httpStreamClient = SequencedHTTPStreamClientProvider(
      outcomes: [
        .throwOnOpen,
        .throwOnOpen,
        .throwOnOpen,
      ]
    )
    let tracer = SSETracingRecorder()
    let collection = makeLiveCollection(
      metadataProvider: metadataProvider,
      httpClient: httpClient,
      httpStreamClient: httpStreamClient,
      tracer: tracer
    )

    let stream = collection.subscribe(circuitBreaker: ZeroDelayCircuitBreaker())
    let consumer = Task {
      for await _ in stream {}
    }

    await httpStreamClient.waitForRequestCount(3)
    await httpClient.waitForRequestCount(1)
    let fallbackPollSpan = await tracer.waitForCompletedSpan(
      named: "electric.poll_stream",
      attributes: [
        "stage": "poll_stream",
        "transport": "http_poll",
      ]
    )

    consumer.cancel()
    await consumer.value

    let streamRequestCount = await httpStreamClient.requestCount()
    #expect(streamRequestCount == 3)
    #expect(await httpClient.requestCount() >= 1)

    #expect(fallbackPollSpan.status == .success)
    #expect(fallbackPollSpan.attributes["message.count"] == "1")
    #expect(fallbackPollSpan.attributes["message.control.up_to_date.count"] == "1")
    #expect(fallbackPollSpan.attributes["has_up_to_date"] == "true")
  }

  @Test
  func subscribeFallsBackToLongPollAfterRepeatedPrematureSSEClosures() async throws {
    let metadataProvider = SeededMetadataProvider()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [.upToDate(offset: "10_0")]
      ]
    )
    let httpStreamClient = SequencedHTTPStreamClientProvider(
      outcomes: [
        .finish(messages: []),
        .finish(messages: []),
        .finish(messages: []),
        .throwOnOpen,
        .throwOnOpen,
        .throwOnOpen,
      ]
    )
    let collection = makeLiveCollection(
      metadataProvider: metadataProvider,
      httpClient: httpClient,
      httpStreamClient: httpStreamClient
    )

    let stream = collection.subscribe(circuitBreaker: ZeroDelayCircuitBreaker())
    let consumer = Task {
      for await _ in stream {}
    }

    await httpClient.waitForRequestCount(1)

    consumer.cancel()
    await consumer.value

    let streamRequestCount = await httpStreamClient.requestCount()
    #expect(streamRequestCount == 3)
    #expect(await httpClient.requestCount() >= 1)
  }

  @Test
  func successfulSSEBoundaryResetsConsecutiveFailureCount() async throws {
    let metadataProvider = SeededMetadataProvider()
    let httpClient = ScriptedHTTPClientProvider(responses: [])
    let httpStreamClient = SequencedHTTPStreamClientProvider(
      outcomes: [
        .throwOnOpen,
        .throwOnOpen,
        .finish(messages: [.upToDate(offset: "10_0")]),
        .throwOnOpen,
        .throwOnOpen,
      ]
    )
    let collection = makeLiveCollection(
      metadataProvider: metadataProvider,
      httpClient: httpClient,
      httpStreamClient: httpStreamClient
    )

    let stream = collection.subscribe(circuitBreaker: ZeroDelayCircuitBreaker())
    let consumer = Task {
      for await _ in stream {}
    }

    await httpClient.waitForRequestCount(1)

    let streamRequestCount = await httpStreamClient.requestCount()
    #expect(streamRequestCount == 5)
    #expect(await httpClient.requestCount() >= 1)

    consumer.cancel()
    await consumer.value
  }

  private func makeLiveCollection(
    metadataProvider: SeededMetadataProvider,
    httpClient: some HTTPClientProvider,
    httpStreamClient: some HTTPStreamClientProvider,
    tracer: (any ElectricSyncTracer)? = nil
  ) -> ElectricCollection<SSETestModel> {
    let resolvedTracer = tracer ?? NoopElectricSyncTracer()
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: SSETestModel.self,
      basePredicate: nil
    )
    metadataProvider.seedSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "10_0",
        handle: "handle-10_0",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: Date()
      )
    )

    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        httpStreamClient: httpStreamClient,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider),
        tracer: resolvedTracer,
        isExactCursorCutoverEnabled: true
      )
    )
    let transactionRunner:
      @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
        try operation(nil)
      }

    return ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: SSETestModel.self,
        syncMode: .onDemand,
        live: true,
        liveTransport: .sseWithFallback,
        shapeTopology: .staticallySimple
      ),
      client: client,
      cacheProvider: NoopCacheProvider(),
      transactionRunner: transactionRunner,
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      logger: NoopLogProvider(),
      tracer: resolvedTracer
    )
  }
}

// MARK: - Fixtures

private struct SSETestModel: ElectricCollectionModel {
  static var tableName: String { "sse_test_models" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(endpoint: "/shapes/sse-test-models", selectedColumns: ["id"])
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
  ) throws -> ProcessedMessage<SSETestModel> {
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

private final class SeededMetadataProvider: MetadataProvider, @unchecked Sendable {
  let supportsDurableRowOwnership = true

  private var syncStates: [String: SyncState] = [:]
  private let lock = NSLock()

  func seedSyncState(collectionId: String, state: SyncState) {
    lock.lock()
    defer { lock.unlock() }
    syncStates[collectionId] = state
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

  func getSyncState(collectionId: String, transaction _: Any?) throws -> SyncState? {
    lock.lock()
    defer { lock.unlock() }
    return syncStates[collectionId]
  }

  func updateSyncState(collectionId: String, state: SyncState, transaction _: Any?) throws {
    lock.lock()
    defer { lock.unlock() }
    syncStates[collectionId] = state
  }

  func resetSyncState(collectionId: String, transaction _: Any?) throws {
    lock.lock()
    defer { lock.unlock() }
    syncStates[collectionId] = SyncState(
      offset: nil,
      handle: nil,
      cursor: nil,
      isUpToDate: false,
      lastSyncedAt: nil
    )
  }
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

struct ZeroDelayCircuitBreaker: CircuitBreakerStrategy {
  mutating func preflightDelay(now _: Date) -> TimeInterval? { nil }
  mutating func recordSuccess() {}
  mutating func recordFailure(now _: Date, reason _: String?) -> TimeInterval { 0 }
  mutating func reset() {}
}

private actor NoopHTTPClientProvider: HTTPClientProvider {
  func fetch(_: ElectricShapeRequest) async throws -> [ElectricMessage] { [] }
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

actor ScriptedHTTPStreamClientProvider: HTTPStreamClientProvider {
  private var streams: [[ElectricMessage]]

  init(streams: [[ElectricMessage]]) {
    self.streams = streams
  }

  func stream(_: ElectricShapeRequest) async throws -> AsyncThrowingStream<ElectricMessage, Error> {
    let messages = streams.isEmpty ? [] : streams.removeFirst()
    return AsyncThrowingStream { continuation in
      for message in messages {
        continuation.yield(message)
      }
      continuation.finish()
    }
  }
}

private actor SequencedHTTPStreamClientProvider: HTTPStreamClientProvider {
  enum Outcome: Sendable {
    case finish(messages: [ElectricMessage])
    case throwOnOpen
  }

  private var outcomes: [Outcome]
  private var requests: [ElectricShapeRequest] = []
  private var waiting: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

  init(outcomes: [Outcome]) {
    self.outcomes = outcomes
  }

  func stream(_ request: ElectricShapeRequest) async throws -> AsyncThrowingStream<
    ElectricMessage, Error
  > {
    requests.append(request)
    resumeSatisfiedWaiters()

    let outcome = outcomes.isEmpty ? .throwOnOpen : outcomes.removeFirst()
    switch outcome {
    case .finish(let messages):
      return AsyncThrowingStream { continuation in
        for message in messages {
          continuation.yield(message)
        }
        continuation.finish()
      }
    case .throwOnOpen:
      throw TestError()
    }
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

  private func resumeSatisfiedWaiters() {
    let requestCount = requests.count
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
}

private struct TestError: Error {}

private struct SSECompletedSpan: Sendable {
  let name: String
  let attributes: [String: String]
  let status: ElectricSyncSpanStatus
}

private struct SSESpanWaiter {
  let name: String
  let attributes: [String: String]
  let continuation: CheckedContinuation<SSECompletedSpan, Never>

  func matches(_ span: SSECompletedSpan) -> Bool {
    span.name == name
      && attributes.allSatisfy { key, value in span.attributes[key] == value }
  }
}

private final class SSETracingRecorder: @unchecked Sendable, ElectricSyncTracer {
  private let lock = NSLock()
  private var spanNamesByID: [UUID: String] = [:]
  private var spanAttributesByID: [UUID: [String: String]] = [:]
  private var completed: [SSECompletedSpan] = []
  private var waiters: [SSESpanWaiter] = []

  func startSpan(name: String, attributes: [String: String]) -> any ElectricSyncSpan {
    let id = UUID()
    lock.lock()
    spanNamesByID[id] = name
    spanAttributesByID[id] = attributes
    lock.unlock()
    return SSETracingSpan(id: id, recorder: self)
  }

  fileprivate func setAttribute(id: UUID, key: String, value: String) {
    lock.lock()
    defer { lock.unlock() }
    var attributes = spanAttributesByID[id] ?? [:]
    attributes[key] = value
    spanAttributesByID[id] = attributes
  }

  fileprivate func finish(id: UUID, status: ElectricSyncSpanStatus) {
    lock.lock()
    let name = spanNamesByID[id] ?? "<unknown>"
    let attributes = spanAttributesByID[id] ?? [:]
    let span = SSECompletedSpan(name: name, attributes: attributes, status: status)
    completed.append(span)
    spanNamesByID.removeValue(forKey: id)
    spanAttributesByID.removeValue(forKey: id)
    var remaining: [SSESpanWaiter] = []
    var satisfied: [SSESpanWaiter] = []
    for waiter in waiters {
      if waiter.matches(span) {
        satisfied.append(waiter)
      } else {
        remaining.append(waiter)
      }
    }
    waiters = remaining
    lock.unlock()

    for waiter in satisfied {
      waiter.continuation.resume(returning: span)
    }
  }

  fileprivate func waitForCompletedSpan(
    named name: String,
    attributes: [String: String]
  ) async -> SSECompletedSpan {
    await withCheckedContinuation { continuation in
      lock.lock()
      if let span = completed.first(where: { span in
        span.name == name
          && attributes.allSatisfy { key, value in span.attributes[key] == value }
      }) {
        lock.unlock()
        continuation.resume(returning: span)
      } else {
        waiters.append(
          SSESpanWaiter(
            name: name,
            attributes: attributes,
            continuation: continuation
          )
        )
        lock.unlock()
      }
    }
  }
}

private final class SSETracingSpan: ElectricSyncSpan, @unchecked Sendable {
  private let id: UUID
  private let recorder: SSETracingRecorder
  private let lock = NSLock()
  private var hasEnded = false

  init(id: UUID, recorder: SSETracingRecorder) {
    self.id = id
    self.recorder = recorder
  }

  func setAttribute(key: String, value: String) {
    lock.lock()
    defer { lock.unlock() }
    guard !hasEnded else { return }
    recorder.setAttribute(id: id, key: key, value: value)
  }

  func end(status: ElectricSyncSpanStatus) {
    lock.lock()
    defer { lock.unlock() }
    guard !hasEnded else { return }
    hasEnded = true
    recorder.finish(id: id, status: status)
  }
}

extension ElectricMessage {
  fileprivate static func snapshot(offset: String) -> ElectricMessage {
    ElectricMessage(
      payload: Data("{\"offset\":\"\(offset)\"}".utf8),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: false,
      kind: .snapshot
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
}
