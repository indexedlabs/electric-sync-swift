import Foundation
import Testing

@testable import ElectricSync

struct BatchBufferingTests {
  @Test
  func liveQueriesBufferUntilUpToDate() async throws {
    let metadataProvider = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [ElectricMessage.make(record: TestRecord(id: "1", name: "Alpha"), offset: "offset-1")],
        [ElectricMessage.subsetEnd(offset: "offset-1")],
      ]
    )
    let fetchTracker = ElectricFetchTracker(metadataProvider: metadataProvider)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: fetchTracker
      )
    )

    let transactionCounter = TransactionCounter()
    let transactionRunner:
      @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
        await transactionCounter.increment()
        try operation(store)
      }

    let configuration = ElectricCollectionConfiguration(
      modelType: TestRecord.self,
      syncMode: .onDemand,
      live: true,
      shapeTopology: .staticallySimple
    )

    let collection = ElectricCollection(
      configuration: configuration,
      client: client,
      cacheProvider: StoreBackedCacheProvider(store: store),
      transactionRunner: transactionRunner
    )

    let results = try await collection.query(where: SQLExpression("id = '1'"))

    #expect(results == [TestRecord(id: "1", name: "Alpha")])
    #expect(await httpClient.requestCount() == 2)
    #expect(await transactionCounter.count() == 1)
  }
}

private actor TransactionCounter {
  private var value: Int = 0

  func increment() {
    value += 1
  }

  func count() -> Int {
    value
  }
}

private struct TestRecord: ElectricCollectionModel, Codable, Equatable {
  let id: String
  let name: String

  static var tableName: String { "test_records" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(
      endpoint: "/shapes/test-records",
      selectedColumns: ["id", "name"]
    )
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
    transaction: Any?
  ) throws -> ProcessedMessage<TestRecord> {
    let record = try JSONDecoder().decode(TestRecord.self, from: message.payload)
    if let store = transaction as? TestRecordStore {
      store.upsert(record)
    }
    return ProcessedMessage(
      records: [record],
      metadata: StoreMetadata(
        offset: message.offset,
        handle: message.handle,
        cursor: message.cursor,
        operation: .insert
      )
    )
  }
}

private final class TestRecordStore: @unchecked Sendable {
  private var records: [TestRecord] = []
  private let lock = NSLock()

  func upsert(_ record: TestRecord) {
    lock.lock()
    defer { lock.unlock() }
    if let index = records.firstIndex(where: { $0.id == record.id }) {
      records[index] = record
    } else {
      records.append(record)
    }
  }

  func allRecords() -> [TestRecord] {
    lock.lock()
    defer { lock.unlock() }
    return records
  }
}

private struct StoreBackedCacheProvider: DataCacheProvider {
  let store: TestRecordStore

  func load<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> [T]
  where T: ElectricCollectionModel {
    guard T.tableName == TestRecord.tableName else { return [] }
    return store.allRecords() as? [T] ?? []
  }

  func hasData<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> Bool
  where T: ElectricCollectionModel {
    guard T.tableName == TestRecord.tableName else { return false }
    return !store.allRecords().isEmpty
  }

  func clear<T>(_ type: T.Type) async throws where T: ElectricCollectionModel {}
}

private final class InMemoryMetadataProvider: MetadataProvider, @unchecked Sendable {
  // Match the durable ownership provider; the protocol default (false)
  // makes request shape scheduling-sensitive (see ElectricSyncClientTests).
  let supportsDurableRowOwnership = true
  private var syncStates: [String: SyncState] = [:]
  private let lock = NSLock()

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

  func resetSyncState(collectionId _: String, transaction _: Any?) throws {}
}

private actor ScriptedHTTPClientProvider: HTTPClientProvider {
  private var responses: [[ElectricMessage]]
  private var requests: [ElectricShapeRequest] = []

  init(responses: [[ElectricMessage]]) {
    self.responses = responses
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    requests.append(request)
    guard !responses.isEmpty else { return [] }
    return responses.removeFirst()
  }

  func requestCount() -> Int {
    requests.count
  }
}

extension ElectricMessage {
  fileprivate static func make(record: TestRecord, offset: String) -> ElectricMessage {
    let payload = try! JSONEncoder().encode(record)
    return ElectricMessage(
      payload: payload,
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
