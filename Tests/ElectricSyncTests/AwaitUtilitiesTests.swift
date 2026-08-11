import Foundation
import Testing

@testable import ElectricSync

struct AwaitUtilitiesTests {
  @Test
  func awaitTxIdResolvesWhenSeenInTxids() async throws {
    let metadata = NoopMetadataProvider()
    let http = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(payload: Data(), txids: [123]),
          ElectricMessage(payload: Data(), isUpToDate: true, control: .upToDate),
        ]
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http
      )
    )

    let awaiting = Task {
      try await client.awaitTxId(123, timeout: 1.0)
    }

    _ = try await client.pollStream(
      TestModel.self,
      basePredicate: nil,
      syncMode: .onDemand,
      live: true
    )

    #expect(try await awaiting.value)
  }

  @Test
  func awaitTxIdResolvesWhenVisibleInSnapshot() async throws {
    let metadata = NoopMetadataProvider()
    let http = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: Data(),
            control: .snapshotEnd,
            postgresSnapshot: PostgresSnapshot(
              xmin: "10",
              xmax: "20",
              xipList: ["11", "12"]
            )
          ),
          ElectricMessage(payload: Data(), isUpToDate: true, control: .upToDate),
        ]
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http
      )
    )

    let awaiting = Task {
      try await client.awaitTxId(13, timeout: 1.0)
    }

    _ = try await client.pollStream(
      TestModel.self,
      basePredicate: nil,
      syncMode: .onDemand,
      live: false
    )

    #expect(try await awaiting.value)
  }

  @Test
  func awaitMatchResolvesAfterUpToDate() async throws {
    let metadata = NoopMetadataProvider()
    let http = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(payload: Data([0x01])),
          ElectricMessage(payload: Data(), isUpToDate: true, control: .upToDate),
        ]
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http
      )
    )

    let awaiting = Task {
      try await client.awaitMatch(timeout: 1.0) { $0.payload == Data([0x01]) }
    }

    _ = try await client.pollStream(
      TestModel.self,
      basePredicate: nil,
      syncMode: .onDemand,
      live: true
    )

    #expect(try await awaiting.value)
  }

  @Test
  func awaitTxIdTimesOut() async {
    let metadata = NoopMetadataProvider()
    let http = ScriptedHTTPClientProvider(responses: [])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http
      )
    )

    do {
      _ = try await client.awaitTxId(999, timeout: 0.05)
      Issue.record("Expected timeout")
    } catch let error as ElectricSyncAwaitError {
      #expect(error == .timeoutWaitingForTxId(999))
    } catch {
      Issue.record("Unexpected error: \(error)")
    }
  }

  @Test
  func awaitMatchTimesOut() async {
    let metadata = NoopMetadataProvider()
    let http = ScriptedHTTPClientProvider(responses: [])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http
      )
    )

    do {
      _ = try await client.awaitMatch(timeout: 0.05) { _ in true }
      Issue.record("Expected timeout")
    } catch let error as ElectricSyncAwaitError {
      #expect(error == .timeoutWaitingForMatch)
    } catch {
      Issue.record("Unexpected error: \(error)")
    }
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

private actor ScriptedHTTPClientProvider: HTTPClientProvider {
  private var responses: [[ElectricMessage]]

  init(responses: [[ElectricMessage]]) {
    self.responses = responses
  }

  func fetch(_: ElectricShapeRequest) async throws -> [ElectricMessage] {
    guard !responses.isEmpty else { return [] }
    return responses.removeFirst()
  }
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
}
