import Foundation
import Testing

@testable import ElectricSync

private final class LockedArray<Element>: @unchecked Sendable {
  private let lock = NSLock()
  private var values: [Element] = []

  func append(_ value: Element) {
    lock.lock()
    defer { lock.unlock() }
    values.append(value)
  }

  func reset() {
    lock.lock()
    defer { lock.unlock() }
    values.removeAll(keepingCapacity: false)
  }

  func snapshot() -> [Element] {
    lock.lock()
    defer { lock.unlock() }
    return values
  }
}

private final class MutableTestDateSource: @unchecked Sendable {
  private let lock = NSLock()
  private var value: Date

  init(_ value: Date) {
    self.value = value
  }

  func now() -> Date {
    lock.withLock { value }
  }

  func set(_ value: Date) {
    lock.withLock { self.value = value }
  }
}

private func fixedRuntimeProvider(_ date: Date) -> ElectricSyncRuntimeProvider {
  ElectricSyncRuntimeProvider(
    now: { date },
    makeUUID: UUID.init,
    sleep: { duration in try await Task.sleep(for: duration) }
  )
}

private final class InMemoryMetadataProvider: MetadataProvider, @unchecked Sendable {
  private struct OwnedRow: Hashable {
    let table: String
    let rowKey: String
  }

  private let lock = NSLock()
  private var syncStates: [String: SyncState] = [:]
  private var ownersByRow: [OwnedRow: Set<String>] = [:]
  private var tagsByOwner: [OwnedRow: [String: Set<String>]] = [:]
  private var ownershipReadScopes: [Set<String>?] = []
  private var ownershipWriteCounts: [Int] = []
  private var fetchMetadataTables = Set<String>()
  let supportsDurableRowOwnership: Bool

  init(supportsDurableRowOwnership: Bool = true) {
    self.supportsDurableRowOwnership = supportsDurableRowOwnership
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

  func clearMetadata(table: String, transaction _: Any?) throws {
    lock.withLock {
      fetchMetadataTables.remove(table)
    }
  }

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

  func claimRowOwnership(
    table: String,
    rowKey: String,
    shapeIdentity: String,
    transaction _: Any?
  ) throws {
    lock.withLock {
      ownersByRow[OwnedRow(table: table, rowKey: rowKey), default: []].insert(shapeIdentity)
      tagsByOwner[OwnedRow(table: table, rowKey: rowKey), default: [:]][shapeIdentity] = []
    }
  }

  func releaseRowOwnership(
    table: String,
    rowKey: String,
    shapeIdentity: String,
    deferredDeleteTombstone _: ElectricMoveOutTombstone?,
    transaction _: Any?
  ) throws -> Bool {
    // Without durable ownership this provider models a single shape, like the
    // protocol's default implementation: eviction is never vetoed.
    guard supportsDurableRowOwnership else { return true }
    return lock.withLock {
      let key = OwnedRow(table: table, rowKey: rowKey)
      guard var owners = ownersByRow[key], owners.remove(shapeIdentity) != nil else {
        return false
      }
      tagsByOwner[key]?[shapeIdentity] = nil
      if tagsByOwner[key]?.isEmpty == true { tagsByOwner[key] = nil }
      ownersByRow[key] = owners.isEmpty ? nil : owners
      return owners.isEmpty
    }
  }

  func releaseAllRowOwnership(
    table: String,
    shapeIdentity: String,
    transaction _: Any?
  ) throws -> [String] {
    lock.withLock {
      var orphanedRowKeys: [String] = []
      for key in Array(ownersByRow.keys) where key.table == table {
        guard var owners = ownersByRow[key], owners.remove(shapeIdentity) != nil else { continue }
        tagsByOwner[key]?[shapeIdentity] = nil
        if tagsByOwner[key]?.isEmpty == true { tagsByOwner[key] = nil }
        ownersByRow[key] = owners.isEmpty ? nil : owners
        if owners.isEmpty { orphanedRowKeys.append(key.rowKey) }
      }
      return orphanedRowKeys.sorted()
    }
  }

  func removeAllRowOwnership(table: String, rowKey: String, transaction _: Any?) throws {
    lock.withLock {
      ownersByRow[OwnedRow(table: table, rowKey: rowKey)] = nil
      tagsByOwner[OwnedRow(table: table, rowKey: rowKey)] = nil
    }
  }

  func getRowOwnershipTags(
    table: String,
    shapeIdentity: String,
    rowKeys: Set<String>?,
    transaction _: Any?
  ) throws -> [String: [String]] {
    lock.withLock {
      ownershipReadScopes.append(rowKeys)
      return Dictionary(
        uniqueKeysWithValues: tagsByOwner.compactMap { key, owners in
          guard key.table == table, rowKeys?.contains(key.rowKey) != false,
            let tags = owners[shapeIdentity]
          else { return nil }
          return (key.rowKey, tags.sorted())
        })
    }
  }

  func trackerRebuildOwnership(
    table: String,
    shapeIdentity: String,
    transaction: Any?
  ) throws -> [String: [String]]? {
    try getRowOwnershipTags(
      table: table,
      shapeIdentity: shapeIdentity,
      rowKeys: nil,
      transaction: transaction
    )
  }

  func updateRowOwnership(
    table: String,
    shapeIdentity: String,
    tagsByRowKey: [String: [String]],
    removedRowKeys: Set<String>,
    transaction _: Any?
  ) throws {
    lock.withLock {
      ownershipWriteCounts.append(Set(tagsByRowKey.keys).union(removedRowKeys).count)
      for rowKey in removedRowKeys {
        let key = OwnedRow(table: table, rowKey: rowKey)
        ownersByRow[key]?.remove(shapeIdentity)
        if ownersByRow[key]?.isEmpty == true { ownersByRow[key] = nil }
        tagsByOwner[key]?[shapeIdentity] = nil
        if tagsByOwner[key]?.isEmpty == true { tagsByOwner[key] = nil }
      }
      for (rowKey, tags) in tagsByRowKey {
        let key = OwnedRow(table: table, rowKey: rowKey)
        ownersByRow[key, default: []].insert(shapeIdentity)
        tagsByOwner[key, default: [:]][shapeIdentity] = Set(tags)
      }
    }
  }

  func ownerCount(table: String, rowKey: String) -> Int {
    lock.withLock {
      ownersByRow[OwnedRow(table: table, rowKey: rowKey)]?.count ?? 0
    }
  }

  func seedOwnership(
    table: String,
    shapeIdentity: String,
    tagsByRowKey: [String: [String]]
  ) {
    lock.withLock {
      for (rowKey, tags) in tagsByRowKey {
        let key = OwnedRow(table: table, rowKey: rowKey)
        ownersByRow[key, default: []].insert(shapeIdentity)
        tagsByOwner[key, default: [:]][shapeIdentity] = Set(tags)
      }
      ownershipReadScopes.removeAll()
      ownershipWriteCounts.removeAll()
    }
  }

  func ownershipWork() -> (readScopes: [Set<String>?], writeCounts: [Int]) {
    lock.withLock { (ownershipReadScopes, ownershipWriteCounts) }
  }

  func seedFetchMetadata(table: String) {
    lock.withLock {
      fetchMetadataTables.insert(table)
    }
  }

  func hasFetchMetadata(table: String) -> Bool {
    lock.withLock { fetchMetadataTables.contains(table) }
  }
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

private enum DeleteCapture {
  static let keys = LockedArray<String>()
}

private enum ProcessCapture {
  static let keys = LockedArray<String>()
}

private struct TestMoveOutModel: ElectricCollectionModel {
  static var tableName: String { "test_table" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(endpoint: "/shapes/test-move-out", selectedColumns: ["id"])
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

  static func processMessage(_ message: ElectricMessage, transaction _: Any?) throws
    -> ProcessedMessage<Self>
  {
    if let key = message.key {
      ProcessCapture.keys.append(key)
    }
    return ProcessedMessage(
      records: [Self()],
      metadata: StoreMetadata(
        offset: message.offset,
        handle: message.handle,
        cursor: message.cursor,
        operation: .insert
      )
    )
  }

  static func deleteByKey(_ key: String, transaction _: Any?) throws {
    DeleteCapture.keys.append(key)
  }
}

private struct VersionedMoveOutRow: Codable, Equatable, Sendable {
  let id: String
  let version: Int64
}

private final class VersionedMoveOutStore: @unchecked Sendable {
  private let lock = NSLock()
  private var rows: [String: VersionedMoveOutRow] = [:]
  private var tombstones: [String: ElectricMoveOutTombstone] = [:]
  private var expiredTombstoneRemovalLimits: [Int] = []

  func row(id: String) -> VersionedMoveOutRow? {
    lock.withLock { rows[id] }
  }

  func upsert(_ row: VersionedMoveOutRow) {
    lock.withLock {
      rows[row.id] = row
      if let tombstone = tombstones[row.id], row.version > tombstone.version {
        tombstones[row.id] = nil
      }
    }
  }

  func delete(id: String) {
    lock.withLock {
      rows[id] = nil
    }
  }

  func tombstone(rowId: String) -> ElectricMoveOutTombstone? {
    lock.withLock { tombstones[rowId] }
  }

  func record(_ tombstone: ElectricMoveOutTombstone) {
    lock.withLock {
      if let existing = tombstones[tombstone.rowId], existing.version > tombstone.version {
        return
      }
      tombstones[tombstone.rowId] = tombstone
    }
  }

  func tombstoneCount() -> Int {
    lock.withLock { tombstones.count }
  }

  func removalLimits() -> [Int] {
    lock.withLock { expiredTombstoneRemovalLimits }
  }

  func deleteExpired(deletedBefore: Date, limit: Int) -> Int {
    lock.withLock {
      expiredTombstoneRemovalLimits.append(limit)
      guard limit > 0 else { return 0 }
      let expiredIds = tombstones.compactMap { rowId, tombstone in
        tombstone.deletedAt < deletedBefore ? rowId : nil
      }.sorted().prefix(limit)
      for rowId in expiredIds {
        tombstones[rowId] = nil
      }
      return expiredIds.count
    }
  }
}

private struct VersionedMoveOutModel: ElectricCollectionModel, Equatable {
  let id: String
  let version: Int64

  static var tableName: String { "versioned_move_out" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(
      endpoint: "/shapes/versioned-move-out",
      selectedColumns: ["id", "version"]
    )
  }
  static var moveOutTombstoneTimeToLive: TimeInterval? { 10 }

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
  ) throws -> ProcessedMessage<VersionedMoveOutModel> {
    if message.payload == Data("delete".utf8) {
      if let key = message.key {
        (transaction as? VersionedMoveOutStore)?.delete(id: key)
      }
      return ProcessedMessage(
        records: [],
        metadata: StoreMetadata(
          offset: message.offset,
          handle: message.handle,
          cursor: message.cursor,
          operation: .delete
        )
      )
    }

    let row = try JSONDecoder().decode(VersionedMoveOutRow.self, from: message.payload)
    let model = VersionedMoveOutModel(id: row.id, version: row.version)

    guard let store = transaction as? VersionedMoveOutStore else {
      return processed(model: model, message: message)
    }
    if let tombstone = store.tombstone(rowId: row.id), row.version <= tombstone.version {
      return ProcessedMessage(
        records: [],
        metadata: StoreMetadata(
          offset: message.offset,
          handle: message.handle,
          cursor: message.cursor,
          operation: .insert
        )
      )
    }

    store.upsert(row)
    return processed(model: model, message: message)
  }

  static func versionedRowForMoveOut(
    rowKey: String,
    transaction: Any?
  ) throws -> ElectricVersionedRowIdentifier? {
    guard let store = transaction as? VersionedMoveOutStore,
      let row = store.row(id: rowKey)
    else {
      return nil
    }
    return ElectricVersionedRowIdentifier(rowId: row.id, version: row.version)
  }

  static func recordMoveOutTombstone(
    _ tombstone: ElectricMoveOutTombstone,
    transaction: Any?
  ) throws {
    (transaction as? VersionedMoveOutStore)?.record(tombstone)
  }

  static func removeExpiredMoveOutTombstones(
    deletedBefore: Date,
    limit: Int,
    transaction: Any?
  ) throws -> Int {
    (transaction as? VersionedMoveOutStore)?.deleteExpired(
      deletedBefore: deletedBefore,
      limit: limit
    ) ?? 0
  }

  static func deleteByKey(_ key: String, transaction: Any?) throws {
    (transaction as? VersionedMoveOutStore)?.delete(id: key)
  }

  private static func processed(
    model: VersionedMoveOutModel,
    message: ElectricMessage
  ) -> ProcessedMessage<VersionedMoveOutModel> {
    ProcessedMessage(
      records: [model],
      metadata: StoreMetadata(
        offset: message.offset,
        handle: message.handle,
        cursor: message.cursor,
        operation: .insert
      )
    )
  }
}

@Suite(.serialized) struct MoveOutSemanticsTests {
  /// Declared DNF shapes keep process-local condition state. Declared
  /// statically simple shapes can rebuild from durable ownership after relaunch.
  @Test
  func requiresProcessTrackerContinuityFollowsTombstoneAndCapabilityTruthTable() async throws {
    func continuityRequired(
      tombstonedModel: Bool,
      supportsDurableRowOwnership: Bool,
      protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy,
      shapeTopology: ElectricShapeTopology
    ) async -> Bool {
      let client = ElectricSyncClientImpl(
        configuration: .init(
          metadataProvider: InMemoryMetadataProvider(
            supportsDurableRowOwnership: supportsDurableRowOwnership
          ),
          httpClient: ScriptedHTTPClientProvider(responses: []),
          protocolCapabilityPolicy: protocolCapabilityPolicy
        )
      )
      // VersionedMoveOutModel keeps move-out tombstones; TestMoveOutModel does not.
      return tombstonedModel
        ? await client.requiresProcessTrackerContinuity(
          VersionedMoveOutModel.self,
          shapeTopology: shapeTopology
        )
        : await client.requiresProcessTrackerContinuity(
          TestMoveOutModel.self,
          shapeTopology: shapeTopology
        )
    }

    let policies: [(String, ElectricProtocolCapabilityPolicy)] = [
      ("legacy", .defaultOff),
      ("tagged", .enabled),
    ]
    var observed: [String: Bool] = [:]
    var expected: [String: Bool] = [:]
    for (epochLabel, policy) in policies {
      for tombstonedModel in [false, true] {
        for supportsDurableRowOwnership in [false, true] {
          for shapeTopology in [ElectricShapeTopology.dnf, .staticallySimple] {
            let row =
              "\(epochLabel)|tombstones:\(tombstonedModel)|durable:\(supportsDurableRowOwnership)|topology:\(shapeTopology)"
            observed[row] = await continuityRequired(
              tombstonedModel: tombstonedModel,
              supportsDurableRowOwnership: supportsDurableRowOwnership,
              protocolCapabilityPolicy: policy,
              shapeTopology: shapeTopology
            )
            expected[row] =
              shapeTopology == .dnf
              || (!supportsDurableRowOwnership && (epochLabel == "tagged" || tombstonedModel))
          }
        }
      }
    }

    // This row protects the tombstone conjunct: dropping it forces an unprimed
    // offset=-1 full bootstrap into in-memory clients.
    #expect(observed == expected)
    #expect(
      await continuityRequired(
        tombstonedModel: false,
        supportsDurableRowOwnership: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .dnf
      )
    )
  }

  @Test
  func nonOwnerTrackerLossBatchDefersWithoutPurgingRowsOrMetadata() async throws {
    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let store = VersionedMoveOutStore()
    let ownerClient = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          versionedInsertBatch(version: 1, offset: "owner-offset", tags: ["scope/row-1"])
        ]
      )
    )
    let ownerBatch = try #require(
      try await ownerClient.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .progressive,
        live: false
      )
    )
    _ = try ownerBatch.apply(in: store)
    metadata.seedFetchMetadata(table: VersionedMoveOutModel.tableName)

    let restartedClient = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          versionedInsertBatch(
            rowId: "row-2",
            version: 1,
            offset: "subset-offset",
            tags: ["scope/row-2"],
            isSubset: true
          )
        ]
      )
    )
    let snapshotBatch = try #require(
      try await restartedClient.fetchSnapshot(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(
          predicate: SQLExpression("id = 'row-2'"),
          orderBy: [],
          limit: nil
        ),
        syncMode: .progressive
      )
    )

    do {
      _ = try snapshotBatch.apply(in: store)
      Issue.record("Expected non-owner tracker-loss batch to defer to the stream owner")
    } catch ElectricSyncError.trackerContinuityBootstrapRequired {
    } catch {
      Issue.record("Unexpected tracker-loss deferral error: \(error)")
    }

    #expect(store.row(id: "row-1") == VersionedMoveOutRow(id: "row-1", version: 1))
    #expect(metadata.hasFetchMetadata(table: VersionedMoveOutModel.tableName))
    #expect(
      metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 1
    )
  }

  @Test
  func parsesElectricRowKey() throws {
    let parsed = try #require(ElectricRowKey.parse("\"foo..bar\".\"baz\"/\"a//b\"/_"))
    #expect(parsed.schema == "foo.bar")
    #expect(parsed.table == "baz")
    #expect(parsed.primaryKeyComponents.count == 2)
    #expect(parsed.primaryKeyComponents[0] == "a/b")
    #expect(parsed.primaryKeyComponents[1] == nil)
  }

  @Test
  func moveInRejectsWholeBatchBeforeRowsMetadataOrSharedTrackerAdvance() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: Data("{}".utf8),
            key: "row-1",
            offset: "1",
            handle: "handle",
            kind: .mutation,
            tags: ["pending"]
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "handle",
            kind: .mutation,
            event: .moveIn(patterns: [MovePattern(pos: 0, value: "pending")])
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "handle",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ],
        moveOutBatch(offset: "2"),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )

    do {
      _ = try await client.pollStream(
        TestMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
      Issue.record("Expected move-in protocol input to be quarantined during intake")
    } catch ElectricSyncError.protocolQuarantined(let quarantine) {
      #expect(quarantine.reason == .moveIn)
    } catch {
      Issue.record("Expected a typed move-in quarantine, got \(error)")
    }

    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    #expect(ProcessCapture.keys.snapshot().isEmpty)
    #expect(try metadata.getSyncState(collectionId: streamStateKey, transaction: nil) == nil)

    let laterMoveOutBatch = try #require(
      try await client.pollStream(
        TestMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try laterMoveOutBatch.apply(in: nil)
    #expect(DeleteCapture.keys.snapshot().isEmpty)
  }

  @Test
  func moveInAfterChunkBoundaryQuarantinesBeforeAnyDurableWork() async throws {
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    var messages = (0...200).map { index in
      versionedInsertBatch(
        rowId: "row-\(index)",
        version: Int64(index + 1),
        offset: "\(index)"
      )[0]
    }
    messages.append(
      ElectricMessage(
        payload: Data(),
        offset: "201",
        handle: "handle",
        kind: .mutation,
        event: .moveIn(patterns: [MovePattern(pos: 0, value: "pending")])
      )
    )
    messages.append(
      ElectricMessage(
        payload: Data(),
        offset: "201",
        handle: "handle",
        isUpToDate: true,
        kind: .snapshot,
        control: .upToDate
      )
    )

    let http = ScriptedHTTPClientProvider(responses: [messages])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    do {
      _ = try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
      Issue.record("Expected move-in protocol input to be quarantined during intake")
    } catch ElectricSyncError.protocolQuarantined(let quarantine) {
      #expect(quarantine.reason == .moveIn)
    } catch {
      Issue.record("Expected a typed move-in quarantine, got \(error)")
    }

    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: VersionedMoveOutModel.self,
      basePredicate: nil
    )
    #expect(store.row(id: "row-0") == nil)
    #expect(store.row(id: "row-199") == nil)
    #expect(store.tombstoneCount() == 0)
    #expect(store.removalLimits().isEmpty)
    #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-0") == 0)
    #expect(metadata.ownershipWork().readScopes.isEmpty)
    #expect(metadata.ownershipWork().writeCounts.isEmpty)
    #expect(try metadata.getSyncState(collectionId: streamStateKey, transaction: store) == nil)
  }

  @Test
  func wireResetProjectionDiscardsMoveInBeforeTruncatePreparation() async throws {
    let store = VersionedMoveOutStore()
    store.upsert(VersionedMoveOutRow(id: "row-1", version: 1))
    let metadata = InMemoryMetadataProvider()
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: VersionedMoveOutModel.self,
      basePredicate: nil
    )
    metadata.seedOwnership(
      table: VersionedMoveOutModel.tableName,
      shapeIdentity: streamStateKey,
      tagsByRowKey: ["row-1": ["pending"]]
    )
    let http = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "handle",
            kind: .mutation,
            event: .moveIn(patterns: [MovePattern(pos: 0, value: "pending")])
          ),
          ElectricMessage(
            payload: Data(),
            offset: "-1",
            handle: nil,
            kind: .truncate,
            control: .mustRefetch
          ),
        ]
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let batch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    #expect(batch.messages.count == 1)
    #expect(batch.messages.first?.kind == .truncate)
    #expect(try batch.apply(in: store).encounteredTruncate)
    #expect(store.row(id: "row-1") == VersionedMoveOutRow(id: "row-1", version: 1))
    #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 1)
  }

  // MARK: - DNF tagged-shape batch semantics through SyncBatch.apply

  @Test
  func silentMoveInProducesNoBaseTableWriteOrPublication() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, false]
        ),
        moveEventBatch(
          offset: "2",
          event: .moveIn(patterns: [MovePattern(pos: 1, value: "hash_b")])
        ),
        moveEventBatch(
          offset: "3",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
      ]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: http,
      isExactCursorCutoverEnabled: true
    )

    let insertBatch = try #require(try await pollOnce(client))
    _ = try insertBatch.apply(in: nil)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])

    let moveInBatch = try #require(try await pollOnce(client))
    let moveInOutput = try moveInBatch.apply(in: nil)

    // A move-in alone publishes nothing and writes no base rows.
    #expect(moveInOutput.records.isEmpty)
    #expect(moveInOutput.encounteredTruncate == false)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])
    #expect(DeleteCapture.keys.snapshot().isEmpty)
    // The stream cursor still advances past the silent event.
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "2"
    )

    // The restored condition position keeps the row through the next move-out.
    let moveOutBatch = try #require(try await pollOnce(client))
    _ = try moveOutBatch.apply(in: nil)
    #expect(DeleteCapture.keys.snapshot().isEmpty)
  }

  @Test
  func dnfMoveOutDeletesOnlyAfterNoDisjunctRemains() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, true]
        ),
        moveEventBatch(
          offset: "2",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
        moveEventBatch(
          offset: "3",
          event: .moveOut(patterns: [MovePattern(pos: 1, value: "hash_b")])
        ),
      ]
    )
    let client = taggedShapeClient(metadata: metadata, http: http)

    _ = try #require(try await pollOnce(client)).apply(in: nil)

    // Overlapping disjuncts: losing one condition does not evict.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot().isEmpty)

    // Losing the last visible disjunct emits exactly one synthetic delete.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
  }

  @Test
  func fullyRemovedRowReturnsOnlyThroughLaterElectricChange() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/hash_b"],
          activeConditions: [true, true]
        ),
        moveEventBatch(
          offset: "2",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
        moveEventBatch(
          offset: "3",
          event: .moveIn(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
        dnfInsertBatch(
          rowId: "row-1",
          offset: "4",
          tags: ["hash_a/hash_b"],
          activeConditions: [true, true]
        ),
      ]
    )
    let client = taggedShapeClient(metadata: metadata, http: http)

    _ = try #require(try await pollOnce(client)).apply(in: nil)
    // Single disjunct [0, 1]: one move-out fully removes the row.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])

    // A move-in alone never resurrects a fully removed row.
    let moveInOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(moveInOutput.records.isEmpty)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])

    // Only a later Electric change message supplies the row again.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(ProcessCapture.keys.snapshot() == ["row-1", "row-1"])
  }

  @Test
  func taggedResumeWithEmptyTrackerClearsResumeStateAndFullBootstraps() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    // A previous process observed this stream and persisted a resumable cursor;
    // this process starts with an empty tracker.
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5",
        handle: "handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    let taggedBatch = dnfInsertBatch(
      rowId: "row-1",
      offset: "6",
      tags: ["hash_a/", "/hash_b"],
      activeConditions: [true, true]
    )
    let http = ScriptedHTTPClientProvider(responses: [taggedBatch, taggedBatch])
    let client = taggedShapeClient(
      metadata: metadata,
      http: http,
      isExactCursorCutoverEnabled: true
    )

    // Resuming a tagged stream over an empty tracker must not fold membership
    // state; it clears the resume state and reports a bootstrap boundary.
    let resumedBatch = try #require(try await pollOnce(client))
    let resumedOutput = try resumedBatch.apply(in: nil)
    #expect(resumedOutput.encounteredTruncate)
    #expect(resumedOutput.records.isEmpty)
    #expect(ProcessCapture.keys.snapshot().isEmpty)
    let clearedState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(clearedState.offset == "-1")
    #expect(clearedState.canResumeWithoutFullBootstrap == false)

    // The follow-up full bootstrap applies and re-establishes tracker continuity.
    let bootstrapBatch = try #require(try await pollOnce(client))
    let bootstrapOutput = try bootstrapBatch.apply(in: nil)
    #expect(bootstrapOutput.encounteredTruncate == false)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "6"
    )
  }

  @Test
  func untaggedResumeDoesNotEstablishContinuityForLaterTaggedInput() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5",
        handle: "handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    let http = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: Data(),
            offset: "6",
            handle: "handle",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          )
        ],
        dnfInsertBatch(
          rowId: "row-1",
          offset: "7",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, true]
        ),
      ]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: http,
      isExactCursorCutoverEnabled: true
    )

    // An untagged batch may apply over the resumed cursor, but it cannot prove
    // the tracker observed the stream since bootstrap.
    let untaggedOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(untaggedOutput.encounteredTruncate == false)

    // The first tagged input over that unproven resume forces a full bootstrap.
    let taggedOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(taggedOutput.encounteredTruncate)
    #expect(taggedOutput.requiresReplacementSwap)
    #expect(ProcessCapture.keys.snapshot().isEmpty)
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "-1"
    )
  }

  // MARK: - Durable-ownership (production-path) DNF semantics: persisted tags
  // are membership state, DNF condition state stays owner-scoped in-process.

  @Test
  func durableOwnershipCarriesDnfStateAcrossBatches() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, true]
        ),
        moveEventBatch(
          offset: "2",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
        moveEventBatch(
          offset: "3",
          event: .moveIn(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
        moveEventBatch(
          offset: "4",
          event: .moveOut(patterns: [MovePattern(pos: 1, value: "hash_b")])
        ),
        moveEventBatch(
          offset: "5",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
      ]
    )
    let client = taggedShapeClient(metadata: metadata, http: http)

    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])

    // Disjunct 1 keeps the row; durable tags stay intact for the retained row.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot().isEmpty)

    // The silent move-in re-activates position 0 ACROSS a batch boundary: the
    // per-batch tracker is rebuilt from persisted tags, so the condition state
    // must be carried by the owner-generation tracker.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])

    // Disjunct 0, restored by the move-in, keeps the row when position 1
    // deactivates. Without owner-carried DNF state this batch would fall back
    // to simple tag removal and delete the row here.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot().isEmpty)

    // No disjunct remains: exactly one delete and ownership is released.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
    #expect(metadata.ownerCount(table: TestMoveOutModel.tableName, rowKey: "row-1") == 0)
  }

  @Test
  func durableTaggedResumeWithoutProcessContinuityFullBootstraps() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5",
        handle: "handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    // Durable tags survive a restart; process-local DNF condition state does
    // not. Durable tags alone must never count as tracker continuity.
    metadata.seedOwnership(
      table: TestMoveOutModel.tableName,
      shapeIdentity: streamStateKey,
      tagsByRowKey: ["row-1": ["hash_a/hash_b"]]
    )
    let http = ScriptedHTTPClientProvider(
      responses: [
        moveEventBatch(
          offset: "6",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
        dnfInsertBatch(
          rowId: "row-1",
          offset: "7",
          tags: ["hash_a/hash_b"],
          activeConditions: [true, true]
        ),
      ]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: http,
      isExactCursorCutoverEnabled: true
    )

    // Tagged input over the resumed cursor with an unestablished owner tracker
    // clears resume state and full-bootstraps instead of applying the move-out
    // with lost DNF state.
    let resumedOutput = try #require(
      try await pollOnce(client, shapeTopology: .dnf)
    ).apply(in: nil)
    #expect(resumedOutput.encounteredTruncate)
    #expect(resumedOutput.requiresReplacementSwap)
    #expect(DeleteCapture.keys.snapshot().isEmpty)
    let clearedState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(clearedState.offset == "-1")
    #expect(clearedState.canResumeWithoutFullBootstrap == false)

    // The follow-up full bootstrap applies and re-establishes continuity.
    let bootstrapOutput = try #require(
      try await pollOnce(client, shapeTopology: .dnf)
    ).apply(in: nil)
    #expect(bootstrapOutput.encounteredTruncate == false)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "7"
    )
  }

  @Test
  func legacyDurableSimpleTagResumeStillAppliesWithoutBootstrap() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5",
        handle: "handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: nil
      ),
      transaction: nil
    )
    metadata.seedOwnership(
      table: TestMoveOutModel.tableName,
      shapeIdentity: streamStateKey,
      tagsByRowKey: ["row-1": ["h1"]]
    )
    // Capability OFF: the legacy simple-shape contract holds — persisted tags
    // are the complete membership state, so a resumed move-out applies
    // directly without a forced bootstrap.
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            moveEventBatch(
              offset: "6",
              event: .moveOut(patterns: [MovePattern(pos: 0, value: "h1")])
            )
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true
      )
    )

    let output = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(output.encounteredTruncate == false)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
    #expect(metadata.ownerCount(table: TestMoveOutModel.tableName, rowKey: "row-1") == 0)
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "6"
    )
  }

  @Test
  func taggedSimpleExactResumeRebuildsMembershipAndEvictsMoveOut() async throws {
    DeleteCapture.keys.reset()
    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let predicate = SQLExpression(predicate: .constant(true))
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: predicate
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5", handle: "handle", cursor: nil, isUpToDate: true, lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    metadata.seedOwnership(
      table: TestMoveOutModel.tableName,
      shapeIdentity: streamStateKey,
      tagsByRowKey: ["row-1": ["h1"]]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          moveEventBatch(offset: "6", event: .moveOut(patterns: [MovePattern(pos: 0, value: "h1")]))
        ]
      ),
      isExactCursorCutoverEnabled: true
    )

    let output = try #require(
      try await pollOnce(
        client,
        basePredicate: predicate,
        shapeTopology: .staticallySimple
      )
    ).apply(in: nil)
    #expect(output.encounteredTruncate == false)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
  }

  @Test
  func declaredSimpleShapeLatchesToDNFAfterOneActiveConditionsRejection() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()
    // Isolate the process-local topology latch; durable ownership has its own
    // lifecycle coverage and is not part of this tracker transition.
    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5", handle: "handle", cursor: nil, isUpToDate: true, lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          dnfInsertBatch(
            rowId: "row-1",
            offset: "6",
            tags: ["h1"],
            activeConditions: [true]
          ),
          dnfInsertBatch(
            rowId: "row-2",
            offset: "7",
            tags: ["hash_a/", "/hash_b"],
            activeConditions: [true, false]
          ),
          moveEventBatch(
            offset: "8",
            event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
          ),
        ]
      ),
      isExactCursorCutoverEnabled: true
    )

    let firstOutput = try #require(
      try await pollOnce(client, shapeTopology: .staticallySimple)
    ).apply(in: nil)
    #expect(firstOutput.encounteredTruncate)
    #expect(firstOutput.requiresReplacementSwap)
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "-1"
    )

    // The rejection is process-latched before the synthetic reset commits.
    // The next active_conditions batch keeps the declared-simple call site but
    // folds as DNF rather than triggering a second bootstrap.
    firstOutput.transactionDidCommit()
    let secondOutput = try #require(
      try await pollOnce(client, shapeTopology: .staticallySimple)
    ).apply(in: nil)
    #expect(secondOutput.encounteredTruncate == false)
    #expect(secondOutput.requiresReplacementSwap == false)
    #expect(ProcessCapture.keys.snapshot() == ["row-2"])
    secondOutput.transactionDidCommit()

    // `activeConditions: [true, false]` makes only the first disjunct live;
    // a matching move-out therefore deletes row-2. Simple-tag handling would
    // retain the second tag, so this proves the latched batch used DNF state.
    let thirdOutput = try #require(
      try await pollOnce(client, shapeTopology: .staticallySimple)
    ).apply(in: nil)
    thirdOutput.transactionDidCommit()
    #expect(DeleteCapture.keys.snapshot() == ["row-2"])
  }

  @Test
  func activeConditionsRejectionLogsThroughConfiguredProvider() async throws {
    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let logger = RecordingLogProvider()
    let predicate = SQLExpression("provider_log_test = true")
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: predicate
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5", handle: "handle", cursor: nil, isUpToDate: true, lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    metadata.seedOwnership(
      table: TestMoveOutModel.tableName,
      shapeIdentity: streamStateKey,
      tagsByRowKey: ["resumed-row": ["h1"]]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          dnfInsertBatch(
            rowId: "row-1",
            offset: "6",
            tags: ["h1"],
            activeConditions: [true]
          )
        ]
      ),
      isExactCursorCutoverEnabled: true,
      logger: logger
    )

    let output = try #require(
      try await pollOnce(
        client,
        basePredicate: predicate,
        shapeTopology: .staticallySimple
      )
    ).apply(in: nil)

    #expect(output.encounteredTruncate)
    #expect(output.requiresReplacementSwap)
    #expect(
      logger.entries().contains(
        .init(
          level: .warning,
          message: "electric_tracker_rebuild_active_conditions_rejected count=1 latched_dnf=true",
          metadata: [
            "table": TestMoveOutModel.tableName,
            "collection": TestMoveOutModel.collectionIdentifier,
            "tracker_rebuild.active_conditions_rejected": "true",
            "tracker_rebuild.active_conditions_latched_dnf": "true",
          ]
        )
      )
    )
  }

  // Tracker-loss recovery must be ATOMIC through the collection path: the
  // synthetic reset carries no truncate message, so the owner loop arms the
  // replacement swap from the apply output and the next snapshot replaces
  // stale rows via prepareTruncateSwap instead of layering on top of them.
  @Test
  func trackerLossResetArmsReplacementSwapThroughSubscribe() async throws {
    SwapCaptures.rows.reset()
    SwapCaptures.truncateCalls.reset()
    SwapCaptures.rows.insert("stale-row")

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TruncateSwapModel.self,
      basePredicate: nil
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5",
        handle: "handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )

    let taggedBatch: [ElectricMessage] = [
      ElectricMessage(
        payload: Data("{}".utf8),
        key: "row-1",
        offset: "6",
        handle: "handle",
        kind: .mutation,
        tags: ["hash_a/", "/hash_b"],
        activeConditions: [true, true]
      ),
      ElectricMessage(
        payload: Data(),
        offset: "6",
        handle: "handle",
        isUpToDate: true,
        kind: .snapshot,
        control: .upToDate
      ),
    ]
    let http = ScriptedHTTPClientProvider(responses: [taggedBatch, taggedBatch])
    let eventHandler = TruncateHookCounter()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        eventHandler: eventHandler,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { true }
        )
      )
    )

    let collection = ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: TruncateSwapModel.self,
        syncMode: .onDemand,
        live: false
      ),
      client: client,
      cacheProvider: EmptyCacheProvider(),
      transactionRunner: { operation in try operation(nil) },
      eventHandler: eventHandler
    )

    let stream = collection.subscribe(circuitBreaker: StaticDelayBreaker(delay: 60))
    let consumer = Task {
      for await _ in stream {}
    }

    await eventHandler.waitForDidTruncateCount(1)

    // The stale pre-reset row was replaced atomically: exactly one truncate
    // swap ran before the replacement snapshot, and only snapshot rows remain.
    #expect(SwapCaptures.truncateCalls.snapshot().count == 1)
    #expect(SwapCaptures.rows.snapshot() == ["row-1"])
    #expect(await eventHandler.willTruncateCount() == 1)
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "6"
    )

    consumer.cancel()
    await consumer.value
  }

  // MARK: - Live capability epoch flips (activation/rollback without a client
  // rebuild): a gate flip is a tracker-generation boundary, never a silent
  // reuse of state folded under the other segment semantics.

  @Test(.timeLimit(.minutes(1)))
  func liveCapabilityRollbackReplacesDurableStreamThroughSubscribe() async throws {
    SwapCaptures.rows.reset()
    SwapCaptures.truncateCalls.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let taggedShapeEnabled = CapabilityFlag(true)
    let http = HookedScriptedHTTPClientProvider(
      responses: [
        // Tagged epoch: DNF row visible through two disjuncts.
        [
          ElectricMessage(
            payload: Data("{}".utf8),
            key: "row-1",
            offset: "1",
            handle: "handle",
            kind: .mutation,
            tags: ["hash_a/", "/hash_b"],
            activeConditions: [true, true]
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "handle",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ],
        moveEventBatch(
          offset: "2",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
        // The owner detects the epoch transition before consuming this
        // response. It first commits a synthetic reset, then applies this as
        // the atomic replacement bootstrap under legacy segment semantics.
        [
          ElectricMessage(
            payload: Data("{}".utf8),
            key: "row-2",
            offset: "3",
            handle: "handle-2",
            kind: .mutation,
            tags: ["scope/target", "other/_"]
          ),
          ElectricMessage(
            payload: Data(),
            offset: "3",
            handle: "handle-2",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ],
        // Legacy "_" wildcard semantics must be active only after the
        // replacement bootstrap. Both tags match position 1, so row-2 evicts.
        moveEventBatch(
          offset: "4",
          event: .moveOut(patterns: [MovePattern(pos: 1, value: "target")])
        ),
      ],
      // Flip while the final tagged batch is in flight. Its frozen epoch still
      // applies tagged DNF semantics; the following owner iteration fences the
      // transition before issuing another HTTP request.
      hooks: [1: { taggedShapeEnabled.value = false }]
    )
    let eventHandler = TruncateHookCounter()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        eventHandler: eventHandler,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { taggedShapeEnabled.value }
        )
      )
    )

    let collection = ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: TruncateSwapModel.self,
        syncMode: .onDemand,
        live: false
      ),
      client: client,
      cacheProvider: EmptyCacheProvider(),
      transactionRunner: { operation in try operation(nil) },
      eventHandler: eventHandler
    )

    let stream = collection.subscribe(circuitBreaker: StaticDelayBreaker(delay: 60))
    let consumer = Task {
      for await _ in stream {}
    }

    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TruncateSwapModel.self,
      basePredicate: nil
    )
    await http.waitForRequestCount(4)
    while true {
      let persistedOffset =
        try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset
      if SwapCaptures.rows.snapshot().isEmpty, persistedOffset == "4" { break }
      try Task.checkCancellation()
      await Task.yield()
    }
    await eventHandler.waitForDidTruncateCount(2)

    // The pristine DNF owner established its initial authoritative generation,
    // then the rollback replaced that stream atomically. The legacy underscore
    // wildcard subsequently evicted the replacement row through the durable
    // provider path.
    // A durable-ownership swap replaces rows via ownership-scoped
    // releaseAllRowOwnership + deleteByKey (sibling-shape safe); it never
    // blanket-truncates the table, so no T.truncate call is expected here —
    // the will/did truncate hooks prove both authoritative swap boundaries fired.
    #expect(SwapCaptures.rows.snapshot().isEmpty)
    #expect(SwapCaptures.truncateCalls.snapshot().isEmpty)
    #expect(await eventHandler.willTruncateCount() == 2)
    #expect(metadata.ownerCount(table: TruncateSwapModel.tableName, rowKey: "row-1") == 0)
    #expect(metadata.ownerCount(table: TruncateSwapModel.tableName, rowKey: "row-2") == 0)
    let finalState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(finalState.offset == "4")
    #expect(finalState.protocolSemanticEpoch == .legacy)

    consumer.cancel()
    await consumer.value
  }

  @Test
  func liveCapabilityRollbackStartsLegacyGenerationForNonDurableStream() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let taggedShapeEnabled = CapabilityFlag(true)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, true]
        ),
        simpleInsertBatch(rowId: "row-2", offset: "2", tags: ["a/b", "c/_"]),
        moveEventBatch(
          offset: "3",
          event: .moveOut(patterns: [MovePattern(pos: 1, value: "b")])
        ),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { taggedShapeEnabled.value }
        )
      )
    )

    // Tagged epoch bootstrap pins the owner generation to tagged semantics.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])

    // Live rollback: the next batch is a generation boundary, not a fold into
    // the tagged tracker — no client rebuild and no manual reset() involved.
    taggedShapeEnabled.value = false
    let epochOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(epochOutput.encounteredTruncate)
    #expect(epochOutput.requiresReplacementSwap)
    #expect(DeleteCapture.keys.snapshot().isEmpty)
    #expect(await http.requestCount() == 1)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "-1"
    )
    epochOutput.transactionDidCommit()

    // The fresh generation runs the legacy contract: "_" wildcard-matches the
    // move-out removal, so row-2 fully evicts.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(ProcessCapture.keys.snapshot() == ["row-1", "row-2"])
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["row-2"])
  }

  @Test
  func persistedEpochFencesRestartBeforeLegacyResponseIsConsumed() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let taggedHTTP = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, true]
        )
      ]
    )
    let taggedClient = taggedShapeClient(
      metadata: metadata,
      http: taggedHTTP,
      isExactCursorCutoverEnabled: true
    )
    _ = try #require(try await pollOnce(taggedClient)).apply(in: nil)

    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    let taggedState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(taggedState.protocolSemanticEpoch == .taggedShape1_7_7)

    // A rebuilt client has no process tracker, so only the persisted epoch can
    // prove that its legacy gate disagrees with the durable tagged cursor.
    let legacyHTTP = ScriptedHTTPClientProvider(
      responses: [
        simpleInsertBatch(
          rowId: "row-2",
          offset: "2",
          tags: ["scope/target", "other/_"]
        )
      ]
    )
    let restartedClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: legacyHTTP,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true
      )
    )

    let resetBatch = try #require(try await pollOnce(restartedClient))
    #expect(resetBatch.messages.isEmpty)
    #expect(await legacyHTTP.requestCount() == 0)
    let resetOutput = try resetBatch.apply(in: nil)
    #expect(resetOutput.encounteredTruncate)
    #expect(resetOutput.requiresReplacementSwap)

    let resetState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(resetState.offset == "-1")
    #expect(resetState.protocolSemanticEpoch == .legacy)

    resetOutput.transactionDidCommit()
    let bootstrapBatch = try #require(try await pollOnce(restartedClient))
    #expect(await legacyHTTP.requestCount() == 1)
    let bootstrapOutput = try bootstrapBatch.apply(in: nil)
    #expect(!bootstrapOutput.encounteredTruncate)
    #expect(ProcessCapture.keys.snapshot() == ["row-1", "row-2"])
  }

  @Test
  func semanticEpochTrackerResetWaitsForTransactionCommitAcknowledgement() async throws {
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let taggedShapeEnabled = CapabilityFlag(true)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, true]
        ),
        simpleInsertBatch(rowId: "row-2", offset: "2", tags: ["scope/target", "other/_"]),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { taggedShapeEnabled.value }
        )
      )
    )
    _ = try #require(try await pollOnce(client)).apply(in: nil)

    taggedShapeEnabled.value = false
    let resetBatch = try #require(try await pollOnce(client))
    #expect(await http.requestCount() == 1)
    let resetOutput = try resetBatch.apply(in: nil)
    #expect(resetOutput.requiresReplacementSwap)

    // Persisted state has changed in the transaction context, but the owner
    // tracker must remain tagged until the transaction runner confirms commit.
    let retryBeforeCommit = try #require(try await pollOnce(client))
    #expect(retryBeforeCommit.messages.isEmpty)
    #expect(await http.requestCount() == 1)

    resetOutput.transactionDidCommit()
    do {
      _ = try await client.requestSnapshot(
        TestMoveOutModel.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(
          predicate: SQLExpression("id = 'row-2'"),
          orderBy: [],
          limit: nil
        ),
        syncMode: .onDemand
      )
      Issue.record("Expected the subset to wait for the owner bootstrap")
    } catch ElectricSyncError.capabilitySemanticEpochTransitionDeferred {
    } catch {
      Issue.record("Unexpected pending-bootstrap subset error: \(error)")
    }
    #expect(await http.requestCount() == 1)

    let bootstrapBatch = try #require(try await pollOnce(client))
    #expect(await http.requestCount() == 2)
    _ = try bootstrapBatch.apply(in: nil)
    #expect(ProcessCapture.keys.snapshot() == ["row-1", "row-2"])
  }

  @Test
  func subsetDefersLegacyToTaggedTransitionToStreamOwner() async throws {
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let taggedShapeEnabled = CapabilityFlag(false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        simpleInsertBatch(rowId: "row-1", offset: "1", tags: ["scope/target"]),
        dnfInsertBatch(
          rowId: "row-2",
          offset: "2",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, true]
        ),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { taggedShapeEnabled.value }
        )
      )
    )
    _ = try #require(try await pollOnce(client)).apply(in: nil)

    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    taggedShapeEnabled.value = true
    do {
      _ = try await client.requestSnapshot(
        TestMoveOutModel.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(
          predicate: SQLExpression("id = 'row-2'"),
          orderBy: [],
          limit: nil
        ),
        syncMode: .onDemand
      )
      Issue.record("Expected the subset to defer the semantic epoch transition")
    } catch ElectricSyncError.capabilitySemanticEpochTransitionDeferred {
    } catch {
      Issue.record("Unexpected subset transition error: \(error)")
    }

    #expect(await http.requestCount() == 1)
    let unchangedState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(unchangedState.offset == "1")
    #expect(unchangedState.protocolSemanticEpoch == .legacy)
    #expect(metadata.ownerCount(table: TestMoveOutModel.tableName, rowKey: "row-1") == 1)

    let ownerReset = try #require(try await pollOnce(client))
    #expect(ownerReset.messages.isEmpty)
    #expect(await http.requestCount() == 1)
    let ownerResetOutput = try ownerReset.apply(in: nil)
    #expect(ownerResetOutput.requiresReplacementSwap)
    ownerResetOutput.transactionDidCommit()
  }

  @Test
  func transientSubsetDefersTaggedToLegacyTransitionToStreamOwner() async throws {
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: true)
    let taggedShapeEnabled = CapabilityFlag(true)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/", "/hash_b"],
          activeConditions: [true, true]
        ),
        simpleInsertBatch(rowId: "row-2", offset: "2", tags: ["scope/target", "other/_"]),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { taggedShapeEnabled.value }
        )
      )
    )
    _ = try #require(try await pollOnce(client)).apply(in: nil)

    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    taggedShapeEnabled.value = false
    do {
      _ = try await client.fetchSnapshot(
        TestMoveOutModel.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(
          predicate: SQLExpression("id = 'row-2'"),
          orderBy: [],
          limit: nil
        ),
        syncMode: .onDemand
      )
      Issue.record("Expected the transient subset to defer the semantic epoch transition")
    } catch ElectricSyncError.capabilitySemanticEpochTransitionDeferred {
    } catch {
      Issue.record("Unexpected transient subset transition error: \(error)")
    }

    #expect(await http.requestCount() == 1)
    let unchangedState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(unchangedState.offset == "1")
    #expect(unchangedState.protocolSemanticEpoch == .taggedShape1_7_7)
    #expect(metadata.ownerCount(table: TestMoveOutModel.tableName, rowKey: "row-1") == 1)

    let ownerReset = try #require(try await pollOnce(client))
    #expect(ownerReset.messages.isEmpty)
    #expect(await http.requestCount() == 1)
    let ownerResetOutput = try ownerReset.apply(in: nil)
    #expect(ownerResetOutput.requiresReplacementSwap)
    ownerResetOutput.transactionDidCommit()
  }

  @Test
  func moveOutEventGeneratesSyntheticDeletes() async throws {
    DeleteCapture.keys.reset()

    let metadataProvider = InMemoryMetadataProvider()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: Data("{\"op\":\"insert\"}".utf8),
            key: "row-1",
            offset: "0",
            handle: "h",
            kind: .mutation,
            tags: ["tagA"]
          ),
          ElectricMessage(
            payload: Data(),
            offset: "0",
            handle: "h",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ],
        [
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "h",
            kind: .mutation,
            event: .moveOut(patterns: [MoveOutPattern(pos: 0, value: "tagA")])
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "h",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ],
      ]
    )

    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider)
      )
    )

    let batch1 = try #require(
      try await client.pollStream(
        TestMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try batch1.apply(in: nil)

    let batch2 = try #require(
      try await client.pollStream(
        TestMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try batch2.apply(in: nil)

    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
  }

  /// End-to-end (client -> MoveOutTagTracker -> deleteByKey) verification with a
  /// MULTI-SEGMENT membership tag, exactly as Electric emits it (`shape.ex`
  /// joins segments with "/"). Before the "/"-delimiter fix the tag parsed as a
  /// single segment, so the position-1 move-out never matched and no synthetic
  /// delete fired — the row would linger on the device.
  @Test
  func multiSegmentSlashTagMoveOutGeneratesSyntheticDelete() async throws {
    DeleteCapture.keys.reset()

    let metadataProvider = InMemoryMetadataProvider()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: Data("{\"op\":\"insert\"}".utf8),
            key: "row-1",
            offset: "0",
            handle: "h",
            kind: .mutation,
            tags: ["calA/calB"]
          ),
          ElectricMessage(
            payload: Data(),
            offset: "0",
            handle: "h",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ],
        [
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "h",
            kind: .mutation,
            event: .moveOut(patterns: [MoveOutPattern(pos: 1, value: "calB")])
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "h",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ],
      ]
    )

    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider)
      )
    )

    let batch1 = try #require(
      try await client.pollStream(
        TestMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try batch1.apply(in: nil)

    let batch2 = try #require(
      try await client.pollStream(
        TestMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try batch2.apply(in: nil)

    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
  }

  @Test
  func ordinaryApplyPersistsOwnershipWorkProportionalToChangedRows() async throws {
    let metadata = InMemoryMetadataProvider()
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    metadata.seedOwnership(
      table: TestMoveOutModel.tableName,
      shapeIdentity: streamStateKey,
      tagsByRowKey: Dictionary(
        uniqueKeysWithValues: (0..<1_000).map { ("row-\($0)", ["scope-\($0)"]) }
      )
    )
    let http = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: Data("{}".utf8),
            key: "row-500",
            offset: "1",
            handle: "handle",
            kind: .mutation,
            tags: ["scope-updated"]
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "handle",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ]
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )

    let batch = try #require(
      try await client.pollStream(
        TestMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try batch.apply(in: nil)

    let work = metadata.ownershipWork()
    #expect(work.readScopes == [Set(["row-500"])])
    #expect(work.writeCounts == [1])
  }

  @Test
  func untaggedMaterializedRowKeepsOwnershipAcrossEmptyBatches() async throws {
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            versionedInsertBatch(version: 1, offset: "insert"),
            [
              ElectricMessage(
                payload: Data(),
                offset: "empty",
                handle: "handle",
                isUpToDate: true,
                kind: .snapshot,
                control: .upToDate
              )
            ],
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )

    for _ in 0..<2 {
      let batch = try #require(
        try await client.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try batch.apply(in: store)
    }

    #expect(store.row(id: "row-1")?.version == 1)
    #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 1)
  }

  @Test
  func sameBatchMoveOutWinsOverEarlierMaterialization() async throws {
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    let insert = try #require(
      versionedInsertBatch(version: 1, offset: "1", tags: ["pending"]).first
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            [
              insert,
              ElectricMessage(
                payload: Data(),
                offset: "2",
                handle: "handle",
                kind: .mutation,
                event: .moveOut(patterns: [MoveOutPattern(pos: 0, value: "pending")])
              ),
              ElectricMessage(
                payload: Data(),
                offset: "2",
                handle: "handle",
                isUpToDate: true,
                kind: .snapshot,
                control: .upToDate
              ),
            ]
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )

    let batch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try batch.apply(in: store)

    #expect(store.row(id: "row-1") == nil)
    #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 0)
  }

  @Test
  func sameBatchDeleteWinsOverEarlierMaterialization() async throws {
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    let insert = try #require(
      versionedInsertBatch(version: 1, offset: "1", tags: ["pending"]).first
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            [
              insert,
              ElectricMessage(
                payload: Data("delete".utf8),
                key: "row-1",
                offset: "2",
                handle: "handle",
                kind: .mutation
              ),
              ElectricMessage(
                payload: Data(),
                offset: "2",
                handle: "handle",
                isUpToDate: true,
                kind: .snapshot,
                control: .upToDate
              ),
            ]
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )

    let batch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try batch.apply(in: store)

    #expect(store.row(id: "row-1") == nil)
    #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 0)
  }

  @Test
  func truncateDropsEarlierMaterializationBeforeReplacementCleanup() async throws {
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    let insert = try #require(
      versionedInsertBatch(version: 1, offset: "1", tags: ["pending"]).first
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            [
              insert,
              ElectricMessage(
                payload: Data(),
                offset: "2",
                handle: "handle",
                kind: .truncate
              ),
            ],
            [
              ElectricMessage(
                payload: Data(),
                offset: "3",
                handle: "replacement",
                isUpToDate: true,
                kind: .snapshot,
                control: .upToDate
              )
            ],
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )

    let truncateBatch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    #expect(try truncateBatch.apply(in: store).encounteredTruncate)
    #expect(store.row(id: "row-1") == nil)
    #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 0)

    let replacementBatch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    try replacementBatch.prepareTruncateSwap(in: store)
    _ = try replacementBatch.apply(in: store)

    #expect(store.row(id: "row-1") == nil)
    #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 0)
  }

  @Test
  func overlappingShapesDeleteOnlyAfterLastShapeMovesOut() async throws {
    let now = Date(timeIntervalSince1970: 9_000)
    let runtimeProvider = fixedRuntimeProvider(now)
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    let firstPredicate = SQLExpression("scope = 'first'")
    let secondPredicate = SQLExpression("scope = 'second'")
    let firstClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            versionedInsertBatch(version: 1, offset: "first-1", tags: ["shape-first"])
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        runtimeProvider: runtimeProvider
      )
    )
    let secondClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            versionedInsertBatch(version: 1, offset: "second-1", tags: ["shape-second"])
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        runtimeProvider: runtimeProvider
      )
    )

    do {
      let firstInsert = try #require(
        try await firstClient.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: firstPredicate,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try firstInsert.apply(in: store)

      let secondInsert = try #require(
        try await secondClient.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: secondPredicate,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try secondInsert.apply(in: store)

      #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 2)

      let restartedFirstClient = ElectricSyncClientImpl(
        configuration: .init(
          metadataProvider: metadata,
          httpClient: ScriptedHTTPClientProvider(
            responses: [moveOutBatch(offset: "first-2", tag: "shape-first")]
          ),
          fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
          runtimeProvider: runtimeProvider
        )
      )
      let restartedSecondClient = ElectricSyncClientImpl(
        configuration: .init(
          metadataProvider: metadata,
          httpClient: ScriptedHTTPClientProvider(
            responses: [moveOutBatch(offset: "second-2", tag: "shape-second")]
          ),
          fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
          runtimeProvider: runtimeProvider
        )
      )

      let firstMoveOut = try #require(
        try await restartedFirstClient.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: firstPredicate,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try firstMoveOut.apply(in: store)

      #expect(store.row(id: "row-1")?.version == 1)
      #expect(store.tombstone(rowId: "row-1") == nil)
      #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 1)

      let secondMoveOut = try #require(
        try await restartedSecondClient.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: secondPredicate,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try secondMoveOut.apply(in: store)

      #expect(store.row(id: "row-1") == nil)
      #expect(store.tombstone(rowId: "row-1")?.version == 1)
      #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 0)
    }
  }

  @Test
  func shapeResetPreservesRowsOwnedBySiblingShape() async throws {
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    let firstPredicate = SQLExpression("scope = 'first'")
    let secondPredicate = SQLExpression("scope = 'second'")
    let firstClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            versionedInsertBatch(
              rowId: "first-only",
              version: 1,
              offset: "first-1",
              tags: ["shape-first"]
            ),
            versionedInsertBatch(
              rowId: "shared",
              version: 1,
              offset: "first-2"
            ),
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let secondClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            versionedInsertBatch(
              rowId: "second-only",
              version: 1,
              offset: "second-1",
              tags: ["shape-second"]
            ),
            versionedInsertBatch(
              rowId: "shared",
              version: 1,
              offset: "second-2"
            ),
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )

    for _ in 0..<2 {
      let firstBatch = try #require(
        try await firstClient.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: firstPredicate,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try firstBatch.apply(in: store)

      let secondBatch = try #require(
        try await secondClient.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: secondPredicate,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try secondBatch.apply(in: store)
    }

    let resetClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            versionedInsertBatch(
              rowId: "shared",
              version: 2,
              offset: "first-reset"
            )
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let freshFirstBatch = try #require(
      try await resetClient.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: firstPredicate,
        syncMode: .onDemand,
        live: false
      )
    )

    try freshFirstBatch.prepareTruncateSwap(in: store)
    _ = try freshFirstBatch.apply(in: store)

    #expect(store.row(id: "first-only") == nil)
    #expect(store.row(id: "second-only")?.version == 1)
    #expect(store.row(id: "shared")?.version == 2)
    #expect(
      metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "second-only") == 1
    )
    #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "shared") == 2)
  }

  @Test
  func durableTruncateDropsPreResetMoveOutTags() async throws {
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [
            versionedInsertBatch(version: 1, offset: "1", tags: ["stale"]),
            [
              ElectricMessage(
                payload: Data(),
                offset: "2",
                handle: "handle",
                kind: .truncate
              )
            ],
            versionedInsertBatch(version: 2, offset: "3", tags: ["current"]),
            moveOutBatch(offset: "4", tag: "stale"),
          ]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )

    let initialBatch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try initialBatch.apply(in: store)

    let truncateBatch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    #expect(try truncateBatch.apply(in: store).encounteredTruncate)
    try truncateBatch.prepareTruncateSwap(in: store)

    let replacementBatch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try replacementBatch.apply(in: store)

    let staleMoveOutBatch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try staleMoveOutBatch.apply(in: store)

    #expect(store.row(id: "row-1")?.version == 2)
  }

  @Test
  func lateSnapshotIsFencedAfterMoveOutAndNewerInsertClearsTombstone() async throws {
    let now = Date(timeIntervalSince1970: 10_000)
    let runtimeProvider = fixedRuntimeProvider(now)
    do {
      let store = VersionedMoveOutStore()
      let metadataProvider = InMemoryMetadataProvider()
      let httpClient = ScriptedHTTPClientProvider(
        responses: [
          versionedInsertBatch(version: 1, offset: "1", tags: ["pending"]),
          versionedInsertBatch(version: 1, offset: "snapshot-1", isSubset: true),
          moveOutBatch(offset: "2"),
          versionedInsertBatch(version: 2, offset: "3", tags: ["pending"]),
        ]
      )
      let client = ElectricSyncClientImpl(
        configuration: .init(
          metadataProvider: metadataProvider,
          httpClient: httpClient,
          fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider),
          runtimeProvider: runtimeProvider
        )
      )

      let initialBatch = try #require(
        try await client.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try initialBatch.apply(in: store)

      let staleSnapshot = try #require(
        try await client.fetchSnapshot(
          VersionedMoveOutModel.self,
          basePredicate: nil,
          descriptor: QueryDescriptor(predicate: SQLExpression("id = 'row-1'")),
          syncMode: .onDemand
        )
      )

      let moveOut = try #require(
        try await client.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try moveOut.apply(in: store)

      #expect(store.row(id: "row-1") == nil)
      #expect(store.tombstone(rowId: "row-1")?.version == 1)

      _ = try staleSnapshot.apply(in: store)

      #expect(store.row(id: "row-1") == nil)
      #expect(store.tombstone(rowId: "row-1")?.version == 1)

      let newerInsert = try #require(
        try await client.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try newerInsert.apply(in: store)

      #expect(store.row(id: "row-1")?.version == 2)
      #expect(store.tombstone(rowId: "row-1") == nil)
    }
  }

  @Test
  func fetchSnapshotDoesNotOwnExpiredTombstoneCleanup() async throws {
    let store = VersionedMoveOutStore()
    let metadataProvider = InMemoryMetadataProvider()
    let deletedAt = Date(timeIntervalSince1970: 20_000)
    let dateSource = MutableTestDateSource(deletedAt)
    let runtimeProvider = ElectricSyncRuntimeProvider(
      now: { dateSource.now() },
      makeUUID: UUID.init,
      sleep: { duration in try await Task.sleep(for: duration) }
    )
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        versionedInsertBatch(version: 1, offset: "1", tags: ["pending"]),
        moveOutBatch(offset: "2"),
        versionedInsertBatch(version: 1, offset: "snapshot-fresh", isSubset: true),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider),
        runtimeProvider: runtimeProvider
      )
    )

    let initialBatch = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    do {
      _ = try initialBatch.apply(in: store)
    }

    let moveOut = try #require(
      try await client.pollStream(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    do {
      _ = try moveOut.apply(in: store)
    }
    #expect(store.tombstone(rowId: "row-1") != nil)

    let freshSnapshot = try #require(
      try await client.fetchSnapshot(
        VersionedMoveOutModel.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(predicate: SQLExpression("id = 'row-1'")),
        syncMode: .onDemand
      )
    )
    dateSource.set(deletedAt.addingTimeInterval(11))
    do {
      _ = try freshSnapshot.apply(in: store)
    }

    #expect(store.tombstone(rowId: "row-1")?.version == 1)
    #expect(store.row(id: "row-1") == nil)
  }

  @Test
  func expiredTombstoneCleanupIsBoundedOncePerBatchAndDrainsAcrossApplies() async throws {
    let now = Date(timeIntervalSince1970: 30_000)
    let runtimeProvider = fixedRuntimeProvider(now)
    let store = VersionedMoveOutStore()
    for index in 0..<(electricMoveOutTombstoneCleanupBatchSize + 5) {
      store.record(
        ElectricMoveOutTombstone(
          tableName: VersionedMoveOutModel.tableName,
          rowId: "expired-\(index)",
          streamStateKey: VersionedMoveOutModel.collectionIdentifier,
          version: 1,
          offset: nil,
          cursor: nil,
          deletedAt: now.addingTimeInterval(-11)
        )
      )
    }

    let metadataProvider = InMemoryMetadataProvider()
    let httpClient = ScriptedHTTPClientProvider(
      responses: [
        versionedInsertBatch(version: 1, offset: "1"),
        versionedInsertBatch(version: 2, offset: "2"),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadataProvider,
        httpClient: httpClient,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadataProvider),
        runtimeProvider: runtimeProvider
      )
    )

    do {
      let firstBatch = try #require(
        try await client.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      for chunk in firstBatch.chunked(maxMessages: 1) {
        _ = try chunk.apply(in: store)
      }

      #expect(store.tombstoneCount() == 5)
      #expect(store.removalLimits() == [electricMoveOutTombstoneCleanupBatchSize])

      let secondBatch = try #require(
        try await client.pollStream(
          VersionedMoveOutModel.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      for chunk in secondBatch.chunked(maxMessages: 1) {
        _ = try chunk.apply(in: store)
      }

      #expect(store.tombstoneCount() == 0)
      #expect(
        store.removalLimits() == [
          electricMoveOutTombstoneCleanupBatchSize,
          electricMoveOutTombstoneCleanupBatchSize,
        ]
      )
    }
  }

  // MARK: - Tracker-discontinuity matrix (tracker-loss bootstrap:
  // owner GC/reacquire, suspension, process restart, reset/handle replacement,
  // account switch, resume-identity mismatch). Process restart itself is
  // `taggedResumeWithEmptyTrackerClearsResumeStateAndFullBootstraps` above.

  @Test
  func ownerGCReacquireAfterCursorAdvanceFullBootstrapsTaggedStream() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let bootstrapBatch = dnfInsertBatch(
      rowId: "row-1",
      offset: "1",
      tags: ["hash_a/", "/hash_b"],
      activeConditions: [true, true]
    )
    let ownerClient = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          bootstrapBatch,
          moveEventBatch(
            offset: "2",
            event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
          ),
        ]
      ),
      isExactCursorCutoverEnabled: true
    )

    // The first owner bootstraps and advances the durable cursor mid-stream.
    _ = try #require(try await pollOnce(ownerClient)).apply(in: nil)
    _ = try #require(try await pollOnce(ownerClient)).apply(in: nil)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "2"
    )

    // The owner is GC'd/evicted; a re-acquiring owner has an empty tracker but
    // inherits the resumable cursor. Tagged input must full-bootstrap, never
    // fold over the lost membership state.
    let reacquiredClient = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(responses: [bootstrapBatch, bootstrapBatch]),
      isExactCursorCutoverEnabled: true
    )
    let discontinuityOutput = try #require(try await pollOnce(reacquiredClient)).apply(in: nil)
    #expect(discontinuityOutput.encounteredTruncate)
    #expect(discontinuityOutput.records.isEmpty)
    discontinuityOutput.transactionDidCommit()
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "-1"
    )

    // The forced bootstrap replaces state atomically and restores continuity.
    let bootstrapOutput = try #require(try await pollOnce(reacquiredClient)).apply(in: nil)
    #expect(bootstrapOutput.encounteredTruncate == false)
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "1"
    )
  }

  @Test
  func suspensionResumeKeepsContinuityOnlyWhileTrackerSurvives() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let survivingClient = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          dnfInsertBatch(
            rowId: "row-1",
            offset: "1",
            tags: ["hash_a/", "/hash_b"],
            activeConditions: [true, true]
          ),
          moveEventBatch(
            offset: "2",
            event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
          ),
        ]
      ),
      isExactCursorCutoverEnabled: true
    )

    _ = try #require(try await pollOnce(survivingClient)).apply(in: nil)

    // A suspension that keeps the process (and tracker) alive resumes the
    // tagged stream without a bootstrap: continuity was never interrupted.
    let resumedOutput = try #require(try await pollOnce(survivingClient)).apply(in: nil)
    #expect(resumedOutput.encounteredTruncate == false)
    #expect(DeleteCapture.keys.snapshot().isEmpty)

    // A suspension that evicts the owner discards the tracker; the replacement
    // owner must full-bootstrap before folding tagged input.
    let evictedOwnerClient = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          moveEventBatch(
            offset: "3",
            event: .moveOut(patterns: [MovePattern(pos: 1, value: "hash_b")])
          )
        ]
      ),
      isExactCursorCutoverEnabled: true
    )
    let evictedOutput = try #require(try await pollOnce(evictedOwnerClient)).apply(in: nil)
    #expect(evictedOutput.encounteredTruncate)
    #expect(DeleteCapture.keys.snapshot().isEmpty)
  }

  @Test
  func mustRefetchResetRebuildsTrackerThroughFullBootstrapWithReplacedHandle() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/hash_b"],
          activeConditions: [true, true]
        ),
        [
          ElectricMessage(
            payload: Data(),
            offset: "-1",
            handle: nil,
            kind: .truncate,
            control: .mustRefetch
          )
        ],
        [
          ElectricMessage(
            payload: Data("{}".utf8),
            key: "row-1",
            offset: "1",
            handle: "replacement",
            kind: .mutation,
            tags: ["hash_c/hash_d"],
            activeConditions: [true, true]
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "replacement",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ],
        moveEventBatch(
          offset: "2",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
        moveEventBatch(
          offset: "3",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_c")])
        ),
      ]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: http,
      isExactCursorCutoverEnabled: true
    )

    _ = try #require(try await pollOnce(client)).apply(in: nil)

    // The server replaces the shape handle via must-refetch: membership state
    // and resume identity from the old handle generation reset together. The
    // tracker reset is deferred to the transaction-commit hook, exactly as the
    // production owner loop runs it.
    let resetOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(resetOutput.encounteredTruncate)
    resetOutput.transactionDidCommit()
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    let clearedState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(clearedState.handle == nil)
    #expect(clearedState.canResumeWithoutFullBootstrap == false)

    // The replacement bootstrap re-establishes continuity under the new handle.
    let replacementOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(replacementOutput.encounteredTruncate == false)
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.handle
        == "replacement"
    )

    // Membership from the retired generation is unreachable; only the
    // replacement generation's tags address the row.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot().isEmpty)
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
  }

  @Test
  func accountSwitchStartsFromScratchWithoutMembershipLeak() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    // Account A holds a tagged row in its own database and tracker.
    let firstAccountMetadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let firstAccountClient = taggedShapeClient(
      metadata: firstAccountMetadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          dnfInsertBatch(
            rowId: "account-a-row",
            offset: "1",
            tags: ["hash_a/hash_b"],
            activeConditions: [true, true]
          ),
          moveEventBatch(
            offset: "2",
            event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
          ),
        ]
      )
    )
    _ = try #require(try await pollOnce(firstAccountClient)).apply(in: nil)

    // An account switch replaces database and client; the fresh account starts
    // with no sync state and no membership. Move-outs addressing the previous
    // account's tags must evict nothing.
    let secondAccountMetadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let secondAccountClient = taggedShapeClient(
      metadata: secondAccountMetadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          moveEventBatch(
            offset: "1",
            event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
          ),
          dnfInsertBatch(
            rowId: "account-b-row",
            offset: "2",
            tags: ["hash_a/hash_b"],
            activeConditions: [true, true]
          ),
        ]
      )
    )
    let staleMoveOutOutput = try #require(try await pollOnce(secondAccountClient)).apply(in: nil)
    #expect(staleMoveOutOutput.encounteredTruncate == false)
    #expect(DeleteCapture.keys.snapshot().isEmpty)

    // The fresh account bootstraps its own membership from scratch.
    _ = try #require(try await pollOnce(secondAccountClient)).apply(in: nil)
    #expect(ProcessCapture.keys.snapshot() == ["account-a-row", "account-b-row"])

    // The switch did not disturb account A's still-live tracker.
    _ = try #require(try await pollOnce(firstAccountClient)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["account-a-row"])
  }

  @Test
  func incompleteResumeIdentityWithEmptyTrackerFullBootstraps() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    // Mismatched persisted identity: a resumable offset whose shape handle is
    // gone. The empty tracker cannot vouch for it either way — tagged input
    // must full-bootstrap rather than trust the partial identity. The epoch
    // matches the client so the tracker-loss fence, not the semantic-epoch
    // fence, is what fires.
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "7",
        handle: nil,
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    let taggedBatch = dnfInsertBatch(
      rowId: "row-1",
      offset: "8",
      tags: ["hash_a/", "/hash_b"],
      activeConditions: [true, true]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(responses: [taggedBatch, taggedBatch]),
      isExactCursorCutoverEnabled: true
    )

    let mismatchOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(mismatchOutput.encounteredTruncate)
    #expect(ProcessCapture.keys.snapshot().isEmpty)
    mismatchOutput.transactionDidCommit()
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "-1"
    )

    let bootstrapOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(bootstrapOutput.encounteredTruncate == false)
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])
  }

  // MARK: - Duplicate / replay convergence (Electric 1.7.6 restart duplicate
  // suppression oracle: replayed transactions must not duplicate rows, tags,
  // tombstones, or cursor movement)

  @Test
  func replayedTaggedBatchAfterServiceRestartConvergesWithoutDuplicateEffects() async throws {
    let store = VersionedMoveOutStore()
    let metadata = InMemoryMetadataProvider()
    let insertBatch = versionedInsertBatch(version: 1, offset: "1", tags: ["pending/x"])
    let replayedMoveOut = [
      ElectricMessage(
        payload: Data(),
        offset: "2",
        handle: "handle",
        kind: .mutation,
        event: .moveOut(patterns: [MovePattern(pos: 0, value: "pending")])
      ),
      ElectricMessage(
        payload: Data(),
        offset: "2",
        handle: "handle",
        isUpToDate: true,
        kind: .snapshot,
        control: .upToDate
      ),
    ]
    let now = Date(timeIntervalSince1970: 40_000)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: ScriptedHTTPClientProvider(
          responses: [insertBatch, insertBatch, replayedMoveOut, replayedMoveOut]
        ),
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        runtimeProvider: fixedRuntimeProvider(now),
        isExactCursorCutoverEnabled: true
      )
    )
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: VersionedMoveOutModel.self,
      basePredicate: nil
    )
    do {
      for _ in 0..<2 {
        let batch = try #require(
          try await client.pollStream(
            VersionedMoveOutModel.self,
            basePredicate: nil,
            syncMode: .onDemand,
            live: false
          )
        )
        _ = try batch.apply(in: store)
      }

      // The replayed insert converges: one row, one owner, no tombstones, and
      // the cursor rests at the replayed offset without regressing.
      #expect(store.row(id: "row-1") == VersionedMoveOutRow(id: "row-1", version: 1))
      #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 1)
      #expect(store.tombstoneCount() == 0)
      #expect(
        try metadata.getSyncState(collectionId: streamStateKey, transaction: store)?.offset == "1"
      )

      for _ in 0..<2 {
        let batch = try #require(
          try await client.pollStream(
            VersionedMoveOutModel.self,
            basePredicate: nil,
            syncMode: .onDemand,
            live: false
          )
        )
        _ = try batch.apply(in: store)
      }

      // The replayed move-out is idempotent: one delete, one tombstone, and
      // stable resume state.
      #expect(store.row(id: "row-1") == nil)
      #expect(store.tombstone(rowId: "row-1")?.version == 1)
      #expect(metadata.ownerCount(table: VersionedMoveOutModel.tableName, rowKey: "row-1") == 0)
      #expect(
        try metadata.getSyncState(collectionId: streamStateKey, transaction: store)?.offset == "2"
      )
    }
  }

  @Test
  func replayedDnfMoveOutEmitsExactlyOneSyntheticDelete() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let moveOut = moveEventBatch(
      offset: "2",
      event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: ScriptedHTTPClientProvider(
        responses: [
          dnfInsertBatch(
            rowId: "row-1",
            offset: "1",
            tags: ["hash_a/hash_b"],
            activeConditions: [true, true]
          ),
          moveOut,
          moveOut,
        ]
      ),
      isExactCursorCutoverEnabled: true
    )

    _ = try #require(try await pollOnce(client)).apply(in: nil)
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])

    // The replayed move-out finds no membership and emits nothing further.
    let replayOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(replayOutput.encounteredTruncate == false)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "2"
    )
  }

  // MARK: - Malformed tagged batches (batch-level: quarantined without partial
  // publication or cursor movement; codec-level corpus is owned by
  // ElectricTaggedProtocolCodecTests)

  @Test
  func malformedTaggedBatchQuarantinesWithoutPartialPublicationOrCursorMovement() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/hash_b"],
          activeConditions: [true, true]
        ),
        // Condition-length mismatch: two tag positions, three conditions.
        dnfInsertBatch(
          rowId: "row-2",
          offset: "2",
          tags: ["hash_c/hash_d"],
          activeConditions: [true, true, true]
        ),
        moveEventBatch(
          offset: "3",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
      ]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: http,
      isExactCursorCutoverEnabled: true
    )

    _ = try #require(try await pollOnce(client)).apply(in: nil)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )

    do {
      _ = try await pollOnce(client)
      Issue.record("Expected the malformed tagged batch to be quarantined during intake")
    } catch ElectricSyncError.protocolQuarantined(let quarantine) {
      #expect(quarantine.reason == .tag)
    } catch {
      Issue.record("Expected a typed tag quarantine, got \(error)")
    }

    // No part of the rejected batch published and the cursor did not move.
    #expect(ProcessCapture.keys.snapshot() == ["row-1"])
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "1"
    )

    // The retained tracker still applies well-formed successor batches.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
  }

  @Test
  func outOfRangeMovePatternInTaggedBatchQuarantines() async throws {
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: Data("{}".utf8),
            key: "row-1",
            offset: "1",
            handle: "handle",
            kind: .mutation,
            tags: ["hash_a/hash_b"],
            activeConditions: [true, true]
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "handle",
            kind: .mutation,
            event: .moveOut(patterns: [MovePattern(pos: 5, value: "hash_a")])
          ),
          ElectricMessage(
            payload: Data(),
            offset: "1",
            handle: "handle",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          ),
        ]
      ]
    )
    let client = taggedShapeClient(metadata: metadata, http: http)

    do {
      _ = try await pollOnce(client)
      Issue.record("Expected the out-of-range move pattern to be quarantined during intake")
    } catch ElectricSyncError.protocolQuarantined(let quarantine) {
      #expect(quarantine.reason == .tag)
    } catch {
      Issue.record("Expected a typed tag quarantine, got \(error)")
    }

    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    #expect(ProcessCapture.keys.snapshot().isEmpty)
    #expect(try metadata.getSyncState(collectionId: streamStateKey, transaction: nil) == nil)
  }

  // MARK: - Resume/control fixtures over a tagged stream

  @Test
  func controlOnlyPageOverTaggedStreamAdvancesCursorWithoutMembershipChange() async throws {
    DeleteCapture.keys.reset()
    ProcessCapture.keys.reset()

    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let http = ScriptedHTTPClientProvider(
      responses: [
        dnfInsertBatch(
          rowId: "row-1",
          offset: "1",
          tags: ["hash_a/hash_b"],
          activeConditions: [true, true]
        ),
        [
          ElectricMessage(
            payload: Data(),
            offset: "2",
            handle: "handle",
            isUpToDate: true,
            kind: .snapshot,
            control: .upToDate
          )
        ],
        moveEventBatch(
          offset: "3",
          event: .moveOut(patterns: [MovePattern(pos: 0, value: "hash_a")])
        ),
      ]
    )
    let client = taggedShapeClient(
      metadata: metadata,
      http: http,
      isExactCursorCutoverEnabled: true
    )

    _ = try #require(try await pollOnce(client)).apply(in: nil)

    // A control-only page advances resume state without publishing rows,
    // deleting rows, or interrupting tracker continuity.
    let controlOutput = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(controlOutput.records.isEmpty)
    #expect(controlOutput.encounteredTruncate == false)
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestMoveOutModel.self,
      basePredicate: nil
    )
    #expect(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)?.offset == "2"
    )

    // Membership survives the empty page: the later move-out still evicts.
    _ = try #require(try await pollOnce(client)).apply(in: nil)
    #expect(DeleteCapture.keys.snapshot() == ["row-1"])
  }
}

private func taggedShapeClient(
  metadata: InMemoryMetadataProvider,
  http: ScriptedHTTPClientProvider,
  isExactCursorCutoverEnabled: Bool = false,
  logger: any LogProvider = NoopLogProvider()
) -> ElectricSyncClientImpl {
  ElectricSyncClientImpl(
    configuration: .init(
      metadataProvider: metadata,
      httpClient: http,
      fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
      logger: logger,
      isExactCursorCutoverEnabled: isExactCursorCutoverEnabled,
      protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
        isTaggedShapeProtocolEnabled: { true }
      )
    )
  )
}

private func pollOnce(
  _ client: ElectricSyncClientImpl,
  basePredicate: SQLExpression? = nil,
  shapeTopology: ElectricShapeTopology = .dnf
) async throws -> SyncBatch<TestMoveOutModel>? {
  try await client.pollStream(
    TestMoveOutModel.self,
    basePredicate: basePredicate,
    shapeTopology: shapeTopology,
    syncMode: .onDemand,
    live: false
  )
}

private func dnfInsertBatch(
  rowId: String,
  offset: String,
  tags: [String],
  activeConditions: [Bool]
) -> [ElectricMessage] {
  [
    ElectricMessage(
      payload: Data("{}".utf8),
      key: rowId,
      offset: offset,
      handle: "handle",
      kind: .mutation,
      tags: tags,
      activeConditions: activeConditions
    ),
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle",
      isUpToDate: true,
      kind: .snapshot,
      control: .upToDate
    ),
  ]
}

private func moveEventBatch(offset: String, event: ElectricEvent) -> [ElectricMessage] {
  [
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle",
      kind: .mutation,
      event: event
    ),
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle",
      isUpToDate: true,
      kind: .snapshot,
      control: .upToDate
    ),
  ]
}

private func simpleInsertBatch(
  rowId: String,
  offset: String,
  tags: [String]
) -> [ElectricMessage] {
  [
    ElectricMessage(
      payload: Data("{}".utf8),
      key: rowId,
      offset: offset,
      handle: "handle",
      kind: .mutation,
      tags: tags
    ),
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle",
      isUpToDate: true,
      kind: .snapshot,
      control: .upToDate
    ),
  ]
}

private final class CapabilityFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var storage: Bool

  init(_ value: Bool) {
    self.storage = value
  }

  var value: Bool {
    get { lock.withLock { storage } }
    set { lock.withLock { storage = newValue } }
  }
}

private actor HookedScriptedHTTPClientProvider: HTTPClientProvider {
  private let responses: [[ElectricMessage]]
  private let hooks: [Int: @Sendable () -> Void]
  private var served = 0
  private var requestCountWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] =
    []

  init(responses: [[ElectricMessage]], hooks: [Int: @Sendable () -> Void]) {
    self.responses = responses
    self.hooks = hooks
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    _ = request
    let index = served
    served += 1
    var remainingWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
    for waiter in requestCountWaiters {
      if served >= waiter.count {
        waiter.continuation.resume()
      } else {
        remainingWaiters.append(waiter)
      }
    }
    requestCountWaiters = remainingWaiters
    if let hook = hooks[index] { hook() }
    guard index < responses.count else { return [] }
    return responses[index]
  }

  func waitForRequestCount(_ count: Int) async {
    if served >= count { return }
    await withCheckedContinuation { continuation in
      requestCountWaiters.append((count: count, continuation: continuation))
    }
  }
}

private final class LockedStringSet: @unchecked Sendable {
  private let lock = NSLock()
  private var values: Set<String> = []

  func insert(_ value: String) {
    lock.withLock { _ = values.insert(value) }
  }

  func remove(_ value: String) {
    lock.withLock { _ = values.remove(value) }
  }

  func removeAll() {
    lock.withLock { values.removeAll(keepingCapacity: false) }
  }

  func reset() {
    removeAll()
  }

  func snapshot() -> [String] {
    lock.withLock { values.sorted() }
  }
}

private enum SwapCaptures {
  static let rows = LockedStringSet()
  static let truncateCalls = LockedArray<String>()
}

private struct TruncateSwapModel: ElectricCollectionModel {
  static var tableName: String { "swap_table" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(endpoint: "/shapes/truncate-swap", selectedColumns: ["id"])
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

  static func processMessage(_ message: ElectricMessage, transaction _: Any?) throws
    -> ProcessedMessage<Self>
  {
    if let key = message.key {
      SwapCaptures.rows.insert(key)
    }
    return ProcessedMessage(
      records: [Self()],
      metadata: StoreMetadata(
        offset: message.offset,
        handle: message.handle,
        cursor: message.cursor,
        operation: .insert
      )
    )
  }

  static func deleteByKey(_ key: String, transaction _: Any?) throws {
    SwapCaptures.rows.remove(key)
  }

  static func truncate(transaction _: Any?) throws {
    SwapCaptures.truncateCalls.append("truncate")
    SwapCaptures.rows.removeAll()
  }
}

private actor TruncateHookCounter: ElectricSyncEventHandler {
  private var willCount = 0
  private var didCount = 0
  private var didWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

  func willReceiveTruncate(table _: String, predicate _: SQLExpression?) async {
    willCount += 1
  }

  func didReceiveTruncate(table _: String, predicate _: SQLExpression?) async {
    didCount += 1
    var remaining: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
    for waiter in didWaiters {
      if didCount >= waiter.count {
        waiter.continuation.resume()
      } else {
        remaining.append(waiter)
      }
    }
    didWaiters = remaining
  }

  func willTruncateCount() -> Int { willCount }

  func waitForDidTruncateCount(_ count: Int) async {
    if didCount >= count { return }
    await withCheckedContinuation { continuation in
      didWaiters.append((count: count, continuation: continuation))
    }
  }
}

private struct EmptyCacheProvider: DataCacheProvider {
  func load<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> [T]
  where T: ElectricCollectionModel {
    []
  }

  func hasData<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> Bool
  where T: ElectricCollectionModel {
    false
  }
}

private struct StaticDelayBreaker: CircuitBreakerStrategy {
  let delay: TimeInterval

  mutating func preflightDelay(now _: Date) -> TimeInterval? { nil }
  mutating func recordSuccess() {}
  mutating func recordFailure(now _: Date, reason _: String?) -> TimeInterval { delay }
  mutating func reset() {}
}

private func versionedInsertBatch(
  rowId: String = "row-1",
  version: Int64,
  offset: String,
  tags: [String]? = nil,
  isSubset: Bool = false
) -> [ElectricMessage] {
  let row = VersionedMoveOutRow(id: rowId, version: version)
  let data = try! JSONEncoder().encode(row)
  var messages = [
    ElectricMessage(
      payload: data,
      key: row.id,
      offset: offset,
      handle: "handle",
      kind: isSubset ? .snapshot : .mutation,
      tags: tags,
      isSubsetSnapshot: isSubset
    )
  ]
  if isSubset {
    messages.append(
      ElectricMessage(
        payload: Data(),
        offset: offset,
        handle: "handle",
        kind: .snapshot,
        control: .snapshotEnd,
        isSubsetSnapshot: true
      )
    )
    messages.append(
      ElectricMessage(
        payload: Data(),
        offset: offset,
        handle: "handle",
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
        handle: "handle",
        isUpToDate: true,
        kind: .snapshot,
        control: .upToDate
      )
    )
  }
  return messages
}

private func moveOutBatch(offset: String, tag: String = "pending") -> [ElectricMessage] {
  [
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle",
      kind: .mutation,
      event: .moveOut(patterns: [MoveOutPattern(pos: 0, value: tag)])
    ),
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle",
      isUpToDate: true,
      kind: .snapshot,
      control: .upToDate
    ),
  ]
}
