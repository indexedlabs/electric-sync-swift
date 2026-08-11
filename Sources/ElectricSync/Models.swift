import Foundation

/// Lightweight SQL-like expression representation that keeps the library storage agnostic.
public struct SQLExpression: Hashable, Sendable {
  public let rawValue: String
  public let predicate: SyncPredicateExpression?

  public init(_ rawValue: String, predicate: SyncPredicateExpression? = nil) {
    self.rawValue = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
    self.predicate = predicate
  }

  public init(predicate: SyncPredicateExpression) {
    self.rawValue = predicate.canonicalDescription()
    self.predicate = predicate
  }

  public func normalized() -> String {
    if let json = encodedPredicateJSON() {
      return "pred:\(json)"
    }
    return rawValue.isEmpty ? "" : "sql:\(rawValue)"
  }

  public func encodedPredicateJSON() -> String? {
    guard
      let data = predicate?.toJSONData(),
      let json = String(data: data, encoding: .utf8)
    else {
      return nil
    }
    return json
  }
}

/// Ordering clause definition for cache + fetch planning.
public struct OrderBy: Hashable, Sendable {
  public enum Direction: String, Sendable {
    case ascending
    case descending
  }

  public let field: String
  public let direction: Direction

  public init(field: String, direction: Direction = .ascending) {
    self.field = field
    self.direction = direction
  }
}

/// Stable hash for predicates so metadata providers can deduplicate fetches.
public struct PredicateHash: Hashable, Sendable {
  public static let all = PredicateHash(value: "all")

  public let value: String

  public init(value: String) {
    self.value = value
  }

  public init(from predicate: SQLExpression?) {
    guard let predicate else {
      self = .all
      return
    }
    let normalized = predicate.normalized()
    self.value = normalized.isEmpty ? "all" : normalized
  }
}

public struct FetchedPredicate: Hashable, Sendable {
  public let predicateHash: PredicateHash
  public let predicateJSON: String?
  public let snapshotBoundary: PostgresSnapshot?
  public let outcome: SubsetObservationOutcome?
  public let isComplete: Bool
  public let fetchedAt: Date

  public init(
    predicateHash: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome?,
    isComplete: Bool,
    fetchedAt: Date
  ) {
    self.predicateHash = predicateHash
    self.predicateJSON = predicateJSON
    self.snapshotBoundary = snapshotBoundary
    self.outcome = outcome
    self.isComplete = isComplete
    self.fetchedAt = fetchedAt
  }
}

extension FetchedPredicate {
  public var expression: SyncPredicateExpression? {
    guard let predicateJSON else { return nil }
    return SyncPredicateExpression.fromJSON(predicateJSON)
  }
}

public struct FetchedRange: Hashable, Sendable {
  public let minValue: String?
  public let maxValue: String?
  public let count: Int
  public let fetchedAt: Date

  public init(minValue: String?, maxValue: String?, count: Int, fetchedAt: Date) {
    self.minValue = minValue
    self.maxValue = maxValue
    self.count = count
    self.fetchedAt = fetchedAt
  }
}

public enum ElectricProtocolSemanticEpoch: String, Codable, Equatable, Sendable {
  case legacy
  case taggedShape1_7_7 = "tagged_shape_1_7_7"
  case unknown

  public var isTaggedShapeCapabilityEnabled: Bool {
    self == .taggedShape1_7_7
  }
}

public struct SyncState: Sendable {
  public let offset: String?
  public let handle: String?
  public let cursor: String?
  public let isUpToDate: Bool
  public let lastSyncedAt: Date?
  public let protocolSemanticEpoch: ElectricProtocolSemanticEpoch

  public init(
    offset: String?,
    handle: String?,
    cursor: String?,
    isUpToDate: Bool,
    lastSyncedAt: Date?,
    protocolSemanticEpoch: ElectricProtocolSemanticEpoch = .legacy
  ) {
    self.offset = offset
    self.handle = handle
    self.cursor = cursor
    self.isUpToDate = isUpToDate
    self.lastSyncedAt = lastSyncedAt
    self.protocolSemanticEpoch = protocolSemanticEpoch
  }
}

extension SyncState {
  public static var fullBootstrap: SyncState {
    fullBootstrap(protocolSemanticEpoch: .legacy)
  }

  public static func fullBootstrap(
    protocolSemanticEpoch: ElectricProtocolSemanticEpoch
  ) -> SyncState {
    SyncState(
      offset: "-1",
      handle: nil,
      cursor: nil,
      isUpToDate: false,
      lastSyncedAt: nil,
      protocolSemanticEpoch: protocolSemanticEpoch
    )
  }

  public func hasSameResumeIdentity(as other: SyncState) -> Bool {
    offset == other.offset
      && handle == other.handle
      && cursor == other.cursor
      && isUpToDate == other.isUpToDate
      && protocolSemanticEpoch == other.protocolSemanticEpoch
  }

  public var canResumeWithoutFullBootstrap: Bool {
    guard let offset else { return false }
    return offset != "-1"
  }
}

public struct PostgresSnapshot: Sendable, Hashable {
  public let xmin: String
  public let xmax: String
  public let xipList: [String]

  public init(xmin: String, xmax: String, xipList: [String]) {
    self.xmin = xmin
    self.xmax = xmax
    self.xipList = xipList
  }

  public func isVisible(transactionId: Int64) -> Bool {
    guard let xmin = Int64(xmin), let xmax = Int64(xmax) else { return false }
    if transactionId < xmin { return true }
    if transactionId >= xmax { return false }
    return !xipList.contains(String(transactionId))
  }
}

public enum SubsetObservationOutcome: String, Codable, Hashable, Sendable {
  case present
  case absent
}

public struct SubsetObservation: Hashable, Sendable {
  public let predicateHash: PredicateHash
  public let predicateJSON: String?
  public let snapshotBoundary: PostgresSnapshot?
  public let outcome: SubsetObservationOutcome
  public let observedAt: Date

  public init(
    predicateHash: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome,
    observedAt: Date
  ) {
    self.predicateHash = predicateHash
    self.predicateJSON = predicateJSON
    self.snapshotBoundary = snapshotBoundary
    self.outcome = outcome
    self.observedAt = observedAt
  }
}

public struct MovePattern: Sendable, Hashable {
  public let pos: Int
  public let value: String

  public init(pos: Int, value: String) {
    self.pos = pos
    self.value = value
  }
}

@available(*, deprecated, renamed: "MovePattern")
public typealias MoveOutPattern = MovePattern

public enum ElectricEvent: Sendable, Hashable {
  case moveOut(patterns: [MovePattern])
  case moveIn(patterns: [MovePattern])
}

public struct ElectricVersionedRowIdentifier: Sendable, Equatable {
  public let rowId: String
  public let version: Int64

  public init(rowId: String, version: Int64) {
    self.rowId = rowId
    self.version = version
  }
}

public struct ElectricDeleteTombstoneContext: Sendable, Equatable {
  public let streamStateKey: String
  public let deletedAt: Date
  public let shouldUseMoveOutTombstones: Bool

  public init(
    streamStateKey: String,
    deletedAt: Date,
    shouldUseMoveOutTombstones: Bool = true
  ) {
    self.streamStateKey = streamStateKey
    self.deletedAt = deletedAt
    self.shouldUseMoveOutTombstones = shouldUseMoveOutTombstones
  }
}

public struct ElectricMoveOutTombstone: Sendable, Equatable {
  public let tableName: String
  public let rowId: String
  public let streamStateKey: String
  public let version: Int64
  public let offset: String?
  public let cursor: String?
  public let deletedAt: Date

  public init(
    tableName: String,
    rowId: String,
    streamStateKey: String,
    version: Int64,
    offset: String?,
    cursor: String?,
    deletedAt: Date
  ) {
    self.tableName = tableName
    self.rowId = rowId
    self.streamStateKey = streamStateKey
    self.version = version
    self.offset = offset
    self.cursor = cursor
    self.deletedAt = deletedAt
  }
}

public struct StoreMetadata: Sendable {
  public enum StoreOperation: Hashable, Sendable {
    case insert
    case update
    case delete
    case truncate
  }

  public let offset: String?
  public let handle: String?
  public let cursor: String?
  public let operation: StoreOperation

  public init(
    offset: String?,
    handle: String?,
    cursor: String? = nil,
    operation: StoreOperation
  ) {
    self.offset = offset
    self.handle = handle
    self.cursor = cursor
    self.operation = operation
  }
}

/// Canonical row/effect address that a model adapter proves it materialized in the owner
/// transaction. Correlation identifiers are attached by `SyncBatch` from the same message.
public struct OptimisticPublishedRowEffect: Hashable, Sendable {
  public let rowId: String
  public let operation: StoreMetadata.StoreOperation

  public init(rowId: String, operation: StoreMetadata.StoreOperation) {
    self.rowId = rowId
    self.operation = operation
  }
}

/// Minimal cross-package identity used to correlate a materialized Electric row with an
/// optimistic mutation. Database-owned overlay models inherit this protocol so generic shape
/// processing emits publication evidence without model-specific wiring.
public protocol OptimisticPublicationRowIdentifiable {
  var optimisticPublicationRowId: String { get }
}

public struct OptimisticPublicationEvidence: Hashable, Sendable {
  public let rowEffect: OptimisticPublishedRowEffect
  public let transactionIds: Set<Int64>
  public let loroFrontiers: Set<Data>

  public init(
    rowEffect: OptimisticPublishedRowEffect,
    transactionIds: Set<Int64>,
    loroFrontiers: Set<Data>
  ) {
    self.rowEffect = rowEffect
    self.transactionIds = transactionIds
    self.loroFrontiers = loroFrontiers
  }
}

public enum ElectricLogMode: String, Sendable {
  case full = "full"
  case changesOnly = "changes_only"
}

/// Controls whether Electric sends changed columns or complete rows for updates.
///
/// Production collections currently declare ``default``; ``full`` is threaded
/// through the request and transport layers so durable typed materializations
/// can adopt complete-row updates once activation is unblocked.
public enum ElectricReplicaMode: String, Sendable, Equatable {
  case `default` = "default"
  case full = "full"
}

public struct ElectricSubsetRequest: Sendable, Equatable {
  public let whereClause: String
  public let paramsJSON: String?
  public let orderByClause: String?
  public let limit: Int?
  public let offset: Int?

  public init(
    whereClause: String,
    paramsJSON: String?,
    orderByClause: String?,
    limit: Int?,
    offset: Int?
  ) {
    self.whereClause = whereClause
    self.paramsJSON = paramsJSON
    self.orderByClause = orderByClause
    self.limit = limit
    self.offset = offset
  }
}

/// The immutable wire contract that determines whether an Electric cursor can
/// safely resume a shape request.
public struct ElectricShapeWireIdentity: Hashable, Sendable {
  public let endpoint: String
  public let selectedColumns: [String]
  public let options: [String: String]
  /// Explicit compatibility marker for the pre-wire-identity cursor format.
  /// Leave nil unless this exact endpoint/column/options contract shipped in that release.
  /// This rollout metadata is intentionally excluded from equality and hashing.
  public let legacyCursorVersion: String?

  public init(
    endpoint: String,
    selectedColumns: [String],
    options: [String: String] = [:],
    legacyCursorVersion: String? = nil
  ) {
    self.endpoint = Self.normalizedEndpoint(endpoint)
    self.selectedColumns = selectedColumns.sorted()
    self.options = options
    self.legacyCursorVersion = legacyCursorVersion
  }

  public static func == (lhs: Self, rhs: Self) -> Bool {
    lhs.endpoint == rhs.endpoint
      && lhs.selectedColumns == rhs.selectedColumns
      && lhs.options == rhs.options
  }

  public func hash(into hasher: inout Hasher) {
    hasher.combine(endpoint)
    hasher.combine(selectedColumns)
    hasher.combine(options)
  }

  static func unspecified(table: String) -> Self {
    Self(
      endpoint: "unspecified://\(table)",
      selectedColumns: ["unspecified"]
    )
  }

  private static func normalizedEndpoint(_ endpoint: String) -> String {
    let trimmed = endpoint.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return "/" }
    let path = trimmed.hasPrefix("/") ? trimmed : "/\(trimmed)"
    return path.replacingOccurrences(of: #"/{2,}"#, with: "/", options: .regularExpression)
  }
}

public struct ElectricShapeRequest: Sendable {
  public let wireIdentity: ElectricShapeWireIdentity
  public let table: String
  public let predicate: SQLExpression?
  public let orderBy: [OrderBy]
  public let limit: Int?
  public let offset: String?
  public let handle: String?
  public let cursor: String?
  public let live: Bool
  public let log: ElectricLogMode?
  public let replica: ElectricReplicaMode
  public let subset: ElectricSubsetRequest?

  public init(
    wireIdentity: ElectricShapeWireIdentity? = nil,
    table: String,
    predicate: SQLExpression?,
    orderBy: [OrderBy] = [],
    limit: Int? = nil,
    offset: String? = nil,
    handle: String? = nil,
    cursor: String? = nil,
    live: Bool = false,
    log: ElectricLogMode? = nil,
    replica: ElectricReplicaMode = .default,
    subset: ElectricSubsetRequest? = nil
  ) {
    self.wireIdentity = wireIdentity ?? .unspecified(table: table)
    self.table = table
    self.predicate = predicate
    self.orderBy = orderBy
    self.limit = limit
    self.offset = offset
    self.handle = handle
    self.cursor = cursor
    self.live = live
    self.log = log
    self.replica = replica
    self.subset = subset
  }

  public func updating(
    offset: String?,
    handle: String?,
    cursor: String?,
    live: Bool? = nil
  ) -> ElectricShapeRequest {
    ElectricShapeRequest(
      wireIdentity: wireIdentity,
      table: table,
      predicate: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live ?? self.live,
      log: log,
      replica: replica,
      subset: subset
    )
  }

  public func with(log: ElectricLogMode?) -> ElectricShapeRequest {
    ElectricShapeRequest(
      wireIdentity: wireIdentity,
      table: table,
      predicate: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live,
      log: log,
      replica: replica,
      subset: subset
    )
  }

  public func with(replica: ElectricReplicaMode) -> ElectricShapeRequest {
    ElectricShapeRequest(
      wireIdentity: wireIdentity,
      table: table,
      predicate: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live,
      log: log,
      replica: replica,
      subset: subset
    )
  }

  public func with(subset: ElectricSubsetRequest?) -> ElectricShapeRequest {
    ElectricShapeRequest(
      wireIdentity: wireIdentity,
      table: table,
      predicate: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live,
      log: log,
      replica: replica,
      subset: subset
    )
  }

  public func with(wireIdentity: ElectricShapeWireIdentity) -> ElectricShapeRequest {
    ElectricShapeRequest(
      wireIdentity: wireIdentity,
      table: table,
      predicate: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live,
      log: log,
      replica: replica,
      subset: subset
    )
  }
}

public struct ElectricMessage: Sendable {
  public enum Control: Sendable {
    case upToDate
    case snapshotEnd
    case mustRefetch
    case subsetEnd
  }

  public enum Kind: Sendable {
    case mutation
    case snapshot
    case truncate
  }

  public let payload: Data
  public let key: String?
  public let offset: String?
  public let handle: String?
  public let cursor: String?
  public let isUpToDate: Bool
  public let kind: Kind
  public let control: Control?
  public let postgresSnapshot: PostgresSnapshot?
  public let txids: [Int64]?
  public let tags: [String]?
  public let removedTags: [String]?
  public let activeConditions: [Bool]?
  public let event: ElectricEvent?
  public let fieldPresence: ElectricPayloadFieldPresence?
  public let isSubsetSnapshot: Bool

  public init(
    payload: Data,
    key: String? = nil,
    offset: String? = nil,
    handle: String? = nil,
    cursor: String? = nil,
    isUpToDate: Bool = false,
    kind: Kind = .mutation,
    control: Control? = nil,
    postgresSnapshot: PostgresSnapshot? = nil,
    txids: [Int64]? = nil,
    tags: [String]? = nil,
    removedTags: [String]? = nil,
    activeConditions: [Bool]? = nil,
    event: ElectricEvent? = nil,
    fieldPresence: ElectricPayloadFieldPresence? = nil,
    isSubsetSnapshot: Bool = false
  ) {
    self.payload = payload
    self.key = key
    self.offset = offset
    self.handle = handle
    self.cursor = cursor
    self.isUpToDate = isUpToDate
    self.kind = kind
    self.control = control
    self.postgresSnapshot = postgresSnapshot
    self.txids = txids
    self.tags = tags
    self.removedTags = removedTags
    self.activeConditions = activeConditions
    self.event = event
    self.fieldPresence = fieldPresence
    self.isSubsetSnapshot = isSubsetSnapshot
  }
}

public struct ProcessedMessage<T: Sendable>: Sendable {
  public let records: [T]
  public let metadata: StoreMetadata
  public let missingRowKeys: [String]
  /// Canonical row/effect addresses durably materialized by this adapter invocation.
  public let optimisticPublishedRowEffects: [OptimisticPublishedRowEffect]
  /// Exact Loro causal frontiers durably published by this processed message.
  /// Model adapters opt in; absence never implies confirmation.
  public let confirmedLoroFrontiers: [Data]

  public init(
    records: [T],
    metadata: StoreMetadata,
    missingRowKeys: [String] = [],
    optimisticPublishedRowEffects: [OptimisticPublishedRowEffect] = [],
    confirmedLoroFrontiers: [Data] = []
  ) {
    self.records = records
    self.metadata = metadata
    self.missingRowKeys = missingRowKeys
    self.optimisticPublishedRowEffects = optimisticPublishedRowEffects
    self.confirmedLoroFrontiers = confirmedLoroFrontiers
  }
}

/// TanStack-style cursor expressions used for stable pagination with orderBy ties.
/// The sync layer issues two subset snapshots:
/// - `whereCurrent` (all ties at cursor, no limit)
/// - `whereFrom` (strictly after cursor, with limit)
public struct ElectricCursorExpressions: Hashable, Sendable {
  public let whereFrom: SyncPredicateExpression
  public let whereCurrent: SyncPredicateExpression

  public init(whereFrom: SyncPredicateExpression, whereCurrent: SyncPredicateExpression) {
    self.whereFrom = whereFrom
    self.whereCurrent = whereCurrent
  }
}

public struct QueryDescriptor: Hashable, Sendable {
  public let predicate: SQLExpression?
  public let orderBy: [OrderBy]
  public let limit: Int?
  public let cursor: ElectricCursorExpressions?

  public init(
    predicate: SQLExpression?,
    orderBy: [OrderBy] = [],
    limit: Int? = nil,
    cursor: ElectricCursorExpressions? = nil
  ) {
    self.predicate = predicate
    self.orderBy = orderBy
    self.limit = limit
    self.cursor = cursor
  }
}

public struct FetchPlan: Sendable {
  public let needsFetch: Bool
  public let predicate: SQLExpression?
  public let ranges: [FetchedRange]?
  public let reuseExisting: Bool

  public init(
    needsFetch: Bool,
    predicate: SQLExpression?,
    ranges: [FetchedRange]?,
    reuseExisting: Bool
  ) {
    self.needsFetch = needsFetch
    self.predicate = predicate
    self.ranges = ranges
    self.reuseExisting = reuseExisting
  }
}
