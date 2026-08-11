import Foundation

/// Tracks fetch + sync metadata outside of the cache.
/// Calls are synchronous; implementations may optionally honor an opaque transaction context.
public protocol MetadataProvider: Sendable {
  var supportsDurableRowOwnership: Bool { get }

  func hasFetched(table: String, predicate: PredicateHash, transaction: Any?) throws -> Bool
  func getFetchedPredicates(table: String, transaction: Any?) throws -> [FetchedPredicate]
  func recordFetch(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome,
    isComplete: Bool,
    transaction: Any?
  ) throws
  func recordFetch(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    basePredicateHash: PredicateHash,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome,
    isComplete: Bool,
    transaction: Any?
  ) throws
  func getLatestObservation(
    table: String,
    predicate: PredicateHash,
    transaction: Any?
  ) throws -> SubsetObservation?
  func recordObservation(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome,
    transaction: Any?
  ) throws

  func getFetchedRanges(table: String, orderField: String, transaction: Any?) throws
    -> [FetchedRange]
  func recordRange(
    table: String,
    orderField: String,
    range: FetchedRange,
    transaction: Any?
  ) throws
  func clearMetadata(table: String, transaction: Any?) throws
  func clearMetadata(
    table: String,
    basePredicateHash: PredicateHash,
    transaction: Any?
  ) throws

  func getSyncState(collectionId: String, transaction: Any?) throws -> SyncState?
  func updateSyncState(collectionId: String, state: SyncState, transaction: Any?) throws
  func resetSyncState(collectionId: String, transaction: Any?) throws
  /// Atomically adopts a proven-compatible legacy resume identity into `collectionId`.
  /// Conflicting or bootstrap-only legacy rows are left untouched and return nil.
  func adoptSyncState(
    collectionId: String,
    legacyCollectionIds: [String],
    transaction: Any?
  ) throws -> SyncState?

  func claimRowOwnership(
    table: String,
    rowKey: String,
    shapeIdentity: String,
    transaction: Any?
  ) throws
  func releaseRowOwnership(
    table: String,
    rowKey: String,
    shapeIdentity: String,
    deferredDeleteTombstone: ElectricMoveOutTombstone?,
    transaction: Any?
  ) throws -> Bool
  func releaseAllRowOwnership(
    table: String,
    shapeIdentity: String,
    transaction: Any?
  ) throws -> [String]
  func removeAllRowOwnership(table: String, rowKey: String, transaction: Any?) throws
  func getRowOwnershipTags(
    table: String,
    shapeIdentity: String,
    rowKeys: Set<String>?,
    transaction: Any?
  ) throws -> [String: [String]]
  func trackerRebuildOwnership(
    table: String,
    shapeIdentity: String,
    transaction: Any?
  ) throws -> [String: [String]]?
  func trackerRebuildOwnership(
    table: String,
    shapeIdentity: String,
    localTableOwnership: ElectricLocalTableOwnership,
    transaction: Any?
  ) throws -> [String: [String]]?
  /// Admits the one safe fresh-owner shortcut: no durable Electric ownership,
  /// coverage, or ownership-coordination evidence from any generation, plus
  /// no unowned rows for an exclusive local table.
  func admitsFreshOnDemandPristineOwner(
    table: String,
    localTableOwnership: ElectricLocalTableOwnership,
    transaction: Any?
  ) throws -> Bool
  func updateRowOwnership(
    table: String,
    shapeIdentity: String,
    tagsByRowKey: [String: [String]],
    removedRowKeys: Set<String>,
    transaction: Any?
  ) throws
  func prepareRowOwnershipCutover(table: String, transaction: Any?) throws -> Bool
  func completeRowOwnershipCutover(
    table: String,
    predicateHash: PredicateHash,
    transaction: Any?
  ) throws -> [String]
  /// Retires app-owned optimism only for exact identities explicitly published by this owner
  /// transaction. Implementations must not infer confirmation from row presence or equality.
  func retireOptimisticMutations(
    table: String,
    publications: Set<OptimisticPublicationEvidence>,
    snapshotBoundary: PostgresSnapshot?,
    transaction: Any?
  ) throws
}

extension MetadataProvider {
  public var supportsDurableRowOwnership: Bool { false }

  public func recordFetch(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    basePredicateHash _: PredicateHash,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome,
    isComplete: Bool,
    transaction: Any?
  ) throws {
    try recordFetch(
      table: table,
      predicate: predicate,
      predicateJSON: predicateJSON,
      snapshotBoundary: snapshotBoundary,
      outcome: outcome,
      isComplete: isComplete,
      transaction: transaction
    )
  }

  public func clearMetadata(
    table: String,
    basePredicateHash _: PredicateHash,
    transaction: Any?
  ) throws {
    try clearMetadata(table: table, transaction: transaction)
  }

  public func getLatestObservation(
    table _: String,
    predicate _: PredicateHash,
    transaction _: Any?
  ) throws -> SubsetObservation? {
    nil
  }

  public func recordObservation(
    table _: String,
    predicate _: PredicateHash,
    predicateJSON _: String?,
    snapshotBoundary _: PostgresSnapshot?,
    outcome _: SubsetObservationOutcome,
    transaction _: Any?
  ) throws {}

  public func resetSyncState(collectionId: String, transaction: Any?) throws {
    // Default no-op; concrete providers can override.
  }

  public func adoptSyncState(
    collectionId: String,
    legacyCollectionIds: [String],
    transaction: Any?
  ) throws -> SyncState? {
    if let current = try getSyncState(collectionId: collectionId, transaction: transaction) {
      return current
    }

    let legacyStates = try legacyCollectionIds.compactMap {
      try getSyncState(collectionId: $0, transaction: transaction)
    }
    guard let first = legacyStates.first, first.canResumeWithoutFullBootstrap else { return nil }
    guard legacyStates.dropFirst().allSatisfy({ $0.hasSameResumeIdentity(as: first) }) else {
      return nil
    }
    try updateSyncState(collectionId: collectionId, state: first, transaction: transaction)
    return first
  }

  public func claimRowOwnership(
    table _: String,
    rowKey _: String,
    shapeIdentity _: String,
    transaction _: Any?
  ) throws {}

  public func releaseRowOwnership(
    table _: String,
    rowKey _: String,
    shapeIdentity _: String,
    deferredDeleteTombstone _: ElectricMoveOutTombstone?,
    transaction _: Any?
  ) throws -> Bool {
    // Test/preview providers historically model a single shape. The live GRDB
    // provider overrides this with durable, default-deny ownership.
    true
  }

  public func releaseAllRowOwnership(
    table _: String,
    shapeIdentity _: String,
    transaction _: Any?
  ) throws -> [String] {
    []
  }

  public func removeAllRowOwnership(
    table _: String,
    rowKey _: String,
    transaction _: Any?
  ) throws {}

  public func getRowOwnershipTags(
    table _: String,
    shapeIdentity _: String,
    rowKeys _: Set<String>?,
    transaction _: Any?
  ) throws -> [String: [String]] {
    [:]
  }

  public func trackerRebuildOwnership(
    table _: String,
    shapeIdentity _: String,
    transaction _: Any?
  ) throws -> [String: [String]]? {
    nil
  }

  /// Returns durable membership only when it is safe to rebuild a cold
  /// tracker for the table's declared local-writer topology. Existing
  /// providers inherit exclusive validation until they implement shared-table
  /// admission explicitly.
  public func trackerRebuildOwnership(
    table: String,
    shapeIdentity: String,
    localTableOwnership _: ElectricLocalTableOwnership,
    transaction: Any?
  ) throws -> [String: [String]]? {
    try trackerRebuildOwnership(
      table: table,
      shapeIdentity: shapeIdentity,
      transaction: transaction
    )
  }

  /// Conservative by default: only storage that can inspect every durable
  /// ownership identity and the local table may admit a fresh shortcut.
  public func admitsFreshOnDemandPristineOwner(
    table _: String,
    localTableOwnership _: ElectricLocalTableOwnership,
    transaction _: Any?
  ) throws -> Bool {
    false
  }

  public func updateRowOwnership(
    table _: String,
    shapeIdentity _: String,
    tagsByRowKey _: [String: [String]],
    removedRowKeys _: Set<String>,
    transaction _: Any?
  ) throws {}

  public func prepareRowOwnershipCutover(table _: String, transaction _: Any?) throws -> Bool {
    false
  }

  public func completeRowOwnershipCutover(
    table _: String,
    predicateHash _: PredicateHash,
    transaction _: Any?
  ) throws -> [String] {
    []
  }

  public func retireOptimisticMutations(
    table _: String,
    publications _: Set<OptimisticPublicationEvidence>,
    snapshotBoundary _: PostgresSnapshot?,
    transaction _: Any?
  ) throws {}
}

/// Cache provider that backs Electric queries (GRDB, SQLite, CoreData, etc).
public protocol DataCacheProvider: Sendable {
  func load<T: ElectricCollectionModel>(_ type: T.Type, request: QueryDescriptor) async throws
    -> [T]
  func hasData<T: ElectricCollectionModel>(_ type: T.Type, request: QueryDescriptor) async throws
    -> Bool
  func clear<T: ElectricCollectionModel>(_ type: T.Type) async throws

}

extension DataCacheProvider {
  public func clear<T: ElectricCollectionModel>(_: T.Type) async throws {}

}

/// HTTP client abstraction for Electric shape requests.
public protocol HTTPClientProvider: Sendable {
  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage]
}

/// Streaming HTTP client abstraction for Electric shape live requests (e.g., SSE).
/// Implementations should yield `ElectricMessage`s in the order received.
public protocol HTTPStreamClientProvider: Sendable {
  func stream(_ request: ElectricShapeRequest) async throws -> AsyncThrowingStream<
    ElectricMessage, Error
  >
}

public protocol CacheStrategy: Sendable {
  func willCheckCache<T: ElectricCollectionModel>(type: T.Type, predicate: SQLExpression?)
  func onCacheMiss<T: ElectricCollectionModel>(type: T.Type, predicate: SQLExpression?)
  func didFetch<T: ElectricCollectionModel>(type: T.Type, count: Int, duration: TimeInterval)
}

public struct NoopCacheStrategy: CacheStrategy {
  public init() {}

  public func willCheckCache<T>(type _: T.Type, predicate _: SQLExpression?)
  where T: ElectricCollectionModel {}
  public func onCacheMiss<T>(type _: T.Type, predicate _: SQLExpression?)
  where T: ElectricCollectionModel {}
  public func didFetch<T>(type _: T.Type, count _: Int, duration _: TimeInterval)
  where T: ElectricCollectionModel {}
}

public struct NoopHTTPClientProvider: HTTPClientProvider {
  public init() {}

  public func fetch(_: ElectricShapeRequest) async throws -> [ElectricMessage] {
    []
  }
}

public struct NoopHTTPStreamClientProvider: HTTPStreamClientProvider {
  public init() {}

  public func stream(_: ElectricShapeRequest) async throws -> AsyncThrowingStream<
    ElectricMessage, Error
  > {
    AsyncThrowingStream { continuation in
      continuation.finish()
    }
  }
}

public protocol ElectricSyncEventHandler: Sendable {
  func willReceiveTruncate(table: String, predicate: SQLExpression?) async
  func didReceiveTruncate(table: String, predicate: SQLExpression?) async
  func didReceiveUpToDate(table: String, predicate: SQLExpression?) async
}

extension ElectricSyncEventHandler {
  public func didReceiveUpToDate(table _: String, predicate _: SQLExpression?) async {}
}

public struct NoopElectricSyncEventHandler: ElectricSyncEventHandler {
  public init() {}
  public func willReceiveTruncate(table _: String, predicate _: SQLExpression?) async {}
  public func didReceiveTruncate(table _: String, predicate _: SQLExpression?) async {}
}

// MARK: - Background Task Protection

/// Abstraction for protecting critical work from app suspension.
/// On iOS, implementations typically use `UIApplication.beginBackgroundTask`.
public protocol BackgroundTaskProvider: Sendable {
  /// Executes the given work with protection from app suspension.
  /// The provider should request background execution time before running the work
  /// and release it after completion.
  func performProtectedWork<T: Sendable>(
    named name: String,
    work: @Sendable () async throws -> T
  ) async throws -> T
}

/// Default no-op implementation that runs work without protection.
public struct NoopBackgroundTaskProvider: BackgroundTaskProvider {
  public init() {}

  public func performProtectedWork<T: Sendable>(
    named name: String,
    work: @Sendable () async throws -> T
  ) async throws -> T {
    try await work()
  }
}

// MARK: - Logging

public enum LogLevel: String, Sendable {
  case debug
  case info
  case warning
  case error
}

/// Lightweight logging hook so host apps can route library logs to their preferred system.
/// The library stays dependency-free and only calls through this protocol.
public protocol LogProvider: Sendable {
  func log(_ level: LogLevel, message: String, metadata: [String: String]?)
}

public struct NoopLogProvider: LogProvider {
  public init() {}
  public func log(_ level: LogLevel, message: String, metadata: [String: String]?) {
    // Intentionally no-op; keeps library silent unless a provider is injected.
  }
}
