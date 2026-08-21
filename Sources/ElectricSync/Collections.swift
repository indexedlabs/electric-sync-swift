import Foundation

final class OutputBox<Output>: @unchecked Sendable {
  var value: Output?

  init(_ value: Output? = nil) {
    self.value = value
  }
}

private final class MutableValueBox<Value>: @unchecked Sendable {
  var value: Value

  init(_ value: Value) {
    self.value = value
  }
}

private struct AppliedBatchResult: Sendable {
  let encounteredTruncate: Bool
  let missingRowKeys: [String]
}

private struct AtomicChunkApplyResult<T: ElectricCollectionModel>: Sendable {
  let index: Int
  let output: SyncBatch<T>.Output
  let durationMs: Double
}

private struct AtomicBoundaryApplyResult<T: ElectricCollectionModel>: Sendable {
  let chunks: [AtomicChunkApplyResult<T>]
  let truncateSwapPreparation: SyncBatch<T>.TruncateSwapPreparation?
}

/// Applies decode-sized chunks inside one writer transaction so GRDB observers
/// can only see the complete Electric commitment boundary. Chunking limits
/// transient decode/output storage and preserves per-chunk tracing; it is not a
/// publication boundary.
private func applyChunksInSingleTransaction<T: ElectricCollectionModel>(
  _ chunks: [SyncBatch<T>],
  runtimeProvider: ElectricSyncRuntimeProvider,
  transactionRunner: ElectricTransactionRunner,
  shouldPrepareTruncateSwap: Bool,
  validatePublication: @escaping @Sendable () throws -> Void,
  tracer: any ElectricSyncTracer,
  chunkSpanName: String,
  chunkAttributes: @escaping @Sendable (Int, Int, SyncBatch<T>) -> [String: String],
  chunkWillApply: @escaping @Sendable (Int, Int, SyncBatch<T>) -> Void
) async throws -> AtomicBoundaryApplyResult<T> {
  let resultBox = OutputBox(
    AtomicBoundaryApplyResult<T>(chunks: [], truncateSwapPreparation: nil)
  )
  let trackerMutationAttempted = OutputBox(false)

  do {
    try await transactionRunner { context in
      var appliedChunks: [AtomicChunkApplyResult<T>] = []
      appliedChunks.reserveCapacity(chunks.count)
      var truncateSwapPreparation: SyncBatch<T>.TruncateSwapPreparation?

      for (chunkIndex, chunk) in chunks.enumerated() {
        try validatePublication()
        chunkWillApply(chunkIndex, chunks.count, chunk)
        let chunkStart = runtimeProvider.now()
        let output = try withElectricSyncSpan(
          tracer: tracer,
          name: chunkSpanName,
          attributes: chunkAttributes(chunkIndex, chunks.count, chunk)
        ) { _ in
          if shouldPrepareTruncateSwap, chunkIndex == 0 {
            truncateSwapPreparation = try chunk.prepareTruncateSwap(in: context)
          }
          trackerMutationAttempted.value = true
          return try chunk.apply(in: context)
        }
        let durationMs = max(0, runtimeProvider.now().timeIntervalSince(chunkStart) * 1000)
        appliedChunks.append(
          AtomicChunkApplyResult(index: chunkIndex, output: output, durationMs: durationMs)
        )
        if output.encounteredTruncate {
          break
        }
      }

      resultBox.value = AtomicBoundaryApplyResult(
        chunks: appliedChunks,
        truncateSwapPreparation: truncateSwapPreparation
      )
    }
  } catch {
    // SyncBatch folds process-local move/DNF state while applying. The writer
    // rollback restores GRDB, and resetting the tracker prevents that rejected
    // fold from escaping the transaction; the owner will safely bootstrap it.
    if trackerMutationAttempted.value == true {
      chunks.first?.moveOutTracker.reset()
    }
    throw error
  }

  return resultBox.value
    ?? AtomicBoundaryApplyResult(chunks: [], truncateSwapPreparation: nil)
}

private struct AppliedPollResult<T: ElectricCollectionModel>: Sendable {
  let batch: SyncBatch<T>?
  let applyResult: AppliedBatchResult?
  let fetchDurationMs: Double
}

private func truncateSwapBatchMetadata<T: ElectricCollectionModel>(
  batch: SyncBatch<T>,
  table: String,
  predicate: SQLExpression?,
  collectionIdentifier: String
) -> [String: String] {
  let messages = batch.messages
  let messageCount = messages.count
  let emptyPayloadCount = messages.reduce(0) { $0 + ($1.payload.isEmpty ? 1 : 0) }
  let payloadBytes = messages.reduce(0) { $0 + $1.payload.count }
  let kindControlCounts = messages.reduce(into: [String: Int]()) { counts, message in
    let controlLabel = message.control.map { String(describing: $0) } ?? "none"
    let key = "\(message.kind)-\(controlLabel)"
    counts[key, default: 0] += 1
  }
  let kindControlSummary =
    kindControlCounts
    .sorted { $0.key < $1.key }
    .map { "\($0.key)=\($0.value)" }
    .joined(separator: ",")

  let first = messages.first
  let last = messages.last

  let hasTruncate = messages.contains { $0.kind == .truncate }
  let hasMustRefetch = messages.contains { $0.control == .mustRefetch }
  let hasUpToDate = messages.contains { $0.control == .upToDate || $0.isUpToDate }
  let hasSnapshotEnd = messages.contains { $0.control == .snapshotEnd }

  return [
    "table": table,
    "collection": collectionIdentifier,
    "predicate": predicate?.rawValue ?? "<nil>",
    "messageCount": "\(messageCount)",
    "emptyPayloads": "\(emptyPayloadCount)",
    "payloadBytes": "\(payloadBytes)",
    "kindControlCounts": kindControlSummary,
    "firstOffset": first?.offset ?? "<nil>",
    "lastOffset": last?.offset ?? "<nil>",
    "firstHandle": first?.handle ?? "<nil>",
    "lastHandle": last?.handle ?? "<nil>",
    "firstCursor": first?.cursor ?? "<nil>",
    "lastCursor": last?.cursor ?? "<nil>",
    "hasTruncate": "\(hasTruncate)",
    "hasMustRefetch": "\(hasMustRefetch)",
    "hasUpToDate": "\(hasUpToDate)",
    "hasSnapshotEnd": "\(hasSnapshotEnd)",
  ]
}

private func withTimingMetadata(
  _ metadata: [String: String],
  durationMs: Double
) -> [String: String] {
  var merged = metadata
  merged["durationMs"] = String(format: "%.2f", durationMs)
  return merged
}

private let electricBatchApplyChunkSize = 200
private let electricSlowFetchThresholdMs: Double = 200
private let electricSlowApplyChunkThresholdMs: Double = 150
private let electricSlowApplyBatchThresholdMs: Double = 300
private let electricVerySlowPollTransportThresholdMs: Double = 15_000
private let electricPollTransportInfoSampleEvery = 30
private let electricExpectedTransportIssueSampleEvery = 20
private let electricTransientHydrationMaxPasses = 5

private func isBoundedWorkingSetDescriptor(_ descriptor: QueryDescriptor) -> Bool {
  // A reset seed is a bounded *working-set* declaration, not a convenient
  // spelling for an unscoped shape. A structured finite predicate is enough
  // (for example `to_id IN <frontier>`); a limit is optional, but when a
  // caller supplies one it must be positive.
  guard descriptor.limit.map({ $0 > 0 }) ?? true else { return false }
  guard let predicate = descriptor.predicate?.predicate else { return false }
  if case .constant(true) = predicate.normalized() { return false }
  return true
}

private enum ElectricExpectedTransportIssue: String {
  case cancelled
  case longPollTimeout = "long_poll_timeout"

  var logLevel: LogLevel {
    switch self {
    case .cancelled:
      return .debug
    case .longPollTimeout:
      return .info
    }
  }

  var message: String {
    switch self {
    case .cancelled:
      return "Electric poll request cancelled"
    case .longPollTimeout:
      return "Electric poll transport timeout"
    }
  }
}

private func electricExpectedTransportIssue(
  error: Error,
  transport: ElectricLiveTransport
) -> ElectricExpectedTransportIssue? {
  guard let code = electricURLErrorCode(for: error) else { return nil }
  switch code {
  case .cancelled:
    return .cancelled
  case .timedOut where transport == .longPoll:
    return .longPollTimeout
  default:
    return nil
  }
}

private func electricURLErrorCode(for error: Error) -> URLError.Code? {
  if let urlError = error as? URLError {
    return urlError.code
  }

  let nsError = error as NSError
  guard nsError.domain == NSURLErrorDomain else { return nil }
  return URLError.Code(rawValue: nsError.code)
}

public enum ElectricCollectionSyncMode: Hashable, Sendable {
  case eager
  case onDemand
  case progressive
}

public enum ElectricLiveTransport: Hashable, Sendable {
  case longPoll
  case sse
  case sseWithFallback
}

public enum ElectricShapeTopology: Hashable, Sendable {
  case dnf
  case staticallySimple
}

/// Controls how an on-demand owner replaces a lost process-local DNF tracker.
///
/// The default deliberately preserves the historical full-bootstrap recovery.
/// `replaceExclusiveWorkingSetFromDemandedSubsets` is only admitted for an
/// exclusive table with durable ownership metadata. Its caller is responsible
/// for keeping a bounded, currently-needed working set of owner demands.
public enum ElectricTrackerContinuityRecoveryPolicy: Hashable, Sendable {
  case fullBootstrap
  case replaceExclusiveWorkingSetFromDemandedSubsets
}

public struct ElectricCollectionConfiguration<T: ElectricCollectionModel>: Sendable {
  public let modelType: T.Type
  public let identifier: String
  public let syncMode: ElectricCollectionSyncMode
  public let live: Bool
  public let liveTransport: ElectricLiveTransport
  /// Predicate applied at the shape endpoint layer (e.g. mapped into query params).
  /// This is distinct from the subset predicate used for query-driven loads.
  public let basePredicate: SQLExpression?
  public let predicate: SQLExpression?
  public let orderBy: [OrderBy]
  public let limit: Int?
  public let shapeTopology: ElectricShapeTopology
  public let trackerContinuityRecoveryPolicy: ElectricTrackerContinuityRecoveryPolicy

  public init(
    modelType: T.Type,
    identifier: String? = nil,
    syncMode: ElectricCollectionSyncMode = .onDemand,
    live: Bool = false,
    liveTransport: ElectricLiveTransport = .longPoll,
    basePredicate: SQLExpression? = nil,
    predicate: SQLExpression? = nil,
    orderBy: [OrderBy] = [],
    limit: Int? = nil,
    shapeTopology: ElectricShapeTopology = .dnf,
    trackerContinuityRecoveryPolicy: ElectricTrackerContinuityRecoveryPolicy = .fullBootstrap
  ) {
    self.modelType = modelType
    self.identifier = identifier ?? modelType.collectionIdentifier
    self.syncMode = syncMode
    self.live = live
    self.liveTransport = liveTransport
    self.basePredicate = basePredicate
    self.predicate = predicate
    self.orderBy = orderBy
    self.limit = limit
    self.shapeTopology = shapeTopology
    self.trackerContinuityRecoveryPolicy = trackerContinuityRecoveryPolicy
  }
}

public enum CollectionLoadingTransition: Sendable {
  case start
  case end
}

public struct CollectionLoadingEvent: Sendable {
  public let transition: CollectionLoadingTransition
}

public struct ElectricSubsetResult<T: ElectricCollectionModel>: Sendable {
  public let appliedRecords: [T]
  public let localRecords: [T]
  public let observation: SubsetObservation?

  public init(
    appliedRecords: [T],
    localRecords: [T],
    observation: SubsetObservation? = nil
  ) {
    self.appliedRecords = appliedRecords
    self.localRecords = localRecords
    self.observation = observation
  }
}

/// Retains one bounded on-demand working-set demand and its shared live tail.
/// Release it when the associated screen/route is no longer visible.
public final class ElectricSubsetDemandLease: @unchecked Sendable {
  private let lock = NSLock()
  private var onCancel: (@Sendable () -> Void)?

  fileprivate init(onCancel: @escaping @Sendable () -> Void) {
    self.onCancel = onCancel
  }

  public func cancel() {
    let action = lock.withLock { () -> (@Sendable () -> Void)? in
      defer { onCancel = nil }
      return onCancel
    }
    action?()
  }

  deinit { cancel() }
}

public struct ElectricSubsetDemandActivation<T: ElectricCollectionModel>: Sendable {
  public let lease: ElectricSubsetDemandLease
  public let result: ElectricSubsetResult<T>

  public init(lease: ElectricSubsetDemandLease, result: ElectricSubsetResult<T>) {
    self.lease = lease
    self.result = result
  }
}

public struct ElectricCollection<T: ElectricCollectionModel>: Sendable {
  public let configuration: ElectricCollectionConfiguration<T>
  public let replica: ElectricShapeReplica<T>

  private var client: ElectricSyncClientImpl { replica.client }
  private var runtimeProvider: ElectricSyncRuntimeProvider { client.runtimeProvider }
  private var sessionProvider: ElectricSyncSessionProvider { client.sessionProvider }
  private var coordinator: ElectricCollectionBackgroundCoordinator<T> { replica.coordinator }
  private var cacheProvider: any DataCacheProvider { replica.cacheProvider }
  private var transactionRunner: ElectricTransactionRunner { replica.transactionRunner }
  private var eventHandler: any ElectricSyncEventHandler { replica.eventHandler }
  private var backgroundTaskProvider: any BackgroundTaskProvider { replica.backgroundTaskProvider }
  private var logger: any LogProvider { replica.logger }
  private var tracer: any ElectricSyncTracer { replica.tracer }

  internal var streamManagerClientId: ObjectIdentifier {
    ObjectIdentifier(client)
  }

  internal var diagnosticsLogger: any LogProvider {
    logger
  }

  internal var diagnosticsTracer: any ElectricSyncTracer {
    tracer
  }

  public init(
    configuration: ElectricCollectionConfiguration<T>,
    client: ElectricSyncClientImpl,
    cacheProvider: any DataCacheProvider,
    transactionRunner:
      @escaping @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws
      -> Void,
    eventHandler: any ElectricSyncEventHandler = NoopElectricSyncEventHandler(),
    backgroundTaskProvider: any BackgroundTaskProvider = NoopBackgroundTaskProvider(),
    logger: any LogProvider = NoopLogProvider(),
    tracer: (any ElectricSyncTracer)? = nil
  ) {
    let resolvedTracer = tracer ?? NoopElectricSyncTracer()
    self.configuration = configuration
    self.replica = ElectricShapeReplica(
      identity: ElectricReplicaIdentity(
        modelType: T.self,
        modelIdentifier: configuration.identifier,
        basePredicate: configuration.basePredicate
      ),
      basePredicate: configuration.basePredicate,
      syncMode: configuration.syncMode,
      client: client,
      cacheProvider: cacheProvider,
      transactionRunner: transactionRunner,
      eventHandler: eventHandler,
      backgroundTaskProvider: backgroundTaskProvider,
      logger: logger,
      tracer: resolvedTracer
    )
  }

  public init(
    configuration: ElectricCollectionConfiguration<T>,
    replica: ElectricShapeReplica<T>
  ) {
    self.configuration = configuration
    self.replica = replica
  }

  public func query(
    where predicate: SQLExpression? = nil,
    orderBy: [OrderBy]? = nil,
    limit: Int? = nil,
    cursor: ElectricCursorExpressions? = nil
  ) async throws -> [T] {
    guard let session = sessionProvider.captureAuthenticatedSession() else {
      throw CancellationError()
    }
    return try await query(
      where: predicate,
      orderBy: orderBy,
      limit: limit,
      cursor: cursor,
      session: session
    )
  }

  public func query(
    where predicate: SQLExpression? = nil,
    orderBy: [OrderBy]? = nil,
    limit: Int? = nil,
    cursor: ElectricCursorExpressions? = nil,
    session: ElectricSyncSession?
  ) async throws -> [T] {
    let queryStart = runtimeProvider.now()
    let queryMetadata: [String: String] = [
      "table": T.tableName,
      "collection": configuration.identifier,
      "predicate": predicate.map { String($0.rawValue.prefix(200)) } ?? "<nil>",
      "limit": limit.map(String.init) ?? "<nil>",
    ]
    logger.log(
      .info,
      message: "Electric subset query requested",
      metadata: queryMetadata
    )
    func logOutcome(_ outcome: String, rows: Int?, error: (any Error)? = nil) {
      let durationMs = max(0, runtimeProvider.now().timeIntervalSince(queryStart) * 1000)
      logger.log(
        error == nil || error is CancellationError ? .info : .warning,
        message: "Electric subset query \(outcome)",
        metadata: queryMetadata.merging([
          "durationMs": String(format: "%.1f", durationMs),
          "rows": rows.map(String.init) ?? "<nil>",
          "error": error.map { "\($0)" } ?? "<nil>",
        ]) { _, new in new }
      )
    }
    guard replica.isAcceptingWork else {
      logOutcome("rejected (not accepting work)", rows: nil)
      throw CancellationError()
    }
    do {
      let rows = try await queryAdmitted(
        where: predicate,
        orderBy: orderBy,
        limit: limit,
        cursor: cursor,
        session: session
      )
      logOutcome("applied", rows: rows.count)
      return rows
    } catch {
      logOutcome("failed", rows: nil, error: error)
      throw error
    }
  }

  private func queryAdmitted(
    where predicate: SQLExpression? = nil,
    orderBy: [OrderBy]? = nil,
    limit: Int? = nil,
    cursor: ElectricCursorExpressions? = nil,
    session: ElectricSyncSession?
  ) async throws -> [T] {
    guard replica.isAcceptingWork else {
      throw CancellationError()
    }
    try await client.validateWorkingSetResetConfiguration(
      T.self,
      identity: replica.identity,
      syncMode: configuration.syncMode,
      shapeTopology: configuration.shapeTopology,
      recoveryPolicy: configuration.trackerContinuityRecoveryPolicy
    )
    let effectivePredicate = predicate ?? configuration.predicate
    let effectiveOrderBy = orderBy ?? configuration.orderBy
    let effectiveLimit = limit ?? configuration.limit

    let descriptor = QueryDescriptor(
      predicate: effectivePredicate,
      orderBy: effectiveOrderBy,
      limit: effectiveLimit,
      cursor: cursor
    )

    return try await coordinator.performCommand {
      _ = try await client.withLegacyBootstrapAdmission(
        identity: replica.identity,
        stage: "query",
        syncMode: configuration.syncMode,
        expectsExactCursorAdvance: false
      ) {
        try await coordinator.performQuery(
          client: client,
          basePredicate: configuration.basePredicate,
          shapeTopology: configuration.shapeTopology,
          trackerContinuityRecoveryPolicy: configuration.trackerContinuityRecoveryPolicy,
          descriptor: descriptor,
          demandSyncMode: configuration.syncMode,
          session: session,
          snapshotReplica: replica,
          transactionRunner: transactionRunner,
          eventHandler: eventHandler
        )
      }

      try Task.checkCancellation()
      let records = try await cacheProvider.load(
        T.self,
        request: descriptor
      )
      try Task.checkCancellation()
      return records
    }
  }

  /// Ensures an authoritative subset through this collection's owning replica.
  ///
  /// The owner serializes the request with its live publication pipeline and
  /// advances resume metadata in the same transaction as the terminal subset
  /// boundary. Unlike `query`, this always issues a fresh network read instead
  /// of consulting reusable fetch coverage. The result contains both records
  /// materialized by this response and matching rows from the cache after apply
  /// commits.
  public func ensureSubset(
    where predicate: SQLExpression,
    orderBy: [OrderBy] = [],
    limit: Int? = nil
  ) async throws -> ElectricSubsetResult<T> {
    guard replica.isAcceptingWork else {
      throw CancellationError()
    }
    guard let session = sessionProvider.captureAuthenticatedSession() else {
      throw CancellationError()
    }
    return try await ensureSubset(
      where: predicate,
      orderBy: orderBy,
      limit: limit,
      session: session,
      finalizing: { _ in }
    )
  }

  public func ensureSubset(
    where predicate: SQLExpression,
    orderBy: [OrderBy] = [],
    limit: Int? = nil,
    session: ElectricSyncSession?
  ) async throws -> ElectricSubsetResult<T> {
    try await ensureSubset(
      where: predicate,
      orderBy: orderBy,
      limit: limit,
      session: session,
      finalizing: { _ in }
    )
  }

  /// Ensures an authoritative subset and runs `finalize` before releasing this owner's
  /// publication gate. The finalizer can publish related durable state without racing a newer
  /// live owner publication after the returned snapshot.
  public func ensureSubset(
    where predicate: SQLExpression,
    orderBy: [OrderBy] = [],
    limit: Int? = nil,
    finalizing finalize: @escaping @Sendable (ElectricSubsetResult<T>) async throws -> Void
  ) async throws -> ElectricSubsetResult<T> {
    guard replica.isAcceptingWork else {
      throw CancellationError()
    }
    guard let session = sessionProvider.captureAuthenticatedSession() else {
      throw CancellationError()
    }
    return try await ensureSubset(
      where: predicate,
      orderBy: orderBy,
      limit: limit,
      session: session,
      finalizing: finalize
    )
  }

  public func ensureSubset(
    where predicate: SQLExpression,
    orderBy: [OrderBy] = [],
    limit: Int? = nil,
    session: ElectricSyncSession?,
    finalizing finalize: @escaping @Sendable (ElectricSubsetResult<T>) async throws -> Void
  ) async throws -> ElectricSubsetResult<T> {
    try await ensureSubsetInternal(
      where: predicate,
      orderBy: orderBy,
      limit: limit,
      session: session,
      isActiveWorkingSetRecoverySeed: false,
      ownerPublicationAlreadyHeld: false,
      finalizing: finalize
    )
  }

  private func ensureSubsetInternal(
    where predicate: SQLExpression,
    orderBy: [OrderBy] = [],
    limit: Int? = nil,
    session: ElectricSyncSession?,
    deferWorkingSetTailReadiness _: Bool = false,
    isActiveWorkingSetRecoverySeed: Bool = false,
    ownerPublicationAlreadyHeld: Bool = false,
    finalizing finalize: @escaping @Sendable (ElectricSubsetResult<T>) async throws -> Void
  ) async throws -> ElectricSubsetResult<T> {
    guard replica.isAcceptingWork else {
      throw CancellationError()
    }
    let isSessionCurrent: @Sendable () -> Bool = {
      guard let session else { return false }
      return sessionProvider.isCurrent(session)
    }
    guard isSessionCurrent() else {
      throw CancellationError()
    }

    let descriptor = QueryDescriptor(
      predicate: predicate,
      orderBy: orderBy,
      limit: limit
    )
    let ownerSyncMode = configuration.syncMode
    let subsetStart = runtimeProvider.now()
    let subsetMetadata: [String: String] = [
      "table": T.tableName,
      "collection": configuration.identifier,
      "predicate": String(predicate.rawValue.prefix(200)),
      "limit": limit.map(String.init) ?? "<nil>",
    ]
    logger.log(
      .info,
      message: "Electric subset snapshot requested",
      metadata: subsetMetadata
    )
    let usesConfiguredWorkingSetReset = await client.usesConfiguredOnDemandWorkingSetReset(
      T.self,
      identity: replica.identity,
      syncMode: ownerSyncMode,
      shapeTopology: configuration.shapeTopology,
      recoveryPolicy: configuration.trackerContinuityRecoveryPolicy
    )
    try await client.validateWorkingSetResetConfiguration(
      T.self,
      identity: replica.identity,
      syncMode: ownerSyncMode,
      shapeTopology: configuration.shapeTopology,
      recoveryPolicy: configuration.trackerContinuityRecoveryPolicy
    )
    if usesConfiguredWorkingSetReset, replica.isTrackerContinuityUnavailable,
      !isActiveWorkingSetRecoverySeed
    {
      // `ensureSubset` is intentionally not a recovery entry point. A
      // one-shot read has no lifetime lease to replay after a later owner
      // eviction, so accepting it would leave the tail either unsafe or
      // silently historical.
      throw ElectricSyncError.fetchFailed(
        "exclusive working-set recovery requires activateDemand"
      )
    }
    do {
      let ownerOperation: @Sendable () async throws -> ElectricSubsetResult<T> = {
        let appliedRecords = try await coordinator.performQuery(
          client: client,
          basePredicate: configuration.basePredicate,
          shapeTopology: configuration.shapeTopology,
          trackerContinuityRecoveryPolicy: configuration.trackerContinuityRecoveryPolicy,
          descriptor: descriptor,
          demandSyncMode: ownerSyncMode,
          session: session,
          snapshotReplica: replica,
          ownerSnapshotDemand: true,
          ownerPublicationAlreadyHeld: true,
          consultFetchCoverage: false,
          recordAsObservation: true,
          transactionRunner: transactionRunner,
          eventHandler: eventHandler
        )

        try Task.checkCancellation()
        guard isSessionCurrent() else { throw CancellationError() }
        let localRecords = try await cacheProvider.load(T.self, request: descriptor)
        let observation = try await client.latestSubsetObservation(
          table: T.tableName,
          basePredicate: configuration.basePredicate,
          descriptor: descriptor
        )
        let result = ElectricSubsetResult(
          appliedRecords: appliedRecords,
          // The response itself is an authoritative local view at the
          // terminal subset boundary. Some lightweight cache providers only
          // understand raw SQL and cannot re-evaluate a structured predicate;
          // preserve the materialized response for route-frontier callers.
          localRecords: localRecords.isEmpty ? appliedRecords : localRecords,
          observation: observation
        )
        try await finalize(result)
        return result
      }
      let result = try await coordinator.performCommand {
        try await client.withLegacyBootstrapAdmission(
          identity: replica.identity,
          stage: "request_snapshot",
          syncMode: ownerSyncMode,
          expectsExactCursorAdvance: false
        ) {
          if ownerPublicationAlreadyHeld {
            return try await ownerOperation()
          }
          return try await replica.ensureSubset {
            try await ownerOperation()
          }
        }
      }
      let durationMs = max(0, runtimeProvider.now().timeIntervalSince(subsetStart) * 1000)
      logger.log(
        .info,
        message: "Electric subset snapshot applied",
        metadata: subsetMetadata.merging([
          "durationMs": String(format: "%.1f", durationMs),
          "appliedRecords": "\(result.appliedRecords.count)",
          "localRecords": "\(result.localRecords.count)",
          "hasObservation": "\(result.observation != nil)",
        ]) { _, new in new }
      )
      return result
    } catch {
      let durationMs = max(0, runtimeProvider.now().timeIntervalSince(subsetStart) * 1000)
      logger.log(
        error is CancellationError ? .info : .warning,
        message: "Electric subset snapshot failed",
        metadata: subsetMetadata.merging([
          "durationMs": String(format: "%.1f", durationMs),
          "error": "\(error)",
          "cancelled": "\(error is CancellationError)",
        ]) { _, new in new }
      )
      throw error
    }
  }

  public func loadingEvents() async -> AsyncStream<CollectionLoadingEvent> {
    await coordinator.loadingEventsStream()
  }

  /// Seeds a bounded route demand and keeps the canonical live tail retained
  /// for the returned lease's lifetime. The result is the initial local view
  /// of that demand, so callers can derive a next lineage frontier without a
  /// second cache read.
  public func activateDemand(
    where predicate: SQLExpression,
    orderBy: [OrderBy] = [],
    limit: Int? = nil,
    session: ElectricSyncSession? = nil
  ) async throws -> ElectricSubsetDemandActivation<T> {
    guard isBoundedWorkingSetDescriptor(
      QueryDescriptor(predicate: predicate, orderBy: orderBy, limit: limit)
    ) else {
      throw ElectricSyncError.fetchFailed("working-set demand must be bounded")
    }
    let resolvedSession = session ?? sessionProvider.captureAuthenticatedSession()
    guard let resolvedSession else { throw CancellationError() }
    try await client.validateWorkingSetResetConfiguration(
      T.self,
      identity: replica.identity,
      syncMode: configuration.syncMode,
      shapeTopology: configuration.shapeTopology,
      recoveryPolicy: configuration.trackerContinuityRecoveryPolicy
    )
    let descriptor = QueryDescriptor(predicate: predicate, orderBy: orderBy, limit: limit)
    // Register before the first suspension. A concurrent owner-loss recovery
    // therefore sees this route in its replay inventory rather than treating
    // it as an after-the-fact one-shot query.
    let leaseID = await replica.registerActiveDemand(descriptor)
    let result: ElectricSubsetResult<T>
    var recoveryEpoch: UInt64?
    do {
      let usesWorkingSetReset = await client.usesConfiguredOnDemandWorkingSetReset(
        T.self,
        identity: replica.identity,
        syncMode: configuration.syncMode,
        shapeTopology: configuration.shapeTopology,
        recoveryPolicy: configuration.trackerContinuityRecoveryPolicy
      )
      let recoveryNeedsSeed = await replica.workingSetRecoveryNeedsSeed()
      let mustRecover = usesWorkingSetReset && (
        replica.isTrackerContinuityUnavailable || recoveryNeedsSeed
      )
      if mustRecover {
        if !replica.isTrackerContinuityUnavailable {
          // The last lease had parked an otherwise-continuous tail. A new
          // lease needs only its ordinary subset, not a generation reset.
          await replica.resumeWorkingSetTailForActiveLease()
          result = try await ensureSubset(
            where: predicate, orderBy: orderBy, limit: limit, session: resolvedSession
          )
        } else if let recoveryToken = await replica.startWorkingSetRecoveryIfNeeded() {
          recoveryEpoch = recoveryToken.epoch
          // This is the only external recovery leader. Hold the snapshot gate
          // across every revision pass so the canonical tail is parked once,
          // not once per descriptor.
          result = try await replica.ensureSubset {
            try await recoverActiveWorkingSet(
              session: resolvedSession,
              prioritizing: descriptor,
              captureResultFor: descriptor,
              ownerPublicationAlreadyHeld: true,
              recoveryEpoch: recoveryToken.epoch,
              lossGeneration: recoveryToken.lossGeneration
            )
          }
        } else {
          try await replica.waitForWorkingSetTailReadiness()
          // Another activation performed the network seed. `localRecords` is
          // authoritative after its transaction; `appliedRecords` remains
          // empty because this caller did not issue that response.
          result = try await restoredLocalSubsetResult(descriptor)
        }
      } else {
        result = try await ensureSubset(
          where: predicate,
          orderBy: orderBy,
          limit: limit,
          session: resolvedSession
        )
      }
    } catch {
      if let recoveryEpoch { replica.failWorkingSetRecovery(epoch: recoveryEpoch) }
      replica.releaseActiveDemand(leaseID)
      throw error
    }
    let streamToken = keepSynced(session: resolvedSession)
    let lease = ElectricSubsetDemandLease(onCancel: {
      replica.releaseActiveDemand(leaseID)
      streamToken.cancel()
    })
    return ElectricSubsetDemandActivation(lease: lease, result: result)
  }

  /// Seeds stable snapshots of the actor-owned lease inventory. The caller
  /// chooses whether it already owns the stream publication gate; this is the
  /// key distinction between an external activation (snapshot gate) and a
  /// running tail after owner loss (stream gate), avoiding self-pause.
  private func recoverActiveWorkingSet(
    session: ElectricSyncSession?,
    prioritizing descriptor: QueryDescriptor? = nil,
    captureResultFor capturedDescriptor: QueryDescriptor? = nil,
    ownerPublicationAlreadyHeld: Bool,
    recoveryEpoch: UInt64,
    lossGeneration: UInt64
  ) async throws -> ElectricSubsetResult<T> {
    var capturedResult: ElectricSubsetResult<T>?
    var seededDescriptors = Set<QueryDescriptor>()
    while true {
      guard await replica.isWorkingSetRecoveryCurrent(
        epoch: recoveryEpoch,
        lossGeneration: lossGeneration
      ) else {
        throw CancellationError()
      }
      let snapshot = await replica.workingSetRecoverySnapshot(prioritizing: descriptor)
      guard !snapshot.descriptors.isEmpty else {
        throw CancellationError()
      }
      for activeDescriptor in snapshot.descriptors {
        guard seededDescriptors.insert(activeDescriptor).inserted else { continue }
        guard let activePredicate = activeDescriptor.predicate,
          isBoundedWorkingSetDescriptor(activeDescriptor)
        else {
          throw ElectricSyncError.fetchFailed("active working-set lease is not bounded")
        }
        let activeResult = try await ensureSubsetInternal(
          where: activePredicate,
          orderBy: activeDescriptor.orderBy,
          limit: activeDescriptor.limit,
          session: session,
          deferWorkingSetTailReadiness: true,
          isActiveWorkingSetRecoverySeed: true,
          ownerPublicationAlreadyHeld: ownerPublicationAlreadyHeld,
          finalizing: { _ in }
        )
        if activeDescriptor == capturedDescriptor { capturedResult = activeResult }
      }
      // Perform any throw-capable cache reconciliation while the epoch is
      // still parked. Once completion resumes the tail, this function must
      // not be able to invalidate an already-established generation.
      let finalResult: ElectricSubsetResult<T>
      if let capturedResult {
        finalResult = capturedResult
      } else {
        finalResult = try await restoredLocalSubsetResult(
          capturedDescriptor ?? snapshot.descriptors[0]
        )
      }
      if await replica.completeWorkingSetRecoveryIfStable(
        revision: snapshot.revision,
        epoch: recoveryEpoch,
        lossGeneration: lossGeneration
      ) {
        return finalResult
      }
      // A lease appeared/disappeared while the prior inventory was being
      // seeded. It is intentionally replayed before the tail is released.
    }
  }

  private func restoredLocalSubsetResult(_ descriptor: QueryDescriptor) async throws -> ElectricSubsetResult<T> {
    let localRecords = try await cacheProvider.load(T.self, request: descriptor)
    let observation = try await client.latestSubsetObservation(
      table: T.tableName,
      basePredicate: configuration.basePredicate,
      descriptor: descriptor
    )
    return ElectricSubsetResult(
      appliedRecords: [],
      localRecords: localRecords,
      observation: observation
    )
  }

  /// Returns an AsyncStream that keeps the collection in sync by repeatedly issuing queries.
  /// Callers can `for await _ in collection.subscribe(...)`.
  public func subscribe(
    where predicate: SQLExpression? = nil,
    orderBy: [OrderBy]? = nil,
    limit: Int? = nil,
    circuitBreaker: (any CircuitBreakerStrategy)? = nil
  ) -> AsyncStream<Void> {
    guard replica.isAcceptingWork else {
      return AsyncStream { continuation in
        continuation.finish()
      }
    }
    guard let session = sessionProvider.captureAuthenticatedSession() else {
      return AsyncStream { continuation in
        continuation.finish()
      }
    }
    return subscribe(
      where: predicate,
      orderBy: orderBy,
      limit: limit,
      session: session,
      circuitBreaker: circuitBreaker
    )
  }

  public func subscribe(
    where predicate: SQLExpression? = nil,
    orderBy: [OrderBy]? = nil,
    limit: Int? = nil,
    session: ElectricSyncSession?,
    circuitBreaker: (any CircuitBreakerStrategy)? = nil,
    protocolSyncMode: ElectricCollectionSyncMode? = nil
  ) -> AsyncStream<Void> {
    subscribe(
      where: predicate,
      orderBy: orderBy,
      limit: limit,
      session: session,
      circuitBreaker: circuitBreaker,
      protocolSyncMode: protocolSyncMode,
      cancellationRelay: nil
    )
  }

  func subscribe(
    where predicate: SQLExpression? = nil,
    orderBy: [OrderBy]? = nil,
    limit: Int? = nil,
    session: ElectricSyncSession?,
    circuitBreaker: (any CircuitBreakerStrategy)? = nil,
    protocolSyncMode: ElectricCollectionSyncMode? = nil,
    cancellationRelay: ElectricSubscriptionCancellationRelay?
  ) -> AsyncStream<Void> {
    let relay = cancellationRelay ?? ElectricSubscriptionCancellationRelay()
    guard replica.isAcceptingWork else {
      relay.finish()
      return AsyncStream { continuation in
        continuation.finish()
      }
    }
    let effectivePredicate = predicate ?? configuration.predicate
    let collectionIdentifier = configuration.identifier
    let tracer = self.tracer
    let streamSyncMode = protocolSyncMode ?? configuration.syncMode

    let breakerSeed: any CircuitBreakerStrategy =
      circuitBreaker
      ?? ExponentialBackoffCircuitBreaker()

    return AsyncStream { continuation in
      let task = Task.detached(priority: .utility) {
        defer {
          relay.finish()
          continuation.finish()
        }
        let isSessionCurrent: @Sendable () -> Bool = {
          guard let session else { return false }
          return sessionProvider.isCurrent(session)
        }
        do {
          try await client.validateWorkingSetResetConfiguration(
            T.self,
            identity: replica.identity,
            syncMode: streamSyncMode,
            shapeTopology: configuration.shapeTopology,
            recoveryPolicy: configuration.trackerContinuityRecoveryPolicy
          )
        } catch {
          logger.log(
            .warning,
            message: "Electric stream rejected invalid working-set recovery configuration",
            metadata: ["table": T.tableName, "collection": collectionIdentifier, "error": "\(error)"]
          )
          return
        }

        var breaker = breakerSeed
        var transport = configuration.liveTransport
        var consecutiveSSEFailures = 0
        let pendingTruncateSwap = OutputBox(false)
        let truncateSwapPrepared = OutputBox(false)
        var truncateAttempts = 0
        var pollTransportWaitEventCount = 0
        var expectedTransportIssueCount = 0
        var hasUsedProtocolFullBootstrapRecovery = false
        var forceProtocolFullBootstrapOnNextPoll = false
        // A tagged owner whose process-local membership tracker lost
        // continuity (fresh process, idle eviction, suspension, reset) must
        // discard incremental resume and full-bootstrap before going live.
        let initiallyRequiresFullBootstrapForTrackerContinuity: Bool
        if replica.isTrackerContinuityUnavailable {
          initiallyRequiresFullBootstrapForTrackerContinuity =
            try await client
            .requiresFullBootstrapForTrackerContinuity(
              T.self,
              identity: replica.identity,
              syncMode: streamSyncMode,
              shapeTopology: configuration.shapeTopology,
              recoveryPolicy: configuration.trackerContinuityRecoveryPolicy
            )
        } else {
          initiallyRequiresFullBootstrapForTrackerContinuity = false
        }
        var mustFullBootstrapForTrackerContinuity =
          initiallyRequiresFullBootstrapForTrackerContinuity
        let usesConfiguredWorkingSetReset = await client.usesConfiguredOnDemandWorkingSetReset(
          T.self,
          identity: replica.identity,
          syncMode: streamSyncMode,
          shapeTopology: configuration.shapeTopology,
          recoveryPolicy: configuration.trackerContinuityRecoveryPolicy
        )
        if usesConfiguredWorkingSetReset && mustFullBootstrapForTrackerContinuity {
          // An opted-in DNF owner has no safe incremental tracker yet. Its
          // bounded owner demand will establish the new generation; do not
          // race that seed with an unfiltered `-1` replay.
          mustFullBootstrapForTrackerContinuity = false
        }

        let markTruncateSwapPending: @Sendable (Bool) -> Void = { [replica] isPending in
          guard pendingTruncateSwap.value != isPending else { return }
          pendingTruncateSwap.value = isPending
          truncateSwapPrepared.value = false
          replica.noteReplacementBuffering(isPending)
        }

        defer {
          markTruncateSwapPending(false)
        }

        func armReplacementForForcedBootstrapIfNeeded() async {
          guard pendingTruncateSwap.value != true else { return }
          logger.log(
            .warning,
            message: "Electric replacement reset pre-armed before forced bootstrap (subscribe)",
            metadata: [
              "table": T.tableName,
              "collection": collectionIdentifier,
              "predicate": effectivePredicate?.rawValue ?? "<nil>",
            ]
          )
          await eventHandler.willReceiveTruncate(
            table: T.tableName,
            predicate: effectivePredicate
          )
          markTruncateSwapPending(true)
        }

        func maybeFallbackFromSSE() {
          guard transport == .sseWithFallback else { return }
          consecutiveSSEFailures += 1
          if consecutiveSSEFailures >= 3 {
            transport = .longPoll
          }
        }

        func recordSSESuccess() {
          consecutiveSSEFailures = 0
        }

        func shouldUseSSE() -> Bool {
          transport == .sse || transport == .sseWithFallback
        }

        @Sendable func applyBatchUnserialized(
          _ batch: SyncBatch<T>,
          stage: String,
          transport: ElectricLiveTransport,
          truncateAttemptCount: Int
        ) async throws -> AppliedBatchResult {
          guard isSessionCurrent() else {
            throw CancellationError()
          }

          let batchAttributes = mergeTraceAttributes(
            [
              "stage": stage,
              "table": T.tableName,
              "collection": collectionIdentifier,
              "sync.mode": electricSyncModeLabel(configuration.syncMode),
              "transport": electricTransportLabel(transport),
              "truncate_attempt_count": "\(truncateAttemptCount)",
              "thread.is_main": electricThreadIsMainValue(),
            ],
            electricMessageAttributes(batch.messages)
          )

          return try await withElectricAsyncSpan(
            tracer: tracer,
            name: "electric.batch.apply",
            attributes: batchAttributes
          ) { batchSpan in
            let batchStart = runtimeProvider.now()
            let chunks = batch.chunked(maxMessages: electricBatchApplyChunkSize)
            batchSpan.setAttribute(key: "chunk.count", value: "\(chunks.count)")
            var pendingHydrationKeys = Set<String>()
            let hasTruncateMessage = batch.messages.contains {
              $0.kind == .truncate || $0.control == .mustRefetch
            }
            if hasTruncateMessage, pendingTruncateSwap.value != true {
              logger.log(
                .warning,
                message: "Electric replacement reset armed from wire control (subscribe)",
                metadata: truncateSwapBatchMetadata(
                  batch: batch,
                  table: T.tableName,
                  predicate: effectivePredicate,
                  collectionIdentifier: collectionIdentifier
                )
              )
              await eventHandler.willReceiveTruncate(
                table: T.tableName,
                predicate: effectivePredicate
              )
              markTruncateSwapPending(true)
            }

            let shouldSwapThisBoundary =
              pendingTruncateSwap.value == true
              && truncateSwapPrepared.value != true
              && !hasTruncateMessage
            let atomicResult = try await backgroundTaskProvider.performProtectedWork(
              named: "ElectricSync.\(collectionIdentifier)"
            ) {
              try await applyChunksInSingleTransaction(
                chunks,
                runtimeProvider: runtimeProvider,
                transactionRunner: transactionRunner,
                shouldPrepareTruncateSwap: shouldSwapThisBoundary,
                validatePublication: {
                  guard isSessionCurrent() else { throw CancellationError() }
                },
                tracer: tracer,
                chunkSpanName: "electric.chunk.apply",
                chunkAttributes: { chunkIndex, chunkCount, chunk in
                  mergeTraceAttributes(
                    [
                      "stage": stage,
                      "table": T.tableName,
                      "collection": collectionIdentifier,
                      "sync.mode": electricSyncModeLabel(configuration.syncMode),
                      "transport": electricTransportLabel(transport),
                      "chunk.index": "\(chunkIndex + 1)",
                      "chunk.count": "\(chunkCount)",
                      "truncate_attempt_count": "\(truncateAttemptCount)",
                      "should_truncate_swap": "\(shouldSwapThisBoundary && chunkIndex == 0)",
                      "thread.is_main": electricThreadIsMainValue(),
                    ],
                    electricMessageAttributes(chunk.messages)
                  )
                },
                chunkWillApply: { chunkIndex, chunkCount, chunk in
                  if shouldSwapThisBoundary, chunkIndex == 0 {
                    logger.log(
                      .info,
                      message: "Electric truncate swap boundary started (subscribe)",
                      metadata: truncateSwapBatchMetadata(
                        batch: chunk,
                        table: T.tableName,
                        predicate: effectivePredicate,
                        collectionIdentifier: collectionIdentifier
                      )
                    )
                  }
                }
              )
            }

            for appliedChunk in atomicResult.chunks {
              let chunk = chunks[appliedChunk.index]
              let output = appliedChunk.output
              output.transactionDidCommit()
              output.emitCursorOwnershipCollisionReports()
              pendingHydrationKeys.formUnion(output.missingRowKeys)

              if appliedChunk.durationMs >= electricSlowApplyChunkThresholdMs {
                logger.log(
                  .warning,
                  message: "Electric sync chunk apply was slow (subscribe)",
                  metadata: withTimingMetadata(
                    truncateSwapBatchMetadata(
                      batch: chunk,
                      table: T.tableName,
                      predicate: effectivePredicate,
                      collectionIdentifier: collectionIdentifier
                    ).merging([
                      "chunkIndex": "\(appliedChunk.index + 1)",
                      "chunkCount": "\(chunks.count)",
                    ]) { _, new in new },
                    durationMs: appliedChunk.durationMs
                  )
                )
              }
            }

            if let preparation = atomicResult.truncateSwapPreparation {
              truncateSwapPrepared.value = true
              let appliedCount = atomicResult.chunks.first?.output.records.count ?? 0
              logger.log(
                .info,
                message: "Electric truncate swap prepared atomically (subscribe)",
                metadata: [
                  "table": T.tableName,
                  "collection": collectionIdentifier,
                  "predicate": effectivePredicate?.rawValue ?? "<nil>",
                  "records": "\(appliedCount)",
                  "unownedRowCount": "\(preparation.unownedRowCount)",
                  "deletedRowCount": "\(preparation.deletedRowCount)",
                  "usedTableTruncate": "\(preparation.usedTableTruncate)",
                  "chunkCount": "\(chunks.count)",
                ]
              )
            }

            if let truncateResult = atomicResult.chunks.first(where: {
              $0.output.encounteredTruncate
            }) {
              let chunk = chunks[truncateResult.index]
              let output = truncateResult.output
              // A synthetic reset (tracker-loss full bootstrap) carries no
              // truncate message, so arm the replacement swap from the apply
              // output. The replacement snapshot itself publishes atomically.
              if output.requiresReplacementSwap, pendingTruncateSwap.value != true {
                logger.log(
                  .warning,
                  message: "Electric replacement reset armed from apply result (subscribe)",
                  metadata: truncateSwapBatchMetadata(
                    batch: chunk,
                    table: T.tableName,
                    predicate: effectivePredicate,
                    collectionIdentifier: collectionIdentifier
                  )
                )
                await eventHandler.willReceiveTruncate(
                  table: T.tableName,
                  predicate: effectivePredicate
                )
                pendingTruncateSwap.value = true
              }
              await Task.yield()
              return AppliedBatchResult(
                encounteredTruncate: true,
                missingRowKeys: []
              )
            }

            let reachedReplacementBoundary = batch.messages.contains {
              $0.control == .upToDate || ($0.control == nil && $0.isUpToDate)
            }
            if pendingTruncateSwap.value == true,
              truncateSwapPrepared.value == true,
              reachedReplacementBoundary
            {
              markTruncateSwapPending(false)
              logger.log(
                .info,
                message: "Electric truncate swap completed at up-to-date boundary (subscribe)",
                metadata: [
                  "table": T.tableName,
                  "collection": collectionIdentifier,
                  "predicate": effectivePredicate?.rawValue ?? "<nil>",
                  "chunkCount": "\(chunks.count)",
                ]
              )
              await eventHandler.didReceiveTruncate(
                table: T.tableName,
                predicate: effectivePredicate
              )
            } else if pendingTruncateSwap.value == true, reachedReplacementBoundary {
              logger.log(
                .error,
                message:
                  "Electric replacement boundary reached before swap preparation (subscribe)",
                metadata: [
                  "table": T.tableName,
                  "collection": collectionIdentifier,
                  "predicate": effectivePredicate?.rawValue ?? "<nil>",
                  "chunkCount": "\(chunks.count)",
                  "truncateSwapPrepared": "\(truncateSwapPrepared.value == true)",
                ]
              )
            }
            if reachedReplacementBoundary {
              await eventHandler.didReceiveUpToDate(
                table: T.tableName,
                predicate: effectivePredicate
              )
            }

            let batchDurationMs = max(0, runtimeProvider.now().timeIntervalSince(batchStart) * 1000)
            batchSpan.setAttribute(
              key: "duration_ms",
              value: String(format: "%.2f", batchDurationMs)
            )
            if batchDurationMs >= electricSlowApplyBatchThresholdMs {
              logger.log(
                .warning,
                message: "Electric sync batch apply was slow (subscribe)",
                metadata: withTimingMetadata(
                  truncateSwapBatchMetadata(
                    batch: batch,
                    table: T.tableName,
                    predicate: effectivePredicate,
                    collectionIdentifier: collectionIdentifier
                  ),
                  durationMs: batchDurationMs
                )
              )
            }
            await Task.yield()
            return AppliedBatchResult(
              encounteredTruncate: false,
              missingRowKeys: Array(pendingHydrationKeys).sorted()
            )
          }
        }

        @Sendable func applyBatch(
          _ batch: SyncBatch<T>,
          stage: String,
          transport: ElectricLiveTransport,
          truncateAttemptCount: Int,
          isReplacementBootstrap: Bool = false
        ) async throws -> AppliedBatchResult {
          try batch.preflightSupportedEvents()
          let replacesSnapshotState =
            isReplacementBootstrap || batch.containsFullSnapshotBoundary
          let finishesProgressiveInitialBuffering = batch.messages.contains(where: {
            $0.control == .upToDate || ($0.control == nil && $0.isUpToDate)
          })
          return try await replica.withStreamPublication(
            finishesProgressiveInitialBuffering: finishesProgressiveInitialBuffering
          ) {
            let filteredBatch = await replica.filterLiveBatch(
              batch,
              isReplacementBootstrap: replacesSnapshotState
            )
            let result = try await applyBatchUnserialized(
              filteredBatch,
              stage: stage,
              transport: transport,
              truncateAttemptCount: truncateAttemptCount
            )
            if replacesSnapshotState, !result.encounteredTruncate {
              await replica.clearSnapshotTrackers()
            }
            return result
          }
        }

        func hydrateMissingRows(
          _ missingRowKeys: [String]
        ) async throws -> Bool {
          var pendingRowKeys = Array(Set(missingRowKeys)).sorted()
          var hydrationPassCount = 0
          let requiresHydration = !pendingRowKeys.isEmpty

          while !pendingRowKeys.isEmpty {
            guard isSessionCurrent() else {
              throw CancellationError()
            }

            hydrationPassCount += 1
            if hydrationPassCount > electricTransientHydrationMaxPasses {
              logger.log(
                .warning,
                message: "Electric transient hydration exceeded max passes",
                metadata: [
                  "table": T.tableName,
                  "collection": collectionIdentifier,
                  "predicate": effectivePredicate?.rawValue ?? "<nil>",
                  "passes": "\(hydrationPassCount - 1)",
                  "missingRowKeys": pendingRowKeys.joined(separator: ","),
                ]
              )
              throw ElectricSyncError.fetchFailed("Transient hydration exceeded max passes")
            }

            let descriptor =
              T.hydrationQueryDescriptor(forMissingRowKeys: pendingRowKeys)
              ?? QueryDescriptor(predicate: nil, orderBy: [], limit: nil)

            let hydrationResult = try await replica.withStreamPublication {
              let hydrationBatch = try await client.requestSnapshot(
                T.self,
                basePredicate: configuration.basePredicate,
                shapeTopology: configuration.shapeTopology,
                descriptor: descriptor,
                syncMode: streamSyncMode,
                consultFetchCoverage: false,
                recordAsObservation: true,
                replicaIdentity: replica.identity
              )
              guard let hydrationBatch else {
                return AppliedBatchResult(encounteredTruncate: false, missingRowKeys: [])
              }
              let result = try await applyBatchUnserialized(
                hydrationBatch,
                stage: "subscribe_hydration",
                transport: transport,
                truncateAttemptCount: truncateAttempts
              )
              if !result.encounteredTruncate {
                await replica.installSnapshotTracker(messages: hydrationBatch.messages)
              }
              return result
            }
            if hydrationResult.encounteredTruncate {
              throw ElectricSyncError.fetchFailed("Transient hydration encountered truncate")
            }

            pendingRowKeys = Array(Set(hydrationResult.missingRowKeys)).sorted()
            if !pendingRowKeys.isEmpty {
              await Task.yield()
            }
          }
          return requiresHydration
        }

        while !Task.isCancelled {
          guard isSessionCurrent() else {
            break
          }

          let isWorkingSetTailReady = await replica.isWorkingSetTailReady
          if usesConfiguredWorkingSetReset && (replica.isTrackerContinuityUnavailable || !isWorkingSetTailReady) {
            if let recoveryToken = await replica.startWorkingSetRecoveryIfNeeded() {
              do {
                // We already own the stream publication gate in this task;
                // seed through the same core without pausing this controller.
                _ = try await replica.withStreamPublication {
                  try await recoverActiveWorkingSet(
                    session: session,
                    ownerPublicationAlreadyHeld: true,
                    recoveryEpoch: recoveryToken.epoch,
                    lossGeneration: recoveryToken.lossGeneration
                  )
                }
              } catch {
                replica.failWorkingSetRecovery(epoch: recoveryToken.epoch)
                throw error
              }
              continue
            }
            // With no leases the actor refuses leadership. A cancellation of
            // this parked tail is observable, so activation can cancel/reseed
            // it without the historic readiness deadlock.
            try await replica.waitForWorkingSetTailReadiness()
            continue
          }

          if let delay = breaker.preflightDelay(now: runtimeProvider.now()) {
            try? await runtimeProvider.sleep(for: .seconds(delay))
            continue
          }

          do {
            if !forceProtocolFullBootstrapOnNextPoll,
              !mustFullBootstrapForTrackerContinuity,
              shouldUseSSE(),
              try await !client.requiresLegacyBootstrapAdmission(
                identity: replica.identity,
                syncMode: streamSyncMode
              )
            {
              let liveStreamController = ElectricLiveStreamController()
              if let stream = try await client.liveBatchStream(
                T.self,
                basePredicate: configuration.basePredicate,
                shapeTopology: configuration.shapeTopology,
                syncMode: streamSyncMode,
                replicaIdentity: replica.identity,
                streamController: liveStreamController
              ) {
                var reconnectAfterHydration = false

                for try await batch in stream {
                  if Task.isCancelled { break }
                  guard isSessionCurrent() else {
                    throw CancellationError()
                  }

                  await Task.yield()
                  let applyResult = try await applyBatch(
                    batch,
                    stage: "subscribe_sse",
                    transport: transport,
                    truncateAttemptCount: truncateAttempts
                  )
                  if applyResult.encounteredTruncate {
                    truncateAttempts += 1
                    if truncateAttempts <= 3 {
                      try? await runtimeProvider.sleep(for: .milliseconds(100))
                    } else {
                      let delay = breaker.recordFailure(
                        now: runtimeProvider.now(), reason: "truncate")
                      try? await runtimeProvider.sleep(for: .seconds(delay))
                    }
                    break
                  }

                  let didHydrate = try await hydrateMissingRows(applyResult.missingRowKeys)

                  truncateAttempts = 0
                  recordSSESuccess()
                  breaker.recordSuccess()
                  continuation.yield(())
                  if didHydrate {
                    reconnectAfterHydration = true
                    break
                  }
                }

                if reconnectAfterHydration {
                  // Join the transport opened from the pre-hydration cursor before the next
                  // loop reconnects immediately from the committed subsetEnd.
                  await liveStreamController.cancelAndWait()
                  continue
                }

                if Task.isCancelled {
                  break
                }

                // Stream ended; reconnect with circuit breaker delay.
                maybeFallbackFromSSE()
                let delay = breaker.recordFailure(now: runtimeProvider.now(), reason: "sse_end")
                try? await runtimeProvider.sleep(for: .seconds(delay))
                continue
              }
            }

            // Long-poll fallback (also used before we are up-to-date enough to enable SSE).
            let pollTruncateAttemptCount = truncateAttempts
            let forceFullBootstrap =
              forceProtocolFullBootstrapOnNextPoll || mustFullBootstrapForTrackerContinuity
            if forceFullBootstrap {
              await armReplacementForForcedBootstrapIfNeeded()
            }
            let pollResult = try await client.withLegacyBootstrapAdmission(
              identity: replica.identity,
              stage: "subscribe_poll",
              syncMode: streamSyncMode
            ) {
              let fetchStart = runtimeProvider.now()
              let batch: SyncBatch<T>? = try await client.pollStream(
                T.self,
                basePredicate: configuration.basePredicate,
                shapeTopology: configuration.shapeTopology,
                syncMode: streamSyncMode,
                live: true,
                forceFullBootstrap: forceFullBootstrap,
                replicaIdentity: replica.identity
              )
              let fetchDurationMs = max(
                0, runtimeProvider.now().timeIntervalSince(fetchStart) * 1000)

              // Some transports can finish normally after cooperative cancellation.
              // Never publish a returned batch after the suspension fence canceled
              // this owner.
              guard !Task.isCancelled else { throw CancellationError() }

              guard let batch else {
                return AppliedPollResult<T>(
                  batch: nil,
                  applyResult: nil,
                  fetchDurationMs: fetchDurationMs
                )
              }

              guard isSessionCurrent() else { throw CancellationError() }
              await Task.yield()
              let applyResult = try await applyBatch(
                batch,
                stage: "subscribe_poll",
                transport: .longPoll,
                truncateAttemptCount: pollTruncateAttemptCount,
                isReplacementBootstrap: forceFullBootstrap
              )
              return AppliedPollResult<T>(
                batch: batch,
                applyResult: applyResult,
                fetchDurationMs: fetchDurationMs
              )
            }

            if let batch = pollResult.batch, let applyResult = pollResult.applyResult {
              pollTransportWaitEventCount += 1
              let shouldLogPollTransportWait =
                pollTransportWaitEventCount == 1
                || pollResult.fetchDurationMs >= electricVerySlowPollTransportThresholdMs
                || pollTransportWaitEventCount % electricPollTransportInfoSampleEvery == 0
              if shouldLogPollTransportWait {
                logger.log(
                  .info,
                  message: "Electric poll transport wait",
                  metadata: withTimingMetadata(
                    truncateSwapBatchMetadata(
                      batch: batch,
                      table: T.tableName,
                      predicate: effectivePredicate,
                      collectionIdentifier: collectionIdentifier
                    ).merging([
                      "timing.classification": "transport_wait",
                      "timing.expected_for_long_poll": "true",
                      "transport.wait.ms": String(format: "%.2f", pollResult.fetchDurationMs),
                      "transport.wait.sampled": "true",
                      "transport.wait.sample_index": "\(pollTransportWaitEventCount)",
                      "stage": "transport_wait",
                    ]) { _, new in new },
                    durationMs: pollResult.fetchDurationMs
                  )
                )
              }
              if applyResult.encounteredTruncate {
                truncateAttempts += 1
                if truncateAttempts <= 3 {
                  try? await runtimeProvider.sleep(for: .milliseconds(100))
                } else {
                  let delay = breaker.recordFailure(now: runtimeProvider.now(), reason: "truncate")
                  try? await runtimeProvider.sleep(for: .seconds(delay))
                }
                continue
              }

              _ = try await hydrateMissingRows(applyResult.missingRowKeys)
              truncateAttempts = 0
              // A protocol quarantine recovery remains latched across fetch
              // or apply failure. Only the terminal full snapshot that has
              // successfully applied may re-enable ordinary poll/SSE resume.
              if forceFullBootstrap {
                forceProtocolFullBootstrapOnNextPoll = false
              }
              if mustFullBootstrapForTrackerContinuity {
                mustFullBootstrapForTrackerContinuity = false
                replica.markTrackerContinuityEstablished()
              }
              breaker.recordSuccess()
              continuation.yield(())
            } else {
              breaker.recordSuccess()
            }

            // Always loop; Electric live poll should keep returning up_to_date
            // headers every interval. Avoid hot loop via a tiny yield.
            await Task.yield()
          } catch is CancellationError {
            // Expected when SwiftUI cancels the request (e.g., navigating away)
            break
          } catch ElectricSyncError.trackerContinuityBootstrapRequired {
            // A fresh static-simple candidate was admitted at owner startup,
            // but its just-in-time durable revalidation changed or failed.
            // Do not retry the synthetic fresh cursor; force the authoritative
            // replacement path on the next poll.
            mustFullBootstrapForTrackerContinuity = true
            continue
          } catch {
            if !isSessionCurrent() {
              break
            }
            if let quarantine = client.protocolQuarantine(for: error) {
              if quarantine.compatibilityMayChangeAfterFullBootstrap,
                !hasUsedProtocolFullBootstrapRecovery
              {
                hasUsedProtocolFullBootstrapRecovery = true
                forceProtocolFullBootstrapOnNextPoll = true
                logger.log(
                  .warning,
                  message:
                    "Electric protocol quarantine scheduled bounded full-bootstrap recovery",
                  metadata: [
                    "auth.generation": session.map { "\($0.generation)" } ?? "<nil>",
                    "capability.gate": ElectricProtocolCapabilityPolicy.gateName,
                    "detail": quarantine.detail,
                    "reason": quarantine.reason.rawValue,
                    "recovery.attempt": "1",
                    "recovery.maximum_attempts": "1",
                    "table": T.tableName,
                    "transport": electricTransportLabel(transport),
                  ]
                )
                continue
              }
              logger.log(
                .error,
                message: "Electric protocol quarantined for collection \(collectionIdentifier)",
                metadata: [
                  "auth.generation": session.map { "\($0.generation)" } ?? "<nil>",
                  "capability.gate": ElectricProtocolCapabilityPolicy.gateName,
                  "compatibility.may_change_after_full_bootstrap":
                    "\(quarantine.compatibilityMayChangeAfterFullBootstrap)",
                  "detail": quarantine.detail,
                  "reason": quarantine.reason.rawValue,
                  "table": T.tableName,
                  "transport": electricTransportLabel(transport),
                ]
              )
              break
            }
            if let expectedIssue = electricExpectedTransportIssue(
              error: error, transport: transport)
            {
              expectedTransportIssueCount += 1
              let shouldLogExpectedIssue =
                expectedTransportIssueCount == 1
                || expectedTransportIssueCount % electricExpectedTransportIssueSampleEvery == 0
              if shouldLogExpectedIssue {
                var metadata: [String: String] = [
                  "table": T.tableName,
                  "collection": collectionIdentifier,
                  "predicate": effectivePredicate?.rawValue ?? "<nil>",
                  "transport": electricTransportLabel(transport),
                  "timing.classification": "transport_wait",
                  "timing.expected_for_long_poll": "\(expectedIssue == .longPollTimeout)",
                  "transport.wait.expected": "true",
                  "transport.wait.reason": expectedIssue.rawValue,
                  "transport.wait.sample_index": "\(expectedTransportIssueCount)",
                  "stage": "transport_wait",
                  "error": "\(error)",
                ]
                if let code = electricURLErrorCode(for: error) {
                  metadata["error.url_code"] = "\(code.rawValue)"
                  metadata["error.url_code_name"] = "\(code)"
                }
                logger.log(
                  expectedIssue.logLevel,
                  message: expectedIssue.message,
                  metadata: metadata
                )
              }
              breaker.recordSuccess()
              if Task.isCancelled {
                break
              }
              await Task.yield()
              continue
            }

            maybeFallbackFromSSE()

            logger.log(
              .error,
              message: "Electric sync error for collection \(collectionIdentifier)",
              metadata: [
                "error": "\(error)"
              ]
            )
            // Back off using circuit breaker to avoid hot-looping
            let delay = breaker.recordFailure(
              now: runtimeProvider.now(), reason: String(describing: error))
            try? await runtimeProvider.sleep(for: .seconds(delay))
          }
        }
      }

      continuation.onTermination = { _ in
        relay.cancel()
      }
      relay.install(producerTask: task)
    }
  }
}

actor ElectricCollectionBackgroundCoordinator<T: ElectricCollectionModel> {
  private enum DemandSemantics: Hashable, Sendable {
    case progressiveFetchSnapshot
    case ownerRequestSnapshot
    case ownerFinalizingSnapshot
  }

  private struct InflightQueryKey: Hashable, Sendable {
    let descriptor: QueryDescriptor
    let authSessionGeneration: Int?
    let demandSemantics: DemandSemantics
  }

  private struct InflightCommand: Sendable {
    let cancel: @Sendable () -> Void
    let wait: @Sendable () async -> Void
  }

  private let syncMode: ElectricCollectionSyncMode
  private let collectionIdentifier: String
  private let backgroundTaskProvider: any BackgroundTaskProvider
  private let runtimeProvider: ElectricSyncRuntimeProvider
  private let logger: any LogProvider
  private let tracer: any ElectricSyncTracer
  private var inflightQueries: [InflightQueryKey: Task<[T], Error>] = [:]
  private var inflightCommands: [UUID: InflightCommand] = [:]
  private var acceptsCommands = true
  private var loadingContinuations: [UUID: AsyncStream<CollectionLoadingEvent>.Continuation] = [:]
  private var loadingCount: Int = 0

  init(
    syncMode: ElectricCollectionSyncMode,
    collectionIdentifier: String,
    backgroundTaskProvider: any BackgroundTaskProvider,
    runtimeProvider: ElectricSyncRuntimeProvider,
    logger: any LogProvider,
    tracer: any ElectricSyncTracer
  ) {
    self.syncMode = syncMode
    self.collectionIdentifier = collectionIdentifier
    self.backgroundTaskProvider = backgroundTaskProvider
    self.runtimeProvider = runtimeProvider
    self.logger = logger
    self.tracer = tracer
  }

  @discardableResult
  private func withAsyncSpan<Output: Sendable>(
    name: String,
    attributes: [String: String] = [:],
    operation: (_ span: any ElectricSyncSpan) async throws -> Output
  ) async throws -> Output {
    try await withElectricAsyncSpan(
      tracer: tracer,
      name: name,
      attributes: attributes,
      operation: operation
    )
  }

  @discardableResult
  func performQuery(
    client: ElectricSyncClientImpl,
    basePredicate: SQLExpression?,
    shapeTopology: ElectricShapeTopology,
    trackerContinuityRecoveryPolicy: ElectricTrackerContinuityRecoveryPolicy = .fullBootstrap,
    descriptor: QueryDescriptor,
    demandSyncMode: ElectricCollectionSyncMode? = nil,
    session: ElectricSyncSession?,
    snapshotReplica: ElectricShapeReplica<T>? = nil,
    ownerSnapshotDemand: Bool = false,
    ownerPublicationAlreadyHeld: Bool = false,
    consultFetchCoverage: Bool = true,
    recordAsObservation: Bool = false,
    transactionRunner:
      @escaping @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws
      -> Void,
    eventHandler: any ElectricSyncEventHandler
  ) async throws -> [T] {
    guard acceptsCommands else {
      throw CancellationError()
    }
    let demandSemantics: DemandSemantics =
      if ownerPublicationAlreadyHeld {
        .ownerFinalizingSnapshot
      } else if ownerSnapshotDemand {
        .ownerRequestSnapshot
      } else {
        .progressiveFetchSnapshot
      }

    // Deduplicate in-flight fetches for progressive/on-demand queries.
    if let existing = matchingInflightTask(
      for: descriptor,
      authSessionGeneration: session?.generation,
      demandSemantics: demandSemantics
    ) {
      return try await existing.value
    }

    let startingFromZero = loadingCount == 0
    loadingCount += 1
    if startingFromZero {
      notifyListeners(.start)
    }

    let taskProvider = backgroundTaskProvider
    let task = Task { () throws -> [T] in
      let progressiveGeneration =
        demandSyncMode == .progressive && !ownerSnapshotDemand
        ? snapshotReplica?.progressiveSnapshotGeneration()
        : nil
      if demandSyncMode == .progressive, progressiveGeneration == nil, !ownerSnapshotDemand {
        return []
      }
      let isSessionCurrent: @Sendable () -> Bool = {
        guard let session else { return false }
        return client.sessionProvider.isCurrent(session)
      }
      let truncateAttempts = MutableValueBox(0)
      let pendingTruncateSwap = MutableValueBox(false)
      let truncateSwapPrepared = MutableValueBox(false)
      var refetchAfterTruncate = false
      var mustFullBootstrapForSemanticEpochTransition = false
      let initiallyRequiresTrackerContinuityRecovery: Bool
      if progressiveGeneration == nil, snapshotReplica?.isTrackerContinuityUnavailable == true {
        initiallyRequiresTrackerContinuityRecovery =
          try await client
          .requiresFullBootstrapForTrackerContinuity(
            T.self,
            identity: snapshotReplica?.identity
              ?? ElectricReplicaIdentity(
                modelType: T.self,
                modelIdentifier: T.collectionIdentifier,
                basePredicate: basePredicate
              ),
            syncMode: syncMode,
            shapeTopology: shapeTopology,
            recoveryPolicy: trackerContinuityRecoveryPolicy
          )
      } else {
        initiallyRequiresTrackerContinuityRecovery = false
      }
      var mustRecoverTrackerContinuity = initiallyRequiresTrackerContinuityRecovery
      var prefersDemandedSubsetResetForTrackerContinuity =
        if initiallyRequiresTrackerContinuityRecovery {
          await client.prefersDemandedSubsetResetForTrackerContinuity(
            T.self,
            identity: snapshotReplica?.identity
              ?? ElectricReplicaIdentity(
                modelType: T.self,
                modelIdentifier: T.collectionIdentifier,
                basePredicate: basePredicate
              ),
            syncMode: syncMode,
            shapeTopology: shapeTopology,
            recoveryPolicy: trackerContinuityRecoveryPolicy
          )
        } else {
          false
        }
      let markQueryTruncateSwapPending: @Sendable (Bool) -> Void = { isPending in
        guard pendingTruncateSwap.value != isPending else { return }
        pendingTruncateSwap.value = isPending
        truncateSwapPrepared.value = false
        snapshotReplica?.noteReplacementBuffering(isPending)
      }
      let ownerDemandNetworkRequestCount = MutableValueBox(0)

      defer {
        markQueryTruncateSwapPending(false)
      }

      let armQueryReplacementIfNeeded: @Sendable (String) async -> Void = { recovery in
        guard !pendingTruncateSwap.value else { return }
        self.logger.log(
          .warning,
          message: "Electric replacement reset pre-armed before recovery snapshot (query)",
          metadata: [
            "table": T.tableName,
            "collection": self.collectionIdentifier,
            "predicate": descriptor.predicate?.rawValue ?? "<nil>",
            "recovery": recovery,
          ]
        )
        await eventHandler.willReceiveTruncate(
          table: T.tableName,
          predicate: descriptor.predicate
        )
        markQueryTruncateSwapPending(true)
      }

      let executeQuery: () async throws -> [T] = {
        while true {
          guard isSessionCurrent() else {
            throw CancellationError()
          }

          if ownerSnapshotDemand {
            ownerDemandNetworkRequestCount.value += 1
          }
          let ownerDemandRequestCount =
            ownerSnapshotDemand
            ? "\(ownerDemandNetworkRequestCount.value)"
            : "<not_owner_demand>"
          let hasDemandedSubset =
            descriptor.predicate != nil
            || !descriptor.orderBy.isEmpty
            || descriptor.limit != nil
            || descriptor.cursor != nil
          let configuredWorkingSetReset = await client.usesConfiguredOnDemandWorkingSetReset(
            T.self,
            identity: snapshotReplica?.identity
              ?? ElectricReplicaIdentity(
                modelType: T.self,
                modelIdentifier: T.collectionIdentifier,
                basePredicate: basePredicate
              ),
            syncMode: self.syncMode,
            shapeTopology: shapeTopology,
            recoveryPolicy: trackerContinuityRecoveryPolicy
          )
          if mustRecoverTrackerContinuity,
            configuredWorkingSetReset,
            !ownerSnapshotDemand
          {
            throw ElectricSyncError.fetchFailed(
              "exclusive working-set recovery requires an active demand lease"
            )
          }
          let canUseDemandedSubsetReset =
            prefersDemandedSubsetResetForTrackerContinuity && hasDemandedSubset
            && (!configuredWorkingSetReset || ownerSnapshotDemand)
          if mustRecoverTrackerContinuity,
            configuredWorkingSetReset,
            !isBoundedWorkingSetDescriptor(descriptor)
          {
            throw ElectricSyncError.fetchFailed(
              "exclusive working-set recovery requires a bounded structured subset"
            )
          }
          let requiresDemandedSubsetReset =
            mustRecoverTrackerContinuity
            && canUseDemandedSubsetReset
            && !mustFullBootstrapForSemanticEpochTransition
          let requiresForcedFullBootstrap =
            (mustRecoverTrackerContinuity
              && !canUseDemandedSubsetReset)
            || mustFullBootstrapForSemanticEpochTransition
          if requiresForcedFullBootstrap {
            await armQueryReplacementIfNeeded("full_bootstrap")
          } else if requiresDemandedSubsetReset {
            await armQueryReplacementIfNeeded("demanded_subset")
          }
          let fetchStart = self.runtimeProvider.now()
          let fetchSpanAttributes: [String: String] = [
            "stage": "query_fetch",
            "table": T.tableName,
            "collection": self.collectionIdentifier,
            "sync.mode": electricSyncModeLabel(self.syncMode),
            "transport": "http_poll",
            "truncate_attempt_count": "\(truncateAttempts.value)",
            "owner_demand_network_request_count": ownerDemandRequestCount,
            "thread.is_main": electricThreadIsMainValue(),
          ]
          let batch: SyncBatch<T>?
          do {
            batch = try await self.withAsyncSpan(
              name: "electric.query.fetch",
              attributes: fetchSpanAttributes
            ) { span in
              let loadedBatch =
                if requiresForcedFullBootstrap {
                  try await client.pollStream(
                    T.self,
                    basePredicate: basePredicate,
                    shapeTopology: shapeTopology,
                    syncMode: self.syncMode,
                    live: false,
                    forceFullBootstrap: true,
                    replicaIdentity: snapshotReplica?.identity
                  )
                } else if requiresDemandedSubsetReset {
                  try await client.requestSnapshot(
                    T.self,
                    basePredicate: basePredicate,
                    shapeTopology: shapeTopology,
                    descriptor: descriptor,
                    syncMode: self.syncMode,
                    coverageSyncMode: demandSyncMode ?? self.syncMode,
                    restartOnDemandFromNow: true,
                    replacesExclusiveWorkingSet: configuredWorkingSetReset,
                    consultFetchCoverage: false,
                    recordAsObservation: recordAsObservation,
                    replicaIdentity: snapshotReplica?.identity
                  )
                } else if progressiveGeneration != nil {
                  try await client.fetchSnapshot(
                    T.self,
                    basePredicate: basePredicate,
                    shapeTopology: shapeTopology,
                    descriptor: descriptor,
                    syncMode: .progressive,
                    replicaIdentity: snapshotReplica?.identity
                  )
                } else {
                  try await client.requestSnapshot(
                    T.self,
                    basePredicate: basePredicate,
                    shapeTopology: shapeTopology,
                    descriptor: descriptor,
                    syncMode: self.syncMode,
                    coverageSyncMode: demandSyncMode ?? self.syncMode,
                    ignorePersistedSyncState: refetchAfterTruncate,
                    consultFetchCoverage: consultFetchCoverage,
                    recordAsObservation: recordAsObservation,
                    replicaIdentity: snapshotReplica?.identity
                  )
                }
              if let loadedBatch {
                try loadedBatch.preflightSupportedEvents()
                for (key, value) in electricMessageAttributes(loadedBatch.messages) {
                  span.setAttribute(key: key, value: value)
                }
                span.setAttribute(key: "result", value: "fetched")
              } else {
                span.setAttribute(key: "result", value: "cache_hit")
              }
              return loadedBatch
            }
          } catch ElectricSyncError.trackerContinuityBootstrapRequired {
            mustRecoverTrackerContinuity = true
            prefersDemandedSubsetResetForTrackerContinuity =
              await client.prefersDemandedSubsetResetForTrackerContinuity(
                T.self,
                identity: snapshotReplica?.identity
                  ?? ElectricReplicaIdentity(
                    modelType: T.self,
                    modelIdentifier: T.collectionIdentifier,
                    basePredicate: basePredicate
                  ),
                syncMode: self.syncMode,
                shapeTopology: shapeTopology,
                recoveryPolicy: trackerContinuityRecoveryPolicy
              )
            continue
          } catch ElectricSyncError.capabilitySemanticEpochTransitionDeferred
            where progressiveGeneration == nil
          {
            mustFullBootstrapForSemanticEpochTransition = true
            continue
          }
          let fetchDurationMs = max(
            0, self.runtimeProvider.now().timeIntervalSince(fetchStart) * 1000)

          guard let batch else { return [] }
          if let progressiveGeneration,
            snapshotReplica?.isProgressiveSnapshotGenerationCurrent(progressiveGeneration) != true
          {
            return []
          }
          try Task.checkCancellation()
          guard isSessionCurrent() else {
            throw CancellationError()
          }
          if fetchDurationMs >= electricSlowFetchThresholdMs {
            self.logger.log(
              .warning,
              message: "Electric subset fetch was slow",
              metadata: withTimingMetadata(
                truncateSwapBatchMetadata(
                  batch: batch,
                  table: T.tableName,
                  predicate: descriptor.predicate,
                  collectionIdentifier: self.collectionIdentifier
                ),
                durationMs: fetchDurationMs
              )
            )
          }

          let applyStart = self.runtimeProvider.now()
          let replacesSnapshotState =
            requiresForcedFullBootstrap
            || requiresDemandedSubsetReset
            || batch.containsFullSnapshotBoundary
          let chunks = batch.chunked(maxMessages: electricBatchApplyChunkSize)
          let didEncounterTruncate = MutableValueBox(false)
          let applySpanAttributes = mergeTraceAttributes(
            [
              "stage": "query_apply",
              "table": T.tableName,
              "collection": self.collectionIdentifier,
              "sync.mode": electricSyncModeLabel(self.syncMode),
              "chunk.count": "\(chunks.count)",
              "truncate_attempt_count": "\(truncateAttempts.value)",
              "owner_demand_network_request_count": ownerDemandRequestCount,
              "thread.is_main": electricThreadIsMainValue(),
            ],
            electricMessageAttributes(batch.messages)
          )

          let applyFetchedBatch: () async throws -> [T] = {
            if let progressiveGeneration,
              snapshotReplica?.isProgressiveSnapshotGenerationCurrent(progressiveGeneration)
                != true
            {
              return []
            }

            let hasTruncateMessage = batch.messages.contains {
              $0.kind == .truncate || $0.control == .mustRefetch
            }
            if hasTruncateMessage, !pendingTruncateSwap.value {
              self.logger.log(
                .warning,
                message: "Electric replacement reset armed from wire control (query)",
                metadata: truncateSwapBatchMetadata(
                  batch: batch,
                  table: T.tableName,
                  predicate: descriptor.predicate,
                  collectionIdentifier: self.collectionIdentifier
                )
              )
              await eventHandler.willReceiveTruncate(
                table: T.tableName,
                predicate: descriptor.predicate
              )
              markQueryTruncateSwapPending(true)
            }

            let shouldSwapThisBoundary =
              pendingTruncateSwap.value && !truncateSwapPrepared.value && !hasTruncateMessage
            let truncateAttemptCount = truncateAttempts.value
            let atomicResult = try await self.withAsyncSpan(
              name: "electric.query.apply",
              attributes: applySpanAttributes
            ) { applySpan in
              let result = try await taskProvider.performProtectedWork(
                named: "ElectricSync.\(self.collectionIdentifier)"
              ) {
                try await applyChunksInSingleTransaction(
                  chunks,
                  runtimeProvider: self.runtimeProvider,
                  transactionRunner: transactionRunner,
                  shouldPrepareTruncateSwap: shouldSwapThisBoundary,
                  validatePublication: {
                    try Task.checkCancellation()
                    guard isSessionCurrent() else { throw CancellationError() }
                    if let progressiveGeneration,
                      snapshotReplica?.isProgressiveSnapshotGenerationCurrent(
                        progressiveGeneration
                      ) != true
                    {
                      throw CancellationError()
                    }
                  },
                  tracer: self.tracer,
                  chunkSpanName: "electric.query.chunk_apply",
                  chunkAttributes: { chunkIndex, chunkCount, chunk in
                    mergeTraceAttributes(
                      [
                        "stage": "query_chunk_apply",
                        "table": T.tableName,
                        "collection": self.collectionIdentifier,
                        "sync.mode": electricSyncModeLabel(self.syncMode),
                        "chunk.index": "\(chunkIndex + 1)",
                        "chunk.count": "\(chunkCount)",
                        "should_truncate_swap":
                          "\(shouldSwapThisBoundary && chunkIndex == 0)",
                        "truncate_attempt_count": "\(truncateAttemptCount)",
                        "thread.is_main": electricThreadIsMainValue(),
                      ],
                      electricMessageAttributes(chunk.messages)
                    )
                  },
                  chunkWillApply: { chunkIndex, chunkCount, chunk in
                    if shouldSwapThisBoundary, chunkIndex == 0 {
                      self.logger.log(
                        .info,
                        message: "Electric truncate swap boundary started (query)",
                        metadata: truncateSwapBatchMetadata(
                          batch: chunk,
                          table: T.tableName,
                          predicate: descriptor.predicate,
                          collectionIdentifier: self.collectionIdentifier
                        )
                      )
                    }
                  }
                )
              }
              let applyDurationMs = max(
                0, self.runtimeProvider.now().timeIntervalSince(applyStart) * 1000)
              applySpan.setAttribute(
                key: "duration_ms", value: String(format: "%.2f", applyDurationMs)
              )
              return result
            }

            var appliedRecords: [T] = []
            for appliedChunk in atomicResult.chunks {
              let chunk = chunks[appliedChunk.index]
              let output = appliedChunk.output
              output.transactionDidCommit()
              output.emitCursorOwnershipCollisionReports()
              appliedRecords.append(contentsOf: output.subsetSnapshotRecords)

              if appliedChunk.durationMs >= electricSlowApplyChunkThresholdMs {
                self.logger.log(
                  .warning,
                  message: "Electric sync chunk apply was slow (query)",
                  metadata: withTimingMetadata(
                    truncateSwapBatchMetadata(
                      batch: chunk,
                      table: T.tableName,
                      predicate: descriptor.predicate,
                      collectionIdentifier: self.collectionIdentifier
                    ).merging([
                      "chunkIndex": "\(appliedChunk.index + 1)",
                      "chunkCount": "\(chunks.count)",
                    ]) { _, new in new },
                    durationMs: appliedChunk.durationMs
                  )
                )
              }
            }

            if let preparation = atomicResult.truncateSwapPreparation {
              truncateSwapPrepared.value = true
              let appliedCount = atomicResult.chunks.first?.output.records.count ?? 0
              self.logger.log(
                .info,
                message: "Electric truncate swap prepared atomically (query)",
                metadata: [
                  "table": T.tableName,
                  "collection": self.collectionIdentifier,
                  "predicate": descriptor.predicate?.rawValue ?? "<nil>",
                  "records": "\(appliedCount)",
                  "unownedRowCount": "\(preparation.unownedRowCount)",
                  "deletedRowCount": "\(preparation.deletedRowCount)",
                  "usedTableTruncate": "\(preparation.usedTableTruncate)",
                  "chunkCount": "\(chunks.count)",
                ]
              )
            }

            if let truncateResult = atomicResult.chunks.first(where: {
              $0.output.encounteredTruncate
            }) {
              let chunk = chunks[truncateResult.index]
              let output = truncateResult.output
              if output.requiresReplacementSwap, !pendingTruncateSwap.value {
                self.logger.log(
                  .warning,
                  message: "Electric replacement reset armed from apply result (query)",
                  metadata: truncateSwapBatchMetadata(
                    batch: chunk,
                    table: T.tableName,
                    predicate: descriptor.predicate,
                    collectionIdentifier: self.collectionIdentifier
                  ).merging([
                    "replacement_origin": "apply_discovered_defensive",
                    "owner_demand_network_request_count": ownerDemandRequestCount,
                  ]) { _, new in new }
                )
                await eventHandler.willReceiveTruncate(
                  table: T.tableName,
                  predicate: descriptor.predicate
                )
                pendingTruncateSwap.value = true
              }
              didEncounterTruncate.value = true
            }

            let reachedReplacementBoundary =
              batch.messages.contains {
                $0.control == .subsetEnd
              }
              || (replacesSnapshotState
                && batch.messages.contains {
                  $0.control == .upToDate || ($0.control == nil && $0.isUpToDate)
                })
            if pendingTruncateSwap.value,
              truncateSwapPrepared.value,
              reachedReplacementBoundary
            {
              markQueryTruncateSwapPending(false)
              self.logger.log(
                .info,
                message: "Electric truncate swap completed at terminal boundary (query)",
                metadata: [
                  "table": T.tableName,
                  "collection": self.collectionIdentifier,
                  "predicate": descriptor.predicate?.rawValue ?? "<nil>",
                  "chunkCount": "\(chunks.count)",
                ]
              )
              await eventHandler.didReceiveTruncate(
                table: T.tableName,
                predicate: descriptor.predicate
              )
            } else if pendingTruncateSwap.value, reachedReplacementBoundary {
              self.logger.log(
                .error,
                message: "Electric replacement boundary reached before swap preparation (query)",
                metadata: [
                  "table": T.tableName,
                  "collection": self.collectionIdentifier,
                  "predicate": descriptor.predicate?.rawValue ?? "<nil>",
                  "chunkCount": "\(chunks.count)",
                  "truncateSwapPrepared": "\(truncateSwapPrepared.value)",
                ]
              )
            }

            let applyDurationMs = max(
              0, self.runtimeProvider.now().timeIntervalSince(applyStart) * 1000)
            if applyDurationMs >= electricSlowApplyBatchThresholdMs {
              self.logger.log(
                .warning,
                message: "Electric sync batch apply was slow (query)",
                metadata: withTimingMetadata(
                  truncateSwapBatchMetadata(
                    batch: batch,
                    table: T.tableName,
                    predicate: descriptor.predicate,
                    collectionIdentifier: self.collectionIdentifier
                  ),
                  durationMs: applyDurationMs
                )
              )
            }
            await Task.yield()
            return appliedRecords
          }

          let applyFetchedBatchAndUpdateSnapshotTracking: () async throws -> [T] = {
            let records = try await applyFetchedBatch()
            guard !didEncounterTruncate.value, let snapshotReplica else { return records }
            if requiresDemandedSubsetReset {
              await snapshotReplica.clearSnapshotTrackers()
              await snapshotReplica.installSnapshotTracker(messages: batch.messages)
            } else if replacesSnapshotState {
              await snapshotReplica.clearSnapshotTrackers()
            } else {
              await snapshotReplica.installSnapshotTracker(messages: batch.messages)
            }
            return records
          }

          let appliedRecords: [T]
          if let progressiveGeneration, let snapshotReplica {
            try await snapshotReplica.beginAcceptedStreamPublication()
            do {
              if snapshotReplica.isProgressiveSnapshotGenerationCurrent(
                progressiveGeneration
              ) {
                appliedRecords = try await applyFetchedBatchAndUpdateSnapshotTracking()
              } else {
                appliedRecords = []
              }
              await snapshotReplica.endStreamPublication()
            } catch {
              await snapshotReplica.endStreamPublication()
              throw error
            }
          } else {
            appliedRecords = try await applyFetchedBatchAndUpdateSnapshotTracking()
          }

          if didEncounterTruncate.value {
            truncateAttempts.value += 1
            guard truncateAttempts.value <= 3 else {
              throw ElectricSyncError.fetchFailed(
                "Owner snapshot reset recovery exceeded 3 retries"
              )
            }
            refetchAfterTruncate = true
            continue
          }

          if mustRecoverTrackerContinuity || mustFullBootstrapForSemanticEpochTransition {
            mustRecoverTrackerContinuity = false
            prefersDemandedSubsetResetForTrackerContinuity = false
            mustFullBootstrapForSemanticEpochTransition = false
            refetchAfterTruncate = false
            snapshotReplica?.markTrackerContinuityEstablished()
            if requiresDemandedSubsetReset {
              return appliedRecords
            }
            continue
          }

          return appliedRecords
        }
      }

      if progressiveGeneration != nil {
        return try await executeQuery()
      } else if let snapshotReplica, !ownerPublicationAlreadyHeld {
        try await snapshotReplica.beginAcceptedSnapshotPublication()
        do {
          let output = try await executeQuery()
          await snapshotReplica.endSnapshotPublication()
          return output
        } catch {
          await snapshotReplica.endSnapshotPublication()
          throw error
        }
      } else {
        return try await executeQuery()
      }
    }

    let inflightKey = InflightQueryKey(
      descriptor: descriptor,
      authSessionGeneration: session?.generation,
      demandSemantics: demandSemantics
    )
    inflightQueries[inflightKey] = task
    defer {
      inflightQueries.removeValue(forKey: inflightKey)
      loadingCount -= 1
      if loadingCount == 0 {
        notifyListeners(.end)
      }
    }
    return try await task.value
  }

  func cancelAllAndWait() async {
    acceptsCommands = false
    let queryTasks = Array(inflightQueries.values)
    let commands = Array(inflightCommands.values)
    for task in queryTasks {
      task.cancel()
    }
    for command in commands {
      command.cancel()
    }
    for task in queryTasks {
      _ = try? await task.value
    }
    for command in commands {
      await command.wait()
    }
  }

  func performCommand<Output: Sendable>(
    operation: @escaping @Sendable () async throws -> Output
  ) async throws -> Output {
    guard acceptsCommands else {
      throw CancellationError()
    }

    let id = runtimeProvider.makeUUID()
    let task = Task {
      try await operation()
    }
    inflightCommands[id] = InflightCommand(
      cancel: { task.cancel() },
      wait: { _ = try? await task.value }
    )
    defer {
      inflightCommands.removeValue(forKey: id)
    }
    return try await withTaskCancellationHandler {
      try await task.value
    } onCancel: {
      task.cancel()
    }
  }

  func loadingEventsStream() -> AsyncStream<CollectionLoadingEvent> {
    AsyncStream { continuation in
      let id = runtimeProvider.makeUUID()
      loadingContinuations[id] = continuation
      continuation.onTermination = { [weak self] _ in
        Task { await self?.removeContinuation(id: id) }
      }
      if loadingCount > 0 {
        continuation.yield(CollectionLoadingEvent(transition: .start))
      }
    }
  }

  private func notifyListeners(_ transition: CollectionLoadingTransition) {
    for continuation in loadingContinuations.values {
      continuation.yield(CollectionLoadingEvent(transition: transition))
    }
  }

  private func removeContinuation(id: UUID) {
    loadingContinuations.removeValue(forKey: id)
  }

  private func matchingInflightTask(
    for descriptor: QueryDescriptor,
    authSessionGeneration: Int?,
    demandSemantics: DemandSemantics
  ) -> Task<[T], Error>? {
    let exactKey = InflightQueryKey(
      descriptor: descriptor,
      authSessionGeneration: authSessionGeneration,
      demandSemantics: demandSemantics
    )
    if let exact = inflightQueries[exactKey] {
      return exact
    }

    for (key, task) in inflightQueries {
      guard
        key.authSessionGeneration == authSessionGeneration,
        key.demandSemantics == demandSemantics
      else { continue }

      // Cursor-based pagination requests must only dedupe exact matches; subset/superset
      // reasoning doesn't apply across different cursor boundaries.
      if key.descriptor.cursor != nil || descriptor.cursor != nil {
        continue
      }
      guard
        key.descriptor.limit == descriptor.limit,
        key.descriptor.orderBy == descriptor.orderBy
      else { continue }
      if descriptor.limit != nil {
        if key.descriptor.predicate == descriptor.predicate {
          return task
        }
      } else {
        if PredicateLogic.isSubset(
          subset: descriptor.predicate?.predicate,
          superset: key.descriptor.predicate?.predicate
        ) {
          return task
        }
      }
    }
    return nil
  }
}
