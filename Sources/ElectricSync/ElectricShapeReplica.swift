import Foundation

public struct ElectricReplicaIdentity: Hashable, Sendable {
  public let serializationVersion: String
  public let modelIdentifier: String
  public let modelTypeName: String
  public let wireIdentity: ElectricShapeWireIdentity
  public let tableName: String
  public let basePredicateHash: PredicateHash
  public let basePredicateIdentity: String
  public let replicaMode: ElectricReplicaMode
  public let shapeDefinitionVersion: String
  public let serializedShapeIdentity: String
  let provenLegacyPersistedCursorKeys: [String]

  public init<Model: ElectricCollectionModel>(
    modelType: Model.Type,
    modelIdentifier: String,
    basePredicate: SQLExpression?,
    wireIdentity: ElectricShapeWireIdentity? = nil,
    replicaMode: ElectricReplicaMode = .default,
    shapeDefinitionVersion: String? = nil
  ) {
    let resolvedWireIdentity = wireIdentity ?? Model.electricShapeWireIdentity
    let resolvedShapeDefinitionVersion =
      shapeDefinitionVersion ?? Model.electricShapeDefinitionVersion
    let request = Model.createIdentifiedShapeRequest(
      where: basePredicate,
      orderBy: [],
      limit: nil,
      offset: nil,
      handle: nil,
      cursor: nil,
      live: false
    )

    let serializationVersion = "2"
    let basePredicateIdentity = request.predicate?.normalized() ?? "all"
    let basePredicateHash = PredicateHash(from: request.predicate)
    let orderIdentity = request.orderBy.map { "\($0.field):\($0.direction.rawValue)" }.joined(
      separator: ",")
    let subsetIdentityFields = request.subset.map {
      [
        $0.whereClause,
        $0.paramsJSON ?? "nil",
        $0.orderByClause ?? "nil",
        $0.limit.map(String.init) ?? "nil",
        $0.offset.map(String.init) ?? "nil",
      ]
    }
    let subsetIdentity = subsetIdentityFields?.map(Self.lengthPrefixed).joined() ?? "nil"
    // The v2 field layout is frozen: default replica mode and shape definition
    // version serialize byte-identically to the pre-mode identity so persisted
    // v2 cursor state stays resumable. Any non-default value appends explicit
    // discriminator fields, isolating state and forcing a full bootstrap.
    var serializedShapeIdentityFields = [
      serializationVersion,
      modelIdentifier,
      String(reflecting: modelType),
      resolvedWireIdentity.endpoint,
      resolvedWireIdentity.selectedColumns.map(Self.lengthPrefixed).joined(),
      resolvedWireIdentity.options.sorted { lhs, rhs in
        if lhs.key != rhs.key { return lhs.key < rhs.key }
        return lhs.value < rhs.value
      }.map { Self.lengthPrefixed($0.key) + Self.lengthPrefixed($0.value) }.joined(),
      request.table,
      basePredicateIdentity,
      orderIdentity,
      request.limit.map(String.init) ?? "nil",
      request.log?.rawValue ?? "nil",
      subsetIdentity,
    ]
    let hasDefaultReplicaDiscriminators =
      replicaMode == .default && resolvedShapeDefinitionVersion == "1"
    if !hasDefaultReplicaDiscriminators {
      serializedShapeIdentityFields.append("replica:\(replicaMode.rawValue)")
      serializedShapeIdentityFields.append("shape-definition:\(resolvedShapeDefinitionVersion)")
    }
    let serializedShapeIdentity = serializedShapeIdentityFields.map(Self.lengthPrefixed).joined()
    let hasProvenV1LegacyMapping =
      hasDefaultReplicaDiscriminators
      && modelIdentifier == Model.collectionIdentifier
      && Model.electricShapeWireIdentity.legacyCursorVersion == "1"
      && resolvedWireIdentity == Model.electricShapeWireIdentity
      && request.wireIdentity == Model.electricShapeWireIdentity
      && request.table == Model.tableName
      && basePredicateIdentity == (basePredicate?.normalized() ?? "all")
      && request.orderBy.isEmpty
      && request.limit == nil
      && request.offset == nil
      && request.handle == nil
      && request.cursor == nil
      && !request.live
      && request.log == nil
      && request.subset == nil
    let legacyPersistedCursorKeys =
      ["\(request.table)|stream|base:\(basePredicateHash.value)"]
      + ["eager", "onDemand", "progressive"].map {
        "\(request.table)|stream|mode:\($0)|base:\(basePredicateHash.value)"
      }

    self.serializationVersion = serializationVersion
    self.modelIdentifier = modelIdentifier
    self.modelTypeName = String(reflecting: modelType)
    self.wireIdentity = resolvedWireIdentity
    self.tableName = request.table
    self.basePredicateHash = basePredicateHash
    self.basePredicateIdentity = basePredicateIdentity
    self.replicaMode = replicaMode
    self.shapeDefinitionVersion = resolvedShapeDefinitionVersion
    self.serializedShapeIdentity = serializedShapeIdentity
    self.provenLegacyPersistedCursorKeys =
      hasProvenV1LegacyMapping ? legacyPersistedCursorKeys : []
  }

  public var persistedCursorKey: String {
    "electric-shape-stream|\(serializedShapeIdentity)"
  }

  public func legacyPersistedCursorKey(
    syncMode: ElectricCollectionSyncMode
  ) -> String {
    let modeKey =
      switch syncMode {
      case .eager: "eager"
      case .onDemand: "onDemand"
      case .progressive: "progressive"
      }
    return "\(tableName)|stream|mode:\(modeKey)|base:\(basePredicateHash.value)"
  }

  public static func == (lhs: Self, rhs: Self) -> Bool {
    lhs.serializedShapeIdentity == rhs.serializedShapeIdentity
  }

  public func hash(into hasher: inout Hasher) {
    hasher.combine(serializedShapeIdentity)
  }

  /// Cursor keys written before exact wire identity was part of persistence.
  /// Keys outside `provenLegacyPersistedCursorKeys` are detection-only and
  /// force staged bootstrap admission.
  public var legacyPersistedCursorKeys: [String] {
    let predicateIdentities =
      basePredicateHash == .all
      ? [PredicateHash.all.value, "none"]
      : [basePredicateHash.value]

    return predicateIdentities.flatMap { predicateIdentity in
      let prefix = "\(tableName)|stream"
      let suffix = "base:\(predicateIdentity)"
      return [
        "\(prefix)|\(suffix)",
        "\(prefix)|mode:eager|\(suffix)",
        "\(prefix)|mode:onDemand|\(suffix)",
        "\(prefix)|mode:progressive|\(suffix)",
      ]
    }
  }

  private static func lengthPrefixed(_ value: String) -> String {
    "\(value.utf8.count):\(value)"
  }
}

public typealias ElectricTransactionRunner =
  @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void

/// The explicit lifecycle of one replica's runtime owner.
///
/// - `dormant`: no runtime owner; committed rows and resume state are retained.
/// - `active`: one runtime owner holds the request loop and publication path.
/// - `idleGrace`: final demand released; the owner survives until GC expiry.
/// - `replacing`: a stream reset is buffering a fresh generation for atomic swap.
/// - `suspended`: the lifecycle fence rejected new work (teardown/suspension).
public enum ElectricReplicaOwnerState: String, Sendable {
  case dormant
  case active
  case idleGrace
  case replacing
  case suspended
}

/// The single session-scoped owner for one immutable Electric base shape.
///
/// Collection values retain demand policy, while this owner retains the client,
/// live request loop, cursor identity, snapshot tracker, and publication gate.
public final class ElectricShapeReplica<Model: ElectricCollectionModel>: @unchecked Sendable {
  public let identity: ElectricReplicaIdentity

  let basePredicate: SQLExpression?
  let client: ElectricSyncClientImpl
  let coordinator: ElectricCollectionBackgroundCoordinator<Model>
  let cacheProvider: any DataCacheProvider
  let transactionRunner: ElectricTransactionRunner
  let eventHandler: any ElectricSyncEventHandler
  let backgroundTaskProvider: any BackgroundTaskProvider
  let logger: any LogProvider
  let tracer: any ElectricSyncTracer

  private let publicationGate = ElectricReplicaPublicationGate()
  private let queryGate = ElectricReplicaPublicationGate()
  private let snapshotTracker: ElectricReplicaSnapshotTracker
  private let streamController: ElectricReplicaStreamController
  private let workGate = ElectricReplicaWorkGate()
  private let replacementBufferingCount = ElectricReplicaAtomicCounter()
  private let trackerContinuity = ElectricReplicaTrackerContinuity()
  private let workingSetTailReadiness = ElectricReplicaWorkingSetTailReadiness()
  private let activeDemandLeases = ElectricReplicaDemandLeases()
  private let progressiveInitialBuffer: ElectricProgressiveInitialBuffer

  public init(
    identity: ElectricReplicaIdentity,
    basePredicate: SQLExpression?,
    syncMode: ElectricCollectionSyncMode,
    client: ElectricSyncClientImpl,
    cacheProvider: any DataCacheProvider,
    transactionRunner: @escaping ElectricTransactionRunner,
    eventHandler: any ElectricSyncEventHandler = NoopElectricSyncEventHandler(),
    backgroundTaskProvider: any BackgroundTaskProvider = NoopBackgroundTaskProvider(),
    logger: any LogProvider = NoopLogProvider(),
    tracer: (any ElectricSyncTracer)? = nil,
    gcTime: TimeInterval = ElectricCollectionStreamManager.defaultGCTime
  ) {
    let resolvedTracer = tracer ?? NoopElectricSyncTracer()
    self.identity = identity
    self.basePredicate = basePredicate
    self.client = client
    self.cacheProvider = cacheProvider
    self.transactionRunner = transactionRunner
    self.eventHandler = eventHandler
    self.backgroundTaskProvider = backgroundTaskProvider
    self.logger = logger
    self.tracer = resolvedTracer
    self.snapshotTracker = ElectricReplicaSnapshotTracker(runtimeProvider: client.runtimeProvider)
    self.progressiveInitialBuffer = ElectricProgressiveInitialBuffer(
      isEnabled: syncMode == .progressive
    )
    self.coordinator = ElectricCollectionBackgroundCoordinator(
      syncMode: syncMode,
      collectionIdentifier: identity.modelIdentifier,
      backgroundTaskProvider: backgroundTaskProvider,
      runtimeProvider: client.runtimeProvider,
      logger: logger,
      tracer: resolvedTracer
    )
    self.streamController = ElectricReplicaStreamController(
      identity: identity,
      clientId: ObjectIdentifier(client),
      logger: logger,
      tracer: resolvedTracer,
      gcTime: gcTime,
      runtimeProvider: client.runtimeProvider,
      diagnostics: client.cursorOwnershipDiagnostics
    )
    let trackerContinuity = self.trackerContinuity
    let workingSetTailReadiness = self.workingSetTailReadiness
    let progressiveInitialBuffer = self.progressiveInitialBuffer
    self.streamController.onRuntimeOwnerEviction = {
      trackerContinuity.markLost()
      workingSetTailReadiness.failRecovery()
      progressiveInitialBuffer.restart()
    }
  }

  /// The explicit lifecycle state of this replica's runtime owner.
  public var ownerState: ElectricReplicaOwnerState {
    guard workGate.isAcceptingWork else { return .suspended }
    if replacementBufferingCount.value > 0 { return .replacing }
    let phase = streamController.runtimePhase
    if phase.hasPendingIdleGC { return .idleGrace }
    if phase.hasRuntimeOwner { return .active }
    return .dormant
  }

  /// True when this owner cannot prove process-local membership-tracker
  /// continuity for an incremental resume. Fresh processes, runtime-owner
  /// eviction, suspension, and cancellation all invalidate continuity; owners
  /// whose model requires the tracker must then replace their stale generation
  /// before going live again. Statically-simple on-demand owners can establish
  /// the replacement from their next demanded subset; other owners bootstrap
  /// the full shape.
  public var isTrackerContinuityUnavailable: Bool {
    !trackerContinuity.isEstablished
  }

  func markTrackerContinuityEstablished() {
    trackerContinuity.markEstablished()
  }

  func beginWorkingSetRecovery() {
    workingSetTailReadiness.beginRecovery()
  }

  func completeWorkingSetRecovery() {
    workingSetTailReadiness.completeRecovery()
  }

  func failWorkingSetRecovery() {
    trackerContinuity.markLost()
    workingSetTailReadiness.failRecovery()
  }

  var isWorkingSetTailReady: Bool { workingSetTailReadiness.isReady }

  func waitForWorkingSetTailReadiness() async throws {
    try workGate.checkAcceptingWork()
    try Task.checkCancellation()
    await workingSetTailReadiness.waitUntilReady()
    try workGate.checkAcceptingWork()
    try Task.checkCancellation()
  }

  func registerActiveDemand(_ descriptor: QueryDescriptor) -> UUID {
    activeDemandLeases.register(descriptor)
  }

  func releaseActiveDemand(_ id: UUID) {
    activeDemandLeases.release(id)
  }

  func activeDemandDescriptors(prioritizing first: QueryDescriptor? = nil) -> [QueryDescriptor] {
    activeDemandLeases.descriptors(prioritizing: first)
  }


  func noteReplacementBuffering(_ isBuffering: Bool) {
    if isBuffering {
      replacementBufferingCount.increment()
    } else {
      replacementBufferingCount.decrement()
    }
  }

  func progressiveSnapshotGeneration() -> Int? {
    progressiveInitialBuffer.capture()
  }

  func isProgressiveSnapshotGenerationCurrent(_ generation: Int) -> Bool {
    progressiveInitialBuffer.isCurrent(generation)
  }

  func finishProgressiveInitialBuffering() {
    progressiveInitialBuffer.finish()
  }

  func beginSnapshotPublication() async {
    await streamController.pause()
    await publicationGate.acquire()
  }

  func beginAcceptedSnapshotPublication() async throws {
    try workGate.checkAcceptingWork()
    await beginSnapshotPublication()
  }

  func endSnapshotPublication() async {
    await publicationGate.release()
    streamController.resume()
  }

  func beginStreamPublication() async {
    await publicationGate.acquire()
  }

  func beginAcceptedStreamPublication() async throws {
    try workGate.checkAcceptingWork()
    await beginStreamPublication()
  }

  func endStreamPublication() async {
    await publicationGate.release()
  }

  var publicationWaiterCount: Int {
    get async {
      await publicationGate.waiterCount
    }
  }

  func ensureSubset<Output: Sendable>(
    _ operation: () async throws -> Output
  ) async throws -> Output {
    try workGate.checkAcceptingWork()
    await beginSnapshotPublication()
    do {
      let output = try await operation()
      await endSnapshotPublication()
      return output
    } catch {
      await endSnapshotPublication()
      throw error
    }
  }

  func withQueryAdmission<Output: Sendable>(
    _ operation: () async throws -> Output
  ) async throws -> Output {
    try workGate.checkAcceptingWork()
    await queryGate.acquire()
    do {
      let output = try await operation()
      await queryGate.release()
      return output
    } catch {
      await queryGate.release()
      throw error
    }
  }

  func withStreamPublication<Output: Sendable>(
    finishesProgressiveInitialBuffering: Bool = false,
    _ operation: () async throws -> Output
  ) async throws -> Output {
    try workGate.checkAcceptingWork()
    await beginStreamPublication()
    do {
      let output = try await operation()
      if finishesProgressiveInitialBuffering {
        progressiveInitialBuffer.finish()
      }
      await endStreamPublication()
      return output
    } catch {
      await endStreamPublication()
      throw error
    }
  }

  func installSnapshotTracker(messages: [ElectricMessage]) async {
    await snapshotTracker.install(messages: messages)
  }

  func clearSnapshotTrackers() async {
    await snapshotTracker.removeAll()
  }

  func filterLiveBatch(
    _ batch: SyncBatch<Model>,
    isReplacementBootstrap: Bool = false
  ) async -> SyncBatch<Model> {
    let replacesSnapshotState =
      isReplacementBootstrap || batch.containsFullSnapshotBoundary
    if replacesSnapshotState {
      return batch
    }
    let messages = await snapshotTracker.filter(messages: batch.messages)
    guard messages.count != batch.messages.count else { return batch }
    return batch.filteringMessages(messages)
  }

  func acquireStream(
    syncMode: ElectricCollectionSyncMode,
    start: @escaping @Sendable () -> Task<Void, Never>
  ) -> ElectricCollectionStreamToken {
    guard
      let token = workGate.ifAcceptingWork({
        streamController.acquire(
          persistedCursorKey: client.persistedCursorKey(
            identity: identity,
            syncMode: syncMode
          ),
          start: start
        )
      })
    else {
      return ElectricCollectionStreamToken(onCancel: {})
    }
    return token
  }

  public func cancel() {
    workGate.close()
    trackerContinuity.markLost()
    workingSetTailReadiness.failRecovery()
    progressiveInitialBuffer.finish()
    streamController.cancelAll()
  }

  public func closeWorkGate() {
    workGate.close()
    trackerContinuity.markLost()
    workingSetTailReadiness.failRecovery()
    progressiveInitialBuffer.finish()
  }

  public func cancelAndWait() async {
    workGate.close()
    trackerContinuity.markLost()
    workingSetTailReadiness.failRecovery()
    progressiveInitialBuffer.finish()
    async let streamCancellation: Void = streamController.cancelAllAndWait()
    async let queryCancellation: Void = coordinator.cancelAllAndWait()
    await streamCancellation
    await queryCancellation
  }

  var liveOwnerCount: Int {
    streamController.liveOwnerCount
  }

  var isAcceptingWork: Bool {
    workGate.isAcceptingWork
  }
}

private final class ElectricReplicaWorkingSetTailReadiness: @unchecked Sendable {
  private let lock = NSLock()
  private var ready = false
  private var waiters: [CheckedContinuation<Void, Never>] = []

  var isReady: Bool { lock.withLock { ready } }

  func beginRecovery() { lock.withLock { ready = false } }
  func completeRecovery() {
    let waiters = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
      ready = true
      defer { self.waiters.removeAll() }
      return self.waiters
    }
    for waiter in waiters { waiter.resume() }
  }
  func failRecovery() {
    let waiters = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
      ready = false
      defer { self.waiters.removeAll() }
      return self.waiters
    }
    for waiter in waiters { waiter.resume() }
  }
  func waitUntilReady() async {
    guard !isReady else { return }
    await withCheckedContinuation { continuation in
      let resumeNow = lock.withLock {
        guard !ready else { return true }
        waiters.append(continuation)
        return false
      }
      if resumeNow { continuation.resume() }
    }
  }
}

private final class ElectricReplicaDemandLeases: @unchecked Sendable {
  private let lock = NSLock()
  private var values: [UUID: QueryDescriptor] = [:]
  func register(_ descriptor: QueryDescriptor) -> UUID {
    let id = UUID()
    lock.withLock { values[id] = descriptor }
    return id
  }
  func release(_ id: UUID) { _ = lock.withLock { values.removeValue(forKey: id) } }
  func descriptors(prioritizing first: QueryDescriptor?) -> [QueryDescriptor] {
    lock.withLock {
      let all = Set(values.values)
      guard let first else { return all.sorted { $0.rawSortKey < $1.rawSortKey } }
      return [first] + all.subtracting([first]).sorted { $0.rawSortKey < $1.rawSortKey }
    }
  }
}

private extension QueryDescriptor {
  var rawSortKey: String {
    let predicateValue = predicate?.rawValue ?? ""
    let limitValue = limit.map(String.init) ?? ""
    return predicateValue + "|" + String(describing: orderBy) + "|" + limitValue
  }
}

private final class ElectricProgressiveInitialBuffer: @unchecked Sendable {
  private let lock = NSLock()
  private let isEnabled: Bool
  private var isBuffering: Bool
  private var generation = 0

  init(isEnabled: Bool) {
    self.isEnabled = isEnabled
    self.isBuffering = isEnabled
  }

  func capture() -> Int? {
    lock.withLock { isBuffering ? generation : nil }
  }

  func isCurrent(_ candidate: Int) -> Bool {
    lock.withLock { isBuffering && generation == candidate }
  }

  func finish() {
    lock.withLock {
      isBuffering = false
      generation += 1
    }
  }

  func restart() {
    lock.withLock {
      guard isEnabled else { return }
      isBuffering = true
      generation += 1
    }
  }
}

private final class ElectricReplicaAtomicCounter: @unchecked Sendable {
  private let lock = NSLock()
  private var count = 0

  var value: Int {
    lock.withLock { count }
  }

  func increment() {
    lock.withLock { count += 1 }
  }

  func decrement() {
    lock.withLock { count = max(0, count - 1) }
  }
}

/// Process-local membership-tracker continuity for one replica owner.
///
/// Continuity starts unavailable (a fresh process has an empty tracker) and is
/// invalidated by runtime-owner eviction, suspension, and cancellation. Owners
/// that depend on the tracker must full-bootstrap before marking it established.
private final class ElectricReplicaTrackerContinuity: @unchecked Sendable {
  private let lock = NSLock()
  private var established = false

  var isEstablished: Bool {
    lock.withLock { established }
  }

  func markEstablished() {
    lock.withLock { established = true }
  }

  func markLost() {
    lock.withLock { established = false }
  }
}

private final class ElectricReplicaWorkGate: @unchecked Sendable {
  private let lock = NSLock()
  private var acceptsWork = true

  var isAcceptingWork: Bool {
    lock.withLock { acceptsWork }
  }

  func checkAcceptingWork() throws {
    guard isAcceptingWork else {
      throw CancellationError()
    }
  }

  func ifAcceptingWork<Output>(_ operation: () -> Output) -> Output? {
    lock.withLock {
      guard acceptsWork else { return nil }
      return operation()
    }
  }

  func close() {
    lock.withLock {
      acceptsWork = false
    }
  }
}

private actor ElectricReplicaPublicationGate {
  private var isAcquired = false
  private var waiters: [CheckedContinuation<Void, Never>] = []

  func acquire() async {
    guard isAcquired else {
      isAcquired = true
      return
    }

    await withCheckedContinuation { continuation in
      waiters.append(continuation)
    }
  }

  func release() {
    guard !waiters.isEmpty else {
      isAcquired = false
      return
    }

    waiters.removeFirst().resume()
  }

  var waiterCount: Int {
    waiters.count
  }
}

private actor ElectricReplicaSnapshotTracker {
  private struct Entry: Sendable {
    let boundary: PostgresSnapshot
    let keys: Set<String>
  }

  private var entries: [UUID: Entry] = [:]
  private let runtimeProvider: ElectricSyncRuntimeProvider

  init(runtimeProvider: ElectricSyncRuntimeProvider) {
    self.runtimeProvider = runtimeProvider
  }

  func install(messages: [ElectricMessage]) {
    guard let boundary = messages.reversed().compactMap(\.postgresSnapshot).first else {
      return
    }

    let keys = Set<String>(
      messages.compactMap { message in
        guard message.isSubsetSnapshot else { return nil }
        return message.key
      })
    let id = runtimeProvider.makeUUID()
    entries[id] = Entry(boundary: boundary, keys: keys)
  }

  func removeAll() {
    entries.removeAll()
  }

  func filter(messages: [ElectricMessage]) -> [ElectricMessage] {
    guard !entries.isEmpty else { return messages }

    var filtered: [ElectricMessage] = []
    filtered.reserveCapacity(messages.count)

    for message in messages {
      guard !message.isSubsetSnapshot,
        let key = message.key,
        let txid = message.txids?.max()
      else {
        filtered.append(message)
        continue
      }

      retireCompletedSnapshots(txid: txid)
      let isAlreadyVisible = entries.values.contains { entry in
        entry.keys.contains(key) && entry.boundary.isVisible(transactionId: txid)
      }
      if !isAlreadyVisible {
        filtered.append(message)
      }
    }

    return filtered
  }

  private func retireCompletedSnapshots(txid: Int64) {
    let completed = entries.compactMap { id, entry -> UUID? in
      guard let xmax = Int64(entry.boundary.xmax), txid >= xmax else { return nil }
      return id
    }
    for id in completed {
      entries.removeValue(forKey: id)
    }
  }

}

private final class ElectricReplicaStreamController: @unchecked Sendable {
  private struct State {
    var subscriberCount = 0
    var pauseCount = 0
    var task: Task<Void, Never>?
    var start: (@Sendable () -> Task<Void, Never>)?
    var gcTask: Task<Void, Never>?
    var ownerRegistration: ElectricCursorOwnerRegistration?
    var persistedCursorKey: String?
    var isEvicting = false
    var evictionGeneration = 0
    var evictionTask: Task<Void, Never>?
  }

  private let identity: ElectricReplicaIdentity
  private let clientId: ObjectIdentifier
  private let logger: any LogProvider
  private let tracer: any ElectricSyncTracer
  private let gcTime: TimeInterval
  private let runtimeProvider: ElectricSyncRuntimeProvider
  private let diagnostics: ElectricCursorOwnershipDiagnostics
  private let lock = NSLock()
  private var state = State()

  /// Invoked whenever the runtime owner (and its process-local tracker
  /// continuity) is torn down: idle-GC expiry, cancellation, or suspension.
  var onRuntimeOwnerEviction: (@Sendable () -> Void)?

  var runtimePhase: (hasRuntimeOwner: Bool, hasPendingIdleGC: Bool) {
    lock.withLock {
      (
        hasRuntimeOwner: state.ownerRegistration != nil || state.isEvicting,
        hasPendingIdleGC: state.gcTask != nil
      )
    }
  }

  init(
    identity: ElectricReplicaIdentity,
    clientId: ObjectIdentifier,
    logger: any LogProvider,
    tracer: any ElectricSyncTracer,
    gcTime: TimeInterval,
    runtimeProvider: ElectricSyncRuntimeProvider,
    diagnostics: ElectricCursorOwnershipDiagnostics = .shared
  ) {
    self.identity = identity
    self.clientId = clientId
    self.logger = logger
    self.tracer = tracer
    self.gcTime = gcTime
    self.runtimeProvider = runtimeProvider
    self.diagnostics = diagnostics
  }

  func acquire(
    persistedCursorKey: String,
    start: @escaping @Sendable () -> Task<Void, Never>
  ) -> ElectricCollectionStreamToken {
    let collisionReport: ElectricCursorOwnershipCollisionReport? = lock.withLock {
      state.subscriberCount += 1
      state.start = state.start ?? start
      state.persistedCursorKey = persistedCursorKey
      state.gcTask?.cancel()
      state.gcTask = nil

      guard !state.isEvicting else {
        return nil
      }

      guard state.ownerRegistration == nil else {
        startIfNeededLocked()
        return nil
      }

      let registration = diagnostics.registerOwner(
        persistedCursorKeys: [persistedCursorKey],
        clientId: clientId,
        table: identity.tableName,
        collectionIdentifier: identity.modelIdentifier,
        logger: logger,
        tracer: tracer,
        runtimeProvider: runtimeProvider
      )
      state.ownerRegistration = registration.registration
      startIfNeededLocked()
      return registration.collisionReport
    }
    collisionReport?.emit()

    return ElectricCollectionStreamToken { [weak self] in
      self?.release()
    }
  }

  func pause() async {
    let task: Task<Void, Never>? = lock.withLock {
      state.pauseCount += 1
      guard state.pauseCount == 1 else { return nil }
      let task = state.task
      state.task = nil
      return task
    }
    task?.cancel()
    await task?.value
  }

  func resume() {
    lock.withLock {
      guard state.pauseCount > 0 else { return }
      state.pauseCount -= 1
      startIfNeededLocked()
    }
  }

  func cancelAll() {
    let tasks = takeAllTasks()
    for task in tasks {
      task.cancel()
    }
  }

  func cancelAllAndWait() async {
    let tasks = takeAllTasks()
    for task in tasks {
      task.cancel()
    }
    for task in tasks {
      await task.value
    }
  }

  private func takeAllTasks() -> [Task<Void, Never>] {
    lock.withLock {
      let tasks = [state.task, state.gcTask, state.evictionTask].compactMap { $0 }
      state.ownerRegistration?.cancel()
      state = State()
      return tasks
    }
  }

  var liveOwnerCount: Int {
    lock.withLock { state.ownerRegistration == nil ? 0 : 1 }
  }

  private func release() {
    let shouldCancelImmediately: Bool = lock.withLock {
      guard state.subscriberCount > 0 else { return false }
      state.subscriberCount -= 1
      guard state.subscriberCount == 0 else { return false }
      guard gcTime > 0 else { return true }

      state.gcTask?.cancel()
      state.gcTask = Task { [weak self] in
        guard let self else { return }
        try? await runtimeProvider.sleep(for: .seconds(self.gcTime))
        guard !Task.isCancelled else { return }
        self.cancelIfUnused()
      }
      return false
    }

    if shouldCancelImmediately {
      cancelIfUnused()
    }
  }

  private func cancelIfUnused() {
    let eviction: (task: Task<Void, Never>?, generation: Int)? = lock.withLock {
      guard state.subscriberCount == 0, !state.isEvicting else { return nil }
      state.isEvicting = true
      state.evictionGeneration += 1
      let generation = state.evictionGeneration
      let task = state.task
      state.task = nil
      state.start = nil
      state.gcTask?.cancel()
      state.gcTask = nil
      return (task, generation)
    }
    guard let eviction else { return }
    eviction.task?.cancel()

    let evictionTask = Task { [weak self] in
      await eviction.task?.value
      self?.finishEviction(generation: eviction.generation)
    }
    let retained = lock.withLock {
      guard state.isEvicting, state.evictionGeneration == eviction.generation else {
        return false
      }
      state.evictionTask = evictionTask
      return true
    }
    if !retained {
      evictionTask.cancel()
    }
  }

  private func finishEviction(generation: Int) {
    let didEvictOwner = lock.withLock {
      guard state.isEvicting, state.evictionGeneration == generation else {
        return false
      }
      let didEvictOwner = state.ownerRegistration != nil
      state.ownerRegistration?.cancel()
      state.ownerRegistration = nil
      state.evictionTask = nil
      return didEvictOwner
    }
    if didEvictOwner {
      onRuntimeOwnerEviction?()
    }

    let collisionReport: ElectricCursorOwnershipCollisionReport? = lock.withLock {
      guard state.isEvicting, state.evictionGeneration == generation else {
        return nil
      }
      state.isEvicting = false
      guard state.subscriberCount > 0, let persistedCursorKey = state.persistedCursorKey else {
        return nil
      }

      let registration = diagnostics.registerOwner(
        persistedCursorKeys: [persistedCursorKey],
        clientId: clientId,
        table: identity.tableName,
        collectionIdentifier: identity.modelIdentifier,
        logger: logger,
        tracer: tracer,
        runtimeProvider: runtimeProvider
      )
      state.ownerRegistration = registration.registration
      startIfNeededLocked()
      return registration.collisionReport
    }
    collisionReport?.emit()
  }

  private func startIfNeededLocked() {
    guard state.subscriberCount > 0,
      state.pauseCount == 0,
      !state.isEvicting,
      state.task == nil,
      let start = state.start
    else { return }
    state.task = start()
  }
}
