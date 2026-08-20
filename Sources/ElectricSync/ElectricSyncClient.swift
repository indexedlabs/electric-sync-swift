import Foundation

let electricMoveOutTombstoneCleanupBatchSize = 200

private struct SubsetObservationEvidence: Sendable {
  let snapshotBoundary: PostgresSnapshot?
  let outcome: SubsetObservationOutcome

  init(messages: [ElectricMessage]) {
    self.snapshotBoundary = messages.reversed().compactMap(\.postgresSnapshot).first
    self.outcome =
      messages.contains { $0.isSubsetSnapshot && !$0.payload.isEmpty }
      ? .present
      : .absent
  }
}

/// Process-local fallback for a shape that was incorrectly declared simple.
///
/// Electric 1.7.7 sends tagged DNF protocol fields for any server WHERE clause
/// containing a SQL subquery. Once one such batch proves a declaration wrong,
/// preserve that fact across replacement bootstraps for the lifetime of this
/// client so the guard cannot bootstrap the same stream forever.
private final class ElectricShapeTopologyLatch: @unchecked Sendable {
  private let lock = NSLock()
  private var dnfStreams = Set<String>()

  func latchDNFSemantics(for streamStateKey: String) -> Bool {
    lock.withLock { dnfStreams.insert(streamStateKey).inserted }
  }

  func hasLatchedDNFSemantics(for streamStateKey: String) -> Bool {
    lock.withLock { dnfStreams.contains(streamStateKey) }
  }
}

public struct SyncBatch<T: ElectricCollectionModel>: Sendable {
  struct TruncateSwapPreparation: Sendable {
    let unownedRowCount: Int
    let deletedRowCount: Int
    let usedTableTruncate: Bool
  }

  public let collectionIdentifier: String
  public let streamStateKey: String
  public let rollbackStreamStateKeys: [String]
  private let invalidationStreamStateKeys: [String]
  public let basePredicateHash: PredicateHash
  public let fetchPredicate: SQLExpression?
  public let fetchDescriptor: QueryDescriptor?
  public let shouldRecordFetch: Bool
  public let shouldRecordObservation: Bool
  public let shouldPersistSyncState: Bool
  private let persistSyncStateOnlyAtTerminalBoundary: Bool
  private let shouldUseMoveOutTombstones: Bool
  private let shouldRemoveExpiredMoveOutTombstones: Bool
  public let messages: [ElectricMessage]
  public let metadataProvider: MetadataProvider
  public let moveOutTracker: MoveOutTagTracker
  private let eventHandler: any ElectricSyncEventHandler
  private let tracer: any ElectricSyncTracer
  private let logger: any LogProvider
  private let cursorOwnershipDiagnostics: ElectricCursorOwnershipDiagnostics
  private let cursorWriterClientId: ObjectIdentifier
  private let isRollbackDualWriteEnabled: @Sendable () -> Bool
  private let observationEvidence: SubsetObservationEvidence
  private let protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy
  private let protocolSemanticEpoch: ElectricProtocolSemanticEpoch
  private let runtimeProvider: ElectricSyncRuntimeProvider
  private let requiresSemanticEpochReset: Bool
  private let shapeTopology: ElectricShapeTopology
  private let shapeTopologyLatch: ElectricShapeTopologyLatch?
  private let protocolInputMessages: [ElectricMessage]

  fileprivate init(
    collectionIdentifier: String,
    streamStateKey: String,
    rollbackStreamStateKeys: [String],
    invalidationStreamStateKeys: [String]? = nil,
    basePredicateHash: PredicateHash,
    fetchPredicate: SQLExpression?,
    fetchDescriptor: QueryDescriptor?,
    shouldRecordFetch: Bool,
    shouldRecordObservation: Bool,
    shouldPersistSyncState: Bool,
    persistSyncStateOnlyAtTerminalBoundary: Bool = false,
    shouldUseMoveOutTombstones: Bool = true,
    shouldRemoveExpiredMoveOutTombstones: Bool = true,
    messages: [ElectricMessage],
    metadataProvider: MetadataProvider,
    moveOutTracker: MoveOutTagTracker,
    eventHandler: any ElectricSyncEventHandler,
    tracer: any ElectricSyncTracer,
    logger: any LogProvider,
    cursorOwnershipDiagnostics: ElectricCursorOwnershipDiagnostics,
    cursorWriterClientId: ObjectIdentifier,
    isRollbackDualWriteEnabled: @escaping @Sendable () -> Bool,
    observationEvidence: SubsetObservationEvidence? = nil,
    protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy,
    protocolSemanticEpoch: ElectricProtocolSemanticEpoch,
    runtimeProvider: ElectricSyncRuntimeProvider = .live,
    requiresSemanticEpochReset: Bool = false,
    shapeTopology: ElectricShapeTopology = .dnf,
    shapeTopologyLatch: ElectricShapeTopologyLatch? = nil,
    protocolInputMessages: [ElectricMessage]? = nil
  ) {
    self.collectionIdentifier = collectionIdentifier
    self.streamStateKey = streamStateKey
    self.rollbackStreamStateKeys = rollbackStreamStateKeys
    self.invalidationStreamStateKeys = invalidationStreamStateKeys ?? rollbackStreamStateKeys
    self.basePredicateHash = basePredicateHash
    self.fetchPredicate = fetchPredicate
    self.fetchDescriptor = fetchDescriptor
    self.shouldRecordFetch = shouldRecordFetch
    self.shouldRecordObservation = shouldRecordObservation
    let containsOwnerBoundary = messages.contains {
      $0.control == .subsetEnd || $0.control == .mustRefetch || $0.kind == .truncate
    }
    self.shouldPersistSyncState =
      shouldPersistSyncState
      && (!persistSyncStateOnlyAtTerminalBoundary || containsOwnerBoundary)
    self.persistSyncStateOnlyAtTerminalBoundary = persistSyncStateOnlyAtTerminalBoundary
    self.shouldUseMoveOutTombstones = shouldUseMoveOutTombstones
    self.shouldRemoveExpiredMoveOutTombstones = shouldRemoveExpiredMoveOutTombstones
    self.messages = messages
    self.metadataProvider = metadataProvider
    self.moveOutTracker = moveOutTracker
    self.eventHandler = eventHandler
    self.tracer = tracer
    self.logger = logger
    self.cursorOwnershipDiagnostics = cursorOwnershipDiagnostics
    self.cursorWriterClientId = cursorWriterClientId
    self.isRollbackDualWriteEnabled = isRollbackDualWriteEnabled
    self.observationEvidence = observationEvidence ?? SubsetObservationEvidence(messages: messages)
    self.protocolCapabilityPolicy = protocolCapabilityPolicy
    self.protocolSemanticEpoch = protocolSemanticEpoch
    self.runtimeProvider = runtimeProvider
    self.requiresSemanticEpochReset = requiresSemanticEpochReset
    self.shapeTopology = shapeTopology
    self.shapeTopologyLatch = shapeTopologyLatch
    self.protocolInputMessages = protocolInputMessages ?? messages
  }

  public struct Output: Sendable {
    public let records: [T]
    public let subsetSnapshotRecords: [T]
    public let encounteredTruncate: Bool
    public let missingRowKeys: [String]
    fileprivate let cursorOwnershipCollisionReports: [ElectricCursorOwnershipCollisionReport]
    /// True when this apply invalidated the replica's resume state and a
    /// snapshot must replace existing rows atomically via `prepareTruncateSwap`.
    /// The owner normally arms its pending swap from this flag; a known
    /// tracker-loss path may arm it before issuing its recovery snapshot, while
    /// this remains the defensive backstop for apply-discovered resets.
    public var requiresReplacementSwap = false
    fileprivate var onTransactionCommitted: @Sendable () -> Void = {}

    func emitCursorOwnershipCollisionReports() {
      for report in cursorOwnershipCollisionReports {
        report.emit()
      }
    }

    func transactionDidCommit() {
      onTransactionCommitted()
    }
  }

  var containsFullSnapshotBoundary: Bool {
    messages.contains { $0.control == .snapshotEnd && !$0.isSubsetSnapshot }
  }

  func chunked(maxMessages: Int) -> [SyncBatch<T>] {
    guard maxMessages > 0, messages.count > maxMessages else { return [self] }
    var chunks: [SyncBatch<T>] = []
    chunks.reserveCapacity((messages.count + maxMessages - 1) / maxMessages)
    var index = 0
    while index < messages.count {
      let end = min(index + maxMessages, messages.count)
      chunks.append(
        replacing(
          messages: Array(messages[index..<end]),
          shouldRecordEvidence: end == messages.count,
          shouldRemoveExpiredMoveOutTombstones: shouldRemoveExpiredMoveOutTombstones && index == 0
        )
      )
      index = end
    }
    return chunks
  }

  private func replacing(
    messages: [ElectricMessage],
    shouldRecordEvidence: Bool,
    shouldRemoveExpiredMoveOutTombstones: Bool
  ) -> SyncBatch<T> {
    SyncBatch(
      collectionIdentifier: collectionIdentifier,
      streamStateKey: streamStateKey,
      rollbackStreamStateKeys: rollbackStreamStateKeys,
      invalidationStreamStateKeys: invalidationStreamStateKeys,
      basePredicateHash: basePredicateHash,
      fetchPredicate: fetchPredicate,
      fetchDescriptor: fetchDescriptor,
      shouldRecordFetch: shouldRecordFetch && shouldRecordEvidence,
      shouldRecordObservation: shouldRecordObservation && shouldRecordEvidence,
      shouldPersistSyncState: shouldPersistSyncState
        && (!persistSyncStateOnlyAtTerminalBoundary || shouldRecordEvidence
          || messages.contains { $0.control == .mustRefetch || $0.kind == .truncate }),
      persistSyncStateOnlyAtTerminalBoundary: persistSyncStateOnlyAtTerminalBoundary,
      shouldUseMoveOutTombstones: shouldUseMoveOutTombstones,
      shouldRemoveExpiredMoveOutTombstones: shouldRemoveExpiredMoveOutTombstones,
      messages: messages,
      metadataProvider: metadataProvider,
      moveOutTracker: moveOutTracker,
      eventHandler: eventHandler,
      tracer: tracer,
      logger: logger,
      cursorOwnershipDiagnostics: cursorOwnershipDiagnostics,
      cursorWriterClientId: cursorWriterClientId,
      isRollbackDualWriteEnabled: isRollbackDualWriteEnabled,
      observationEvidence: observationEvidence,
      protocolCapabilityPolicy: protocolCapabilityPolicy,
      protocolSemanticEpoch: protocolSemanticEpoch,
      runtimeProvider: runtimeProvider,
      requiresSemanticEpochReset: requiresSemanticEpochReset,
      shapeTopology: shapeTopology,
      shapeTopologyLatch: shapeTopologyLatch,
      protocolInputMessages: protocolInputMessages
    )
  }

  func filteringMessages(_ messages: [ElectricMessage]) -> SyncBatch<T> {
    replacing(
      messages: messages,
      shouldRecordEvidence: true,
      shouldRemoveExpiredMoveOutTombstones: shouldRemoveExpiredMoveOutTombstones
    )
  }

  private func persistSyncState(_ state: SyncState, transactionContext: Any?) throws {
    try metadataProvider.updateSyncState(
      collectionId: streamStateKey,
      state: state,
      transaction: transactionContext
    )
    guard state.canResumeWithoutFullBootstrap ? isRollbackDualWriteEnabled() : true else { return }
    let keys =
      state.canResumeWithoutFullBootstrap
      ? rollbackStreamStateKeys
      : invalidationStreamStateKeys
    for rollbackStreamStateKey in keys
    where rollbackStreamStateKey != streamStateKey {
      try metadataProvider.updateSyncState(
        collectionId: rollbackStreamStateKey,
        state: state,
        transaction: transactionContext
      )
    }
  }

  private func needsSemanticEpochReset(transactionContext: Any?) throws -> Bool {
    if requiresSemanticEpochReset { return true }
    if let persistedEpoch = try metadataProvider.getSyncState(
      collectionId: streamStateKey,
      transaction: transactionContext
    )?.protocolSemanticEpoch,
      persistedEpoch != protocolSemanticEpoch
    {
      return true
    }
    if let trackerEpoch = moveOutTracker.pinnedTaggedSegmentSemantics,
      trackerEpoch != protocolSemanticEpoch.isTaggedShapeCapabilityEnabled
    {
      return true
    }
    return false
  }

  private func preflightSupportedEvents(transactionContext: Any?) throws {
    if try needsSemanticEpochReset(transactionContext: transactionContext) {
      guard shouldPersistSyncState || persistSyncStateOnlyAtTerminalBoundary else {
        throw ElectricSyncError.capabilitySemanticEpochTransitionDeferred
      }
      return
    }
    // A committed reset owns the stream until its replacement bootstrap
    // advances the cursor. Letting a subset apply in this window could
    // establish tracker continuity from partial data before the atomic swap.
    if !shouldPersistSyncState,
      !persistSyncStateOnlyAtTerminalBoundary,
      try metadataProvider.getSyncState(
        collectionId: streamStateKey,
        transaction: transactionContext
      ).map({ !$0.canResumeWithoutFullBootstrap }) == true
    {
      throw ElectricSyncError.capabilitySemanticEpochTransitionDeferred
    }
    let taggedShapeCapabilityEnabled = protocolSemanticEpoch.isTaggedShapeCapabilityEnabled
    let continuityFenceApplies =
      !metadataProvider.supportsDurableRowOwnership || taggedShapeCapabilityEnabled
    if continuityFenceApplies,
      !moveOutTracker.isContinuityEstablished,
      try metadataProvider.getSyncState(
        collectionId: streamStateKey,
        transaction: transactionContext
      )?.canResumeWithoutFullBootstrap == true,
      protocolInputMessages.contains(where: Self.isTaggedProtocolInput),
      !(shouldPersistSyncState || persistSyncStateOnlyAtTerminalBoundary)
    {
      throw ElectricSyncError.trackerContinuityBootstrapRequired
    }
    if let quarantine = protocolCapabilityPolicy.quarantine(
      for: protocolInputMessages,
      semanticEpoch: protocolSemanticEpoch
    ) {
      throw ElectricSyncError.protocolQuarantined(quarantine)
    }
  }

  func preflightSupportedEvents() throws {
    try preflightSupportedEvents(transactionContext: nil)
  }

  /// Applies the sync batch to the local database.
  /// MUST be called within a transaction.
  public func apply(in transactionContext: Any?) throws -> Output {
    let processAttributes = mergeTraceAttributes(
      [
        "stage": "batch_process",
        "table": T.tableName,
        "collection": collectionIdentifier,
        "should_record_fetch": "\(shouldRecordFetch)",
        "should_record_observation": "\(shouldRecordObservation)",
        "should_persist_sync_state": "\(shouldPersistSyncState)",
        "rollback_dual_write_key_count": "\(rollbackStreamStateKeys.count)",
        "thread.is_main": electricThreadIsMainValue(),
      ],
      electricMessageAttributes(messages)
    )

    return try withElectricSyncSpan(
      tracer: tracer,
      name: "electric.sync_batch.process",
      attributes: processAttributes
    ) { processSpan in
      var totalRecords = 0
      var subsetSnapshotRecords: [T] = []
      var encounteredTruncate = false
      var lastProcessedMessage: ElectricMessage?
      var processedMessages: [ProcessedMessage<T>] = []
      var missingRowKeys = Set<String>()
      var decodedMessageCount = 0
      var decodedPayloadBytes = 0
      var optimisticPublications = Set<OptimisticPublicationEvidence>()
      var moveOutDeleteCount = 0
      var moveInEventCount = 0
      var moveOutTombstoneCount = 0
      var expiredMoveOutTombstoneCount = 0
      var cursorOwnershipCollisionReports: [ElectricCursorOwnershipCollisionReport] = []

      let syncState = try metadataProvider.getSyncState(
        collectionId: streamStateKey,
        transaction: transactionContext
      )
      let resumedMidStream = syncState?.canResumeWithoutFullBootstrap == true
      let taggedShapeCapabilityEnabled =
        protocolSemanticEpoch.isTaggedShapeCapabilityEnabled

      func forceTrackerLossFullBootstrap(reasonAttribute: String) throws -> Output {
        guard shouldPersistSyncState || persistSyncStateOnlyAtTerminalBoundary else {
          throw ElectricSyncError.trackerContinuityBootstrapRequired
        }
        if metadataProvider.supportsDurableRowOwnership {
          try metadataProvider.clearMetadata(
            table: T.tableName,
            basePredicateHash: basePredicateHash,
            transaction: transactionContext
          )
        } else {
          try metadataProvider.clearMetadata(table: T.tableName, transaction: transactionContext)
        }
        let collisionReport = cursorOwnershipDiagnostics.cursorWriteCollisionReport(
          persistedCursorKey: streamStateKey,
          writerClientId: cursorWriterClientId,
          table: T.tableName,
          collectionIdentifier: collectionIdentifier,
          tracer: tracer
        )
        try persistSyncState(
          .fullBootstrap(protocolSemanticEpoch: protocolSemanticEpoch),
          transactionContext: transactionContext
        )
        if let collisionReport {
          cursorOwnershipCollisionReports.append(collisionReport)
        }
        processSpan.setAttribute(key: reasonAttribute, value: "true")
        var output = Output(
          records: [],
          subsetSnapshotRecords: [],
          encounteredTruncate: true,
          missingRowKeys: [],
          cursorOwnershipCollisionReports: cursorOwnershipCollisionReports
        )
        output.requiresReplacementSwap = true
        output.onTransactionCommitted = { moveOutTracker.reset() }
        return output
      }

      // `preflightSupportedEvents()` is also invoked before a batch reaches
      // this writer. Keep the writer-side check here as the final fence, but
      // let an owner turn a lost-continuity refusal into the established
      // replacement bootstrap instead of leaking it to a preload or mutation
      // caller. Non-owner subsets still throw so their owner can recover.
      do {
        try preflightSupportedEvents(transactionContext: transactionContext)
      } catch ElectricSyncError.trackerContinuityBootstrapRequired {
        return try forceTrackerLossFullBootstrap(
          reasonAttribute: "tracker_preflight_full_bootstrap"
        )
      }

      if try needsSemanticEpochReset(transactionContext: transactionContext) {
        guard shouldPersistSyncState || persistSyncStateOnlyAtTerminalBoundary else {
          throw ElectricSyncError.capabilitySemanticEpochTransitionDeferred
        }
        return try forceTrackerLossFullBootstrap(
          reasonAttribute: "capability_epoch_full_bootstrap"
        )
      }

      if shapeTopology == .staticallySimple,
        moveOutTracker.isContinuityEstablished,
        messages.contains(where: { $0.activeConditions != nil })
      {
        _ = shapeTopologyLatch?.latchDNFSemantics(for: streamStateKey)
        logger.log(
          .warning,
          message: "electric_tracker_rebuild_active_conditions_rejected count=1 latched_dnf=true",
          metadata: [
            "table": T.tableName,
            "collection": collectionIdentifier,
            "tracker_rebuild.active_conditions_rejected": "true",
            "tracker_rebuild.active_conditions_latched_dnf": "true",
          ]
        )
        processSpan.setAttribute(key: "tracker_rebuild.active_conditions_rejected", value: "true")
        processSpan.setAttribute(
          key: "tracker_rebuild.active_conditions_latched_dnf", value: "true")
        return try forceTrackerLossFullBootstrap(
          reasonAttribute: "tracker_rebuild_active_conditions_full_bootstrap"
        )
      }

      if let quarantine = protocolCapabilityPolicy.quarantine(
        for: protocolInputMessages,
        semanticEpoch: protocolSemanticEpoch
      ) {
        throw ElectricSyncError.protocolQuarantined(quarantine)
      }

      let batchMoveOutTracker: MoveOutTagTracker
      var persistedOwnershipTags: [String: [String]] = [:]
      var materializedOwnershipTags: [String: [String]] = [:]
      var releasedOwnershipRowKeys = Set<String>()
      if metadataProvider.supportsDurableRowOwnership {
        batchMoveOutTracker = MoveOutTagTracker(
          isTaggedShapeProtocolEnabled: { taggedShapeCapabilityEnabled }
        )
        // Move events address rows by tag pattern, not by message key, so any
        // move-out or move-in batch must see the complete persisted tag index.
        let containsMoveEvent = messages.contains { $0.event != nil }
        let touchedRowKeys = containsMoveEvent ? nil : Set(messages.compactMap(\.key))
        persistedOwnershipTags = try metadataProvider.getRowOwnershipTags(
          table: T.tableName,
          shapeIdentity: streamStateKey,
          rowKeys: touchedRowKeys,
          transaction: transactionContext
        )
        for (rowKey, tags) in persistedOwnershipTags {
          batchMoveOutTracker.applyTagDelta(
            key: rowKey,
            operation: .insert,
            tags: tags,
            removedTags: nil
          )
        }
        // Durable tags are membership state, not DNF state: active conditions
        // and disjunct positions are process-local and live in the
        // owner-generation tracker between batches. Fold them into this batch's
        // tracker so move-in/move-out evaluate DNF visibility instead of
        // falling back to simple tag removal.
        batchMoveOutTracker.adoptDNFState(from: moveOutTracker)
      } else {
        batchMoveOutTracker = moveOutTracker
      }

      let applyDate: Date? = {
        guard shouldUseMoveOutTombstones, T.moveOutTombstoneTimeToLive != nil else { return nil }
        return runtimeProvider.now()
      }()
      if let applyDate, let timeToLive = T.moveOutTombstoneTimeToLive,
        shouldRemoveExpiredMoveOutTombstones
      {
        expiredMoveOutTombstoneCount = try T.removeExpiredMoveOutTombstones(
          deletedBefore: applyDate.addingTimeInterval(-timeToLive),
          limit: electricMoveOutTombstoneCleanupBatchSize,
          transaction: transactionContext
        )
      }
      let deleteTombstoneContext = ElectricDeleteTombstoneContext(
        streamStateKey: streamStateKey,
        deletedAt: applyDate ?? .distantPast,
        shouldUseMoveOutTombstones: shouldUseMoveOutTombstones
      )

      // Tracker-loss boundary: a process-local owner-generation tracker that
      // has not continuously observed this stream since a full bootstrap
      // (owner eviction, suspension, process restart, reset, identity
      // mismatch) must never fold tagged protocol input over a resumed
      // cursor. Clear the resume state and force a full bootstrap instead of
      // resuming with an empty tracker.
      //
      // Durable-ownership providers persist membership tags, but durable tags
      // are NOT continuity: DNF condition state is process-local, so once the
      // tagged-shape capability is enabled they fence exactly like
      // process-local trackers. With the capability disabled, the legacy
      // simple-shape contract holds — persisted tags are the complete
      // membership state and resume stays safe.
      let continuityFenceApplies =
        !metadataProvider.supportsDurableRowOwnership || taggedShapeCapabilityEnabled
      if continuityFenceApplies, !moveOutTracker.isContinuityEstablished {
        if !resumedMidStream {
          moveOutTracker.establishContinuity(taggedMode: taggedShapeCapabilityEnabled)
        } else if messages.contains(where: Self.isTaggedProtocolInput) {
          batchMoveOutTracker.reset()
          return try forceTrackerLossFullBootstrap(
            reasonAttribute: "tracker_loss_full_bootstrap"
          )
        }
      }

      let decodeAttributes = mergeTraceAttributes(
        [
          "stage": "decode",
          "table": T.tableName,
          "collection": collectionIdentifier,
          "thread.is_main": electricThreadIsMainValue(),
        ],
        electricMessageAttributes(messages)
      )

      try withElectricSyncSpan(
        tracer: tracer,
        name: "electric.sync_batch.decode",
        attributes: decodeAttributes
      ) { decodeSpan in
        for message in messages {
          if let event = message.event {
            switch event {
            case .moveOut(let patterns):
              let keysToDelete = batchMoveOutTracker.processMoveOut(patterns: patterns)
              if !keysToDelete.isEmpty {
                for key in keysToDelete {
                  releasedOwnershipRowKeys.insert(key)
                  if let materializedTags = materializedOwnershipTags.removeValue(forKey: key) {
                    try metadataProvider.updateRowOwnership(
                      table: T.tableName,
                      shapeIdentity: streamStateKey,
                      tagsByRowKey: [key: materializedTags],
                      removedRowKeys: [],
                      transaction: transactionContext
                    )
                  }
                  let moveOutTombstone: ElectricMoveOutTombstone? = try {
                    guard let applyDate,
                      let versionedRow = try T.versionedRowForMoveOut(
                        rowKey: key,
                        transaction: transactionContext
                      )
                    else { return nil }
                    return ElectricMoveOutTombstone(
                      tableName: T.tableName,
                      rowId: versionedRow.rowId,
                      streamStateKey: streamStateKey,
                      version: versionedRow.version,
                      offset: message.offset,
                      cursor: message.cursor,
                      deletedAt: applyDate
                    )
                  }()
                  guard
                    try metadataProvider.releaseRowOwnership(
                      table: T.tableName,
                      rowKey: key,
                      shapeIdentity: streamStateKey,
                      deferredDeleteTombstone: moveOutTombstone,
                      transaction: transactionContext
                    )
                  else { continue }
                  if let moveOutTombstone {
                    try T.recordMoveOutTombstone(
                      moveOutTombstone,
                      transaction: transactionContext
                    )
                    moveOutTombstoneCount += 1
                  }
                  try T.deleteByKey(key, transaction: transactionContext)
                  moveOutDeleteCount += 1
                }
              }
              continue
            case .moveIn(let patterns):
              // Silent: re-activates retained tracker positions only. No
              // base-table write, no publication; a fully removed row returns
              // only through a later Electric change message.
              batchMoveOutTracker.processMoveIn(patterns: patterns)
              moveInEventCount += 1
              continue
            }
          }

          if message.kind == .truncate || message.control == .mustRefetch {
            encounteredTruncate = true
            break
          }

          lastProcessedMessage = message

          // Header-only messages (empty payload) convey offset/handle when the server returns an
          // empty snapshot. Skip decoding but keep metadata.
          if message.payload.isEmpty {
            continue
          }

          decodedMessageCount += 1
          decodedPayloadBytes += message.payload.count
          let processed = try T.processMessage(
            message,
            deleteTombstoneContext: deleteTombstoneContext,
            transaction: transactionContext
          )
          if !processed.missingRowKeys.isEmpty {
            missingRowKeys.formUnion(processed.missingRowKeys)
          }
          let publishedTypedBaseEffect =
            !processed.records.isEmpty
            || (processed.metadata.operation == .delete && processed.missingRowKeys.isEmpty)
          if publishedTypedBaseEffect {
            for rowEffect in processed.optimisticPublishedRowEffects {
              optimisticPublications.insert(
                OptimisticPublicationEvidence(
                  rowEffect: rowEffect,
                  transactionIds: Set(message.txids ?? []),
                  loroFrontiers: Set(processed.confirmedLoroFrontiers)
                )
              )
            }
          }
          if let key = message.key, !metadataProvider.supportsDurableRowOwnership {
            batchMoveOutTracker.applyTagDelta(
              key: key,
              operation: processed.metadata.operation,
              tags: message.tags,
              removedTags: message.removedTags,
              activeConditions: message.activeConditions
            )
          } else if let key = message.key, processed.metadata.operation == .delete {
            batchMoveOutTracker.applyTagDelta(
              key: key,
              operation: .delete,
              tags: message.tags,
              removedTags: message.removedTags
            )
            if metadataProvider.supportsDurableRowOwnership {
              materializedOwnershipTags[key] = nil
              try metadataProvider.removeAllRowOwnership(
                table: T.tableName,
                rowKey: key,
                transaction: transactionContext
              )
            }
          }
          guard !processed.records.isEmpty else { continue }
          if metadataProvider.supportsDurableRowOwnership,
            let key = message.key,
            processed.metadata.operation != .delete
          {
            batchMoveOutTracker.applyTagDelta(
              key: key,
              operation: processed.metadata.operation,
              tags: message.tags,
              removedTags: message.removedTags,
              activeConditions: message.activeConditions
            )
            let noTagsMeansEmptyOwnership =
              processed.metadata.operation == .insert
              || message.tags != nil
              || message.removedTags != nil
            materializedOwnershipTags[key] =
              batchMoveOutTracker.tags(for: key)
              ?? (noTagsMeansEmptyOwnership
                ? [] : materializedOwnershipTags[key] ?? persistedOwnershipTags[key] ?? [])
          }
          totalRecords += processed.records.count
          if message.isSubsetSnapshot {
            subsetSnapshotRecords.append(contentsOf: processed.records)
          }
          processedMessages.append(processed)
        }

        decodeSpan.setAttribute(key: "decoded_message_count", value: "\(decodedMessageCount)")
        decodeSpan.setAttribute(key: "decoded_payload_bytes", value: "\(decodedPayloadBytes)")
        decodeSpan.setAttribute(key: "move_out_deletes", value: "\(moveOutDeleteCount)")
        decodeSpan.setAttribute(key: "move_in_events", value: "\(moveInEventCount)")
        decodeSpan.setAttribute(key: "move_out_tombstones", value: "\(moveOutTombstoneCount)")
        decodeSpan.setAttribute(key: "encountered_truncate", value: "\(encounteredTruncate)")
      }

      processSpan.setAttribute(key: "decoded_message_count", value: "\(decodedMessageCount)")
      processSpan.setAttribute(key: "decoded_payload_bytes", value: "\(decodedPayloadBytes)")
      processSpan.setAttribute(key: "move_out_deletes", value: "\(moveOutDeleteCount)")
      processSpan.setAttribute(key: "move_out_tombstones", value: "\(moveOutTombstoneCount)")
      processSpan.setAttribute(
        key: "expired_move_out_tombstones",
        value: "\(expiredMoveOutTombstoneCount)"
      )
      processSpan.setAttribute(key: "records_count", value: "\(totalRecords)")
      processSpan.setAttribute(key: "encountered_truncate", value: "\(encounteredTruncate)")

      let persistDurableRowOwnership = {
        guard self.metadataProvider.supportsDurableRowOwnership else { return }
        let finalOwnershipTags = batchMoveOutTracker.tagsByRowKey()
        let changedRowKeys = Set(persistedOwnershipTags.keys).union(finalOwnershipTags.keys)
          .filter { persistedOwnershipTags[$0] != finalOwnershipTags[$0] }
        var updatedTags = Dictionary(
          uniqueKeysWithValues: changedRowKeys.compactMap { rowKey in
            finalOwnershipTags[rowKey].map { (rowKey, $0) }
          }
        )
        updatedTags.merge(materializedOwnershipTags) { _, materialized in materialized }
        let removedRowKeys = releasedOwnershipRowKeys.subtracting(materializedOwnershipTags.keys)
        try self.metadataProvider.updateRowOwnership(
          table: T.tableName,
          shapeIdentity: self.streamStateKey,
          tagsByRowKey: updatedTags,
          removedRowKeys: removedRowKeys,
          transaction: transactionContext
        )
      }

      if encounteredTruncate {
        try persistDurableRowOwnership()
        if metadataProvider.supportsDurableRowOwnership {
          try metadataProvider.clearMetadata(
            table: T.tableName,
            basePredicateHash: basePredicateHash,
            transaction: transactionContext
          )
        } else {
          try metadataProvider.clearMetadata(table: T.tableName, transaction: transactionContext)
        }
        if shouldPersistSyncState {
          let state = SyncState(
            offset: "-1",
            // A truncate/must-refetch invalidates the old stream identity; resume from a fresh bootstrap.
            handle: nil,
            cursor: nil,
            isUpToDate: false,
            lastSyncedAt: nil,
            protocolSemanticEpoch: protocolSemanticEpoch
          )
          let collisionReport = cursorOwnershipDiagnostics.cursorWriteCollisionReport(
            persistedCursorKey: streamStateKey,
            writerClientId: cursorWriterClientId,
            table: T.tableName,
            collectionIdentifier: collectionIdentifier,
            tracer: tracer
          )
          try persistSyncState(state, transactionContext: transactionContext)
          if let collisionReport {
            cursorOwnershipCollisionReports.append(collisionReport)
          }
        }

        var output = Output(
          records: [],
          subsetSnapshotRecords: [],
          encounteredTruncate: true,
          missingRowKeys: [],
          cursorOwnershipCollisionReports: cursorOwnershipCollisionReports
        )
        output.requiresReplacementSwap = true
        output.onTransactionCommitted = { moveOutTracker.reset() }
        return output
      }

      let lastMessage = lastProcessedMessage ?? messages.last
      let headerMessage = messages.first

      let recordedOffset = lastMessage?.offset ?? headerMessage?.offset ?? syncState?.offset
      let recordedHandle = lastMessage?.handle ?? headerMessage?.handle ?? syncState?.handle
      let recordedCursor = lastMessage?.cursor ?? headerMessage?.cursor ?? syncState?.cursor

      // Backward compatibility: older call sites set `isUpToDate=true` for both `up-to-date`
      // and `snapshot-end`. If the message includes an explicit `control`, we can distinguish.
      let recordedControl = lastMessage?.control ?? headerMessage?.control
      let legacyEndOfBatch = lastMessage?.isUpToDate ?? headerMessage?.isUpToDate ?? false

      let recordedIsUpToDate: Bool = {
        guard let recordedControl else { return legacyEndOfBatch }
        if recordedControl == .upToDate { return true }
        if recordedControl == .snapshotEnd || recordedControl == .subsetEnd {
          return syncState?.isUpToDate ?? false
        }
        return false
      }()

      let recordedIsComplete: Bool = {
        guard let recordedControl else { return legacyEndOfBatch }
        return recordedControl == .upToDate || recordedControl == .snapshotEnd
          || recordedControl == .subsetEnd
      }()

      processSpan.setAttribute(key: "recorded_is_up_to_date", value: "\(recordedIsUpToDate)")
      processSpan.setAttribute(key: "recorded_is_complete", value: "\(recordedIsComplete)")

      if shouldRecordFetch || shouldRecordObservation {
        let fetchDescriptor =
          fetchDescriptor
          ?? QueryDescriptor(
            predicate: fetchPredicate,
            orderBy: [],
            limit: nil
          )
        let coveragePredicate = fetchPredicate ?? fetchDescriptor.predicate
        let metadataKey = ElectricFetchTracker.metadataKey(
          predicate: coveragePredicate,
          orderBy: fetchDescriptor.orderBy,
          limit: fetchDescriptor.limit,
          cursor: fetchDescriptor.cursor
        )

        if shouldRecordFetch {
          try metadataProvider.recordFetch(
            table: T.tableName,
            predicate: metadataKey.predicateHash,
            predicateJSON: metadataKey.predicateJSON,
            basePredicateHash: basePredicateHash,
            snapshotBoundary: observationEvidence.snapshotBoundary,
            outcome: observationEvidence.outcome,
            isComplete: recordedIsComplete,
            transaction: transactionContext
          )
        } else {
          try metadataProvider.recordObservation(
            table: T.tableName,
            predicate: metadataKey.predicateHash,
            predicateJSON: metadataKey.predicateJSON,
            snapshotBoundary: observationEvidence.snapshotBoundary,
            outcome: observationEvidence.outcome,
            transaction: transactionContext
          )
        }
      }

      if shouldPersistSyncState {
        let state = SyncState(
          offset: recordedOffset,
          handle: recordedHandle,
          cursor: recordedCursor,
          isUpToDate: recordedIsUpToDate,
          lastSyncedAt: runtimeProvider.now(),
          protocolSemanticEpoch: protocolSemanticEpoch
        )

        let collisionReport = cursorOwnershipDiagnostics.cursorWriteCollisionReport(
          persistedCursorKey: streamStateKey,
          writerClientId: cursorWriterClientId,
          table: T.tableName,
          collectionIdentifier: collectionIdentifier,
          tracer: tracer
        )
        try persistSyncState(state, transactionContext: transactionContext)
        if let collisionReport {
          cursorOwnershipCollisionReports.append(collisionReport)
        }
      }

      try persistDurableRowOwnership()
      try metadataProvider.retireOptimisticMutations(
        table: T.tableName,
        publications: optimisticPublications,
        snapshotBoundary: observationEvidence.snapshotBoundary,
        transaction: transactionContext
      )

      // Carry the batch's DNF fold (move-out deactivations, silent move-in
      // re-activations, condition overwrites) forward on the owner-generation
      // tracker so the next durable batch adopts current process-local state.
      if metadataProvider.supportsDurableRowOwnership {
        moveOutTracker.adoptDNFState(from: batchMoveOutTracker)
      }

      return Output(
        records: processedMessages.flatMap { $0.records },
        subsetSnapshotRecords: subsetSnapshotRecords,
        encounteredTruncate: false,
        missingRowKeys: Array(missingRowKeys).sorted(),
        cursorOwnershipCollisionReports: cursorOwnershipCollisionReports
      )
    }
  }

  private static func isTaggedProtocolInput(_ message: ElectricMessage) -> Bool {
    message.tags != nil
      || message.removedTags != nil
      || message.activeConditions != nil
      || message.event != nil
  }

  @discardableResult
  func prepareTruncateSwap(in transactionContext: Any?) throws -> TruncateSwapPreparation {
    try preflightSupportedEvents(transactionContext: transactionContext)
    guard shouldPersistSyncState || persistSyncStateOnlyAtTerminalBoundary else {
      throw ElectricSyncError.trackerContinuityBootstrapRequired
    }
    try persistSyncState(
      .fullBootstrap(protocolSemanticEpoch: protocolSemanticEpoch),
      transactionContext: transactionContext
    )
    // The replacement snapshot is authoritative for this base shape. Drop its
    // prior coverage inside the same transaction that fences the old rows, so
    // a pending subset cannot cache-hit between generations and skip its POST.
    if metadataProvider.supportsDurableRowOwnership {
      try metadataProvider.clearMetadata(
        table: T.tableName,
        basePredicateHash: basePredicateHash,
        transaction: transactionContext
      )
    } else {
      try metadataProvider.clearMetadata(table: T.tableName, transaction: transactionContext)
    }
    guard metadataProvider.supportsDurableRowOwnership else {
      try T.truncate(transaction: transactionContext)
      return TruncateSwapPreparation(
        unownedRowCount: 0,
        deletedRowCount: 0,
        usedTableTruncate: true
      )
    }

    let keysToDelete = try metadataProvider.releaseAllRowOwnership(
      table: T.tableName,
      shapeIdentity: streamStateKey,
      transaction: transactionContext
    )
    try tracer.withSpan(
      name: "electric.truncate_swap.ownership_removals",
      attributes: [
        "table": T.tableName,
        "stream_state_key": streamStateKey,
        "removed_row_count": "\(keysToDelete.count)",
        "removed_row_keys": keysToDelete.sorted().joined(separator: ","),
      ]
    ) { _ in
      for key in keysToDelete {
        try T.deleteByKey(key, transaction: transactionContext)
      }
    }
    return TruncateSwapPreparation(
      unownedRowCount: keysToDelete.count,
      deletedRowCount: keysToDelete.count,
      usedTableTruncate: false
    )
  }
}

public struct ElectricSyncClientConfiguration: Sendable {
  public let metadataProvider: MetadataProvider
  public let httpClient: HTTPClientProvider
  public let httpStreamClient: (any HTTPStreamClientProvider)?
  public let fetchTracker: ElectricFetchTracker
  public let eventHandler: any ElectricSyncEventHandler
  public let tracer: any ElectricSyncTracer
  public let sessionProvider: ElectricSyncSessionProvider
  public let runtimeProvider: ElectricSyncRuntimeProvider
  public let logger: any LogProvider
  public let isExactCursorCutoverEnabled: Bool
  public let legacyBootstrapAdmissionController: ElectricLegacyBootstrapAdmissionController?
  public let isRollbackDualWriteEnabled: @Sendable () -> Bool
  public let protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy

  public init(
    metadataProvider: MetadataProvider,
    httpClient: HTTPClientProvider,
    httpStreamClient: (any HTTPStreamClientProvider)? = nil,
    fetchTracker: ElectricFetchTracker? = nil,
    eventHandler: any ElectricSyncEventHandler = NoopElectricSyncEventHandler(),
    tracer: any ElectricSyncTracer = NoopElectricSyncTracer(),
    sessionProvider: ElectricSyncSessionProvider = .unmanaged,
    runtimeProvider: ElectricSyncRuntimeProvider = .live,
    logger: any LogProvider = NoopLogProvider(),
    isExactCursorCutoverEnabled: Bool = false,
    legacyBootstrapAdmissionController: ElectricLegacyBootstrapAdmissionController? = nil,
    isRollbackDualWriteEnabled: @escaping @Sendable () -> Bool = { true },
    protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy = .defaultOff
  ) {
    self.metadataProvider = metadataProvider
    self.httpClient = httpClient
    self.httpStreamClient = httpStreamClient
    self.fetchTracker = fetchTracker ?? ElectricFetchTracker(metadataProvider: metadataProvider)
    self.eventHandler = eventHandler
    self.tracer = tracer
    self.sessionProvider = sessionProvider
    self.runtimeProvider = runtimeProvider
    self.logger = logger
    self.isExactCursorCutoverEnabled = isExactCursorCutoverEnabled
    self.legacyBootstrapAdmissionController = legacyBootstrapAdmissionController
    self.isRollbackDualWriteEnabled = isRollbackDualWriteEnabled
    self.protocolCapabilityPolicy = protocolCapabilityPolicy
  }
}

public enum ElectricSyncError: Error {
  case noNetworkClient
  case subscriptionCancelled
  case fetchFailed(String)
  case legacyExactMissBootstrapDisabled
  case capabilitySemanticEpochTransitionDeferred
  case trackerContinuityBootstrapRequired
  case protocolQuarantined(ElectricProtocolQuarantine)
}

private enum ElectricLegacyBootstrapScope {
  @TaskLocal static var admission: ElectricLegacyBootstrapAdmission?
}

public enum ElectricSyncAwaitError: Error, Equatable {
  case timeoutWaitingForTxId(Int64)
  case timeoutWaitingForMatch
}

public final class ElectricLiveStreamController: @unchecked Sendable {
  private struct Operations: Sendable {
    let cancel: @Sendable () -> Void
    let wait: @Sendable () async -> Void
  }

  private let lock = NSLock()
  private var operations: Operations?

  fileprivate func install(task: Task<Void, Never>) {
    lock.withLock {
      operations = Operations(
        cancel: { task.cancel() },
        wait: { await task.value }
      )
    }
  }

  public func cancelAndWait() async {
    let operations = lock.withLock { self.operations }
    operations?.cancel()
    await operations?.wait()
  }
}

public actor ElectricSyncClientImpl {
  private let metadataProvider: MetadataProvider
  private let httpClient: HTTPClientProvider
  private let httpStreamClient: (any HTTPStreamClientProvider)?
  private let fetchTracker: ElectricFetchTracker
  private let eventHandler: any ElectricSyncEventHandler
  private let tracer: any ElectricSyncTracer
  public nonisolated let sessionProvider: ElectricSyncSessionProvider
  public nonisolated let runtimeProvider: ElectricSyncRuntimeProvider
  public nonisolated let logger: any LogProvider
  public nonisolated let isExactCursorCutoverEnabled: Bool
  let legacyBootstrapAdmissionController: ElectricLegacyBootstrapAdmissionController?
  private let isRollbackDualWriteEnabled: @Sendable () -> Bool
  public nonisolated let protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy
  let cursorOwnershipDiagnostics: ElectricCursorOwnershipDiagnostics

  private let maxBufferedMessagesPerSync: Int = 50_000
  private let maxHTTPFetchesPerSync: Int = 20
  private var moveOutTrackers: [String: MoveOutTagTracker] = [:]
  private let shapeTopologyLatch = ElectricShapeTopologyLatch()

  private struct TxidWaiter: Sendable {
    let txid: Int64
    let continuation: CheckedContinuation<Bool, Error>
    let timeoutTask: Task<Void, Never>
  }

  private struct MatchWaiter: Sendable {
    let match: @Sendable (ElectricMessage) -> Bool
    var matched: Bool
    let continuation: CheckedContinuation<Bool, Error>
    let timeoutTask: Task<Void, Never>
  }

  private enum ResumeSource: String, Equatable {
    case exact
    case fresh
    case legacyAdopted = "legacy_adopted"
    case legacyExactMiss = "legacy_exact_miss"
    case legacyPreCutover = "legacy_pre_cutover"
    /// Exact-disabled resume whose persisted state carries a compatible-mode
    /// bridge attestation: the application atomically renamed the cursor, row
    /// ownership, and tombstones from another mode's legacy key and recorded
    /// the source mode on the state itself.
    case legacyBridged = "legacy_bridged"
  }

  private struct ResumedSyncState {
    let state: SyncState?
    let source: ResumeSource
    let rollbackStreamStateKeys: [String]
    let invalidationStreamStateKeys: [String]

    var persistedState: SyncState? {
      switch source {
      case .fresh, .legacyExactMiss:
        nil
      case .exact, .legacyAdopted, .legacyPreCutover, .legacyBridged:
        state
      }
    }

    var hasPersistedFullBootstrap: Bool {
      persistedState?.canResumeWithoutFullBootstrap == false
    }
  }

  private var seenTxids: Set<Int64> = []
  private var seenSnapshots: [PostgresSnapshot] = []
  private var currentBatchMessages: [ElectricMessage] = []
  private var pendingTxidWaiters: [UUID: TxidWaiter] = [:]
  private var pendingMatchWaiters: [UUID: MatchWaiter] = [:]

  public init(configuration: ElectricSyncClientConfiguration) {
    self.metadataProvider = configuration.metadataProvider
    self.httpClient = configuration.httpClient
    self.httpStreamClient = configuration.httpStreamClient
    self.fetchTracker = configuration.fetchTracker
    self.eventHandler = configuration.eventHandler
    self.tracer = configuration.tracer
    self.sessionProvider = configuration.sessionProvider
    self.runtimeProvider = configuration.runtimeProvider
    self.logger = configuration.logger
    self.isExactCursorCutoverEnabled = configuration.isExactCursorCutoverEnabled
    self.legacyBootstrapAdmissionController = configuration.legacyBootstrapAdmissionController
    self.isRollbackDualWriteEnabled = configuration.isRollbackDualWriteEnabled
    self.protocolCapabilityPolicy = configuration.protocolCapabilityPolicy
    self.cursorOwnershipDiagnostics = .shared
  }

  init(
    configuration: ElectricSyncClientConfiguration,
    cursorOwnershipDiagnostics: ElectricCursorOwnershipDiagnostics
  ) {
    self.metadataProvider = configuration.metadataProvider
    self.httpClient = configuration.httpClient
    self.httpStreamClient = configuration.httpStreamClient
    self.fetchTracker = configuration.fetchTracker
    self.eventHandler = configuration.eventHandler
    self.tracer = configuration.tracer
    self.sessionProvider = configuration.sessionProvider
    self.runtimeProvider = configuration.runtimeProvider
    self.logger = configuration.logger
    self.isExactCursorCutoverEnabled = configuration.isExactCursorCutoverEnabled
    self.legacyBootstrapAdmissionController = configuration.legacyBootstrapAdmissionController
    self.isRollbackDualWriteEnabled = configuration.isRollbackDualWriteEnabled
    self.protocolCapabilityPolicy = configuration.protocolCapabilityPolicy
    self.cursorOwnershipDiagnostics = cursorOwnershipDiagnostics
  }

  private func moveOutTracker(streamStateKey: String) -> MoveOutTagTracker {
    if let existing = moveOutTrackers[streamStateKey] {
      return existing
    }
    let tracker = makeMoveOutTracker()
    moveOutTrackers[streamStateKey] = tracker
    return tracker
  }

  private func makeMoveOutTracker() -> MoveOutTagTracker {
    let capabilityPolicy = protocolCapabilityPolicy
    return MoveOutTagTracker(
      isTaggedShapeProtocolEnabled: { capabilityPolicy.isTaggedShapeCapabilityEnabled() }
    )
  }

  private func effectiveShapeTopology(
    _ declaredTopology: ElectricShapeTopology,
    streamStateKey: String
  ) -> ElectricShapeTopology {
    guard declaredTopology == .staticallySimple,
      shapeTopologyLatch.hasLatchedDNFSemantics(for: streamStateKey)
    else { return declaredTopology }
    return .dnf
  }

  private func requiresSemanticEpochReset(
    syncState: SyncState?,
    tracker: MoveOutTagTracker,
    semanticEpoch: ElectricProtocolSemanticEpoch
  ) -> Bool {
    if let persistedEpoch = syncState?.protocolSemanticEpoch,
      persistedEpoch != semanticEpoch
    {
      return true
    }
    if let trackerEpoch = tracker.pinnedTaggedSegmentSemantics,
      trackerEpoch != semanticEpoch.isTaggedShapeCapabilityEnabled
    {
      return true
    }
    return false
  }

  private enum SimpleTrackerRebuildAdmission {
    case admitted
    case alreadyEstablished
    case refused(reason: String)
  }

  private static let bridgeCompatibleModes: Set<ElectricCollectionSyncMode> = [.eager, .progressive]

  private func rebuildSimpleTrackerIfAdmissible<T: ElectricCollectionModel>(
    _: T.Type,
    identity: ElectricReplicaIdentity,
    resumedState: ResumedSyncState,
    shapeTopology: ElectricShapeTopology,
    syncMode: ElectricCollectionSyncMode,
    tracker: MoveOutTagTracker,
    semanticEpoch: ElectricProtocolSemanticEpoch
  ) throws -> SimpleTrackerRebuildAdmission {
    guard metadataProvider.supportsDurableRowOwnership else {
      return .refused(reason: "durable_row_ownership_unsupported")
    }
    guard shapeTopology == .staticallySimple else {
      return .refused(reason: "shape_topology_not_statically_simple")
    }
    // `.exact` carries continuity by construction. `.legacyBridged` carries it
    // by application attestation: the cursor, row ownership, and tombstones
    // were atomically renamed from a compatible mode's legacy key. The bridge
    // is only defined between the full-log modes; onDemand starts a fresh
    // changes_only tail instead and never rides a bridge.
    let ownershipShapeIdentity: String
    switch resumedState.source {
    case .exact:
      ownershipShapeIdentity = identity.persistedCursorKey
    case .legacyBridged:
      guard let bridgedFrom = resumedState.persistedState?.bridgedFromSyncMode,
        Self.bridgeCompatibleModes.contains(bridgedFrom),
        Self.bridgeCompatibleModes.contains(syncMode)
      else {
        return .refused(reason: "bridge_mode_pair_not_compatible")
      }
      ownershipShapeIdentity = identity.legacyPersistedCursorKey(syncMode: syncMode)
    case .fresh, .legacyAdopted, .legacyExactMiss, .legacyPreCutover:
      return .refused(reason: "resume_source_not_exact")
    }
    guard resumedState.persistedState?.canResumeWithoutFullBootstrap == true else {
      return .refused(reason: "persisted_state_not_resumable")
    }
    guard resumedState.persistedState?.protocolSemanticEpoch == semanticEpoch else {
      return .refused(reason: "protocol_semantic_epoch_mismatch")
    }
    guard !tracker.isContinuityEstablished else {
      return .alreadyEstablished
    }
    guard
      let tags = try metadataProvider.trackerRebuildOwnership(
        table: T.tableName,
        shapeIdentity: ownershipShapeIdentity,
        localTableOwnership: T.electricLocalTableOwnership,
        transaction: nil
      )
    else {
      return .refused(reason: "local_ownership_validation_refused")
    }
    tracker.rebuildSimpleMembership(tags, taggedMode: semanticEpoch.isTaggedShapeCapabilityEnabled)
    return .admitted
  }

  private func admitsFreshOnDemandStaticSimple<T: ElectricCollectionModel>(
    _: T.Type,
    resumedState: ResumedSyncState,
    syncMode: ElectricCollectionSyncMode,
    shapeTopology: ElectricShapeTopology
  ) throws -> Bool {
    guard
      syncMode == .onDemand
        && shapeTopology == .staticallySimple
        && resumedState.source == .fresh
        && resumedState.persistedState == nil
        && metadataProvider.supportsDurableRowOwnership
    else { return false }
    // A fresh state has a synthetic `-1` cursor. Once this path has identified
    // itself as a candidate, a refused or unreadable pristine admission must
    // never fall through to an ordinary on-demand request using that cursor.
    // The collection owner catches this sentinel and retries as an explicit
    // authoritative replacement instead.
    do {
      guard
        try metadataProvider.admitsFreshOnDemandPristineOwner(
          table: T.tableName,
          localTableOwnership: T.electricLocalTableOwnership,
          transaction: nil
        )
      else {
        throw ElectricSyncError.trackerContinuityBootstrapRequired
      }
      return true
    } catch ElectricSyncError.trackerContinuityBootstrapRequired {
      throw ElectricSyncError.trackerContinuityBootstrapRequired
    } catch {
      throw ElectricSyncError.trackerContinuityBootstrapRequired
    }
  }

  /// True when this model's move-out semantics depend on the process-local
  /// membership tracker: continuity loss (restart, eviction, suspension,
  /// reset, mismatch) then requires a full bootstrap instead of an
  /// incremental resume. Durable ownership persists membership tags, but the
  /// tagged-shape protocol's DNF condition state remains process-local, so the
  /// tagged epoch always depends on the tracker. Under legacy segment
  /// semantics only move-out tombstones live in the tracker, and durable row
  /// ownership keeps those in GRDB.
  public func requiresProcessTrackerContinuity<T: ElectricCollectionModel>(
    _: T.Type,
    shapeTopology: ElectricShapeTopology = .dnf
  ) -> Bool {
    (T.moveOutTombstoneTimeToLive != nil && !metadataProvider.supportsDurableRowOwnership)
      || (protocolCapabilityPolicy.semanticEpoch().isTaggedShapeCapabilityEnabled
        && !metadataProvider.supportsDurableRowOwnership)
      || shapeTopology == .dnf
  }

  /// Determines whether this owner must replace its stream before it can use
  /// tagged protocol input. Declared-simple shapes can avoid that replacement
  /// only when their exact durable ownership state reconstructs the tracker.
  /// A refused rebuild is not permission to resume: eager and DNF owners take
  /// the full-bootstrap path, while statically-simple on-demand owners replace
  /// their working set from the pending demanded subset.
  public func requiresFullBootstrapForTrackerContinuity<T: ElectricCollectionModel>(
    _: T.Type,
    identity: ElectricReplicaIdentity,
    syncMode: ElectricCollectionSyncMode,
    shapeTopology: ElectricShapeTopology
  ) throws -> Bool {
    let semanticEpoch = protocolCapabilityPolicy.semanticEpoch()
    let streamStateKey = persistedCursorKey(identity: identity, syncMode: syncMode)
    let effectiveShapeTopology = effectiveShapeTopology(
      shapeTopology,
      streamStateKey: streamStateKey
    )
    let tracker = moveOutTracker(streamStateKey: streamStateKey)
    do {
      // resumeSyncState is inside the catch: it throws deterministically for
      // legacy-cursor devices without an exact cursor when no bootstrap
      // admission is bound (legacyExactMissBootstrapDisabled), and this
      // precheck runs before the owner flow binds one. An escaped throw here
      // kills the detached owner task for the whole session.
      let resumedState = try resumeSyncState(identity: identity, syncMode: syncMode)
      // A fresh on-demand static-simple owner has no prior generation whose
      // tracker or rows need replacement. Its first subset is the baseline;
      // forcing an unscoped bootstrap here would violate on-demand's
      // snapshot-first contract. This deliberately excludes exact, adopted,
      // legacy-miss, and DNF state, which remain fail-closed below.
      if try admitsFreshOnDemandStaticSimple(
        T.self,
        resumedState: resumedState,
        syncMode: syncMode,
        shapeTopology: effectiveShapeTopology
      ) {
        return false
      }
      let rebuildAdmission = try rebuildSimpleTrackerIfAdmissible(
        T.self,
        identity: identity,
        resumedState: resumedState,
        shapeTopology: effectiveShapeTopology,
        syncMode: syncMode,
        tracker: tracker,
        semanticEpoch: semanticEpoch
      )
      if case .refused(let reason) = rebuildAdmission {
        logger.log(
          .warning,
          message: "electric_tracker_rebuild_admission_refused",
          metadata: [
            "table": T.tableName,
            "collection": identity.modelIdentifier,
            "reason": reason,
            "local_table_ownership": T.electricLocalTableOwnership.rawValue,
            "shape_topology": String(describing: effectiveShapeTopology),
            "resume_source": resumedState.source.rawValue,
            "semantic_epoch": semanticEpoch.rawValue,
          ]
        )
      }
    } catch {
      // Admission is an optimization. Any failed validation (resume, SQL,
      // malformed row key, or provider failure) is indistinguishable from a
      // refused rebuild. Statically-simple on-demand owners can replace their
      // working set from the pending subset; other owners still need a full
      // bootstrap.
      let usesDemandedSubsetReset = prefersDemandedSubsetResetForTrackerContinuity(
        identity: identity,
        syncMode: syncMode,
        shapeTopology: shapeTopology
      )
      logger.log(
        .warning,
        message:
          usesDemandedSubsetReset
          ? "electric_tracker_rebuild_admission_failed_subset_reset"
          : "electric_tracker_rebuild_admission_failed_full_bootstrap",
        metadata: [
          "table": T.tableName,
          "collection": identity.modelIdentifier,
          "reason": "admission_error",
          "local_table_ownership": T.electricLocalTableOwnership.rawValue,
          "error_type": String(describing: type(of: error)),
        ]
      )
      return true
    }

    guard !tracker.isContinuityEstablished else { return false }
    if requiresProcessTrackerContinuity(T.self, shapeTopology: effectiveShapeTopology) {
      return true
    }
    return semanticEpoch.isTaggedShapeCapabilityEnabled
      && effectiveShapeTopology == .staticallySimple
  }

  func prefersDemandedSubsetResetForTrackerContinuity(
    identity: ElectricReplicaIdentity,
    syncMode: ElectricCollectionSyncMode,
    shapeTopology: ElectricShapeTopology
  ) -> Bool {
    guard syncMode == .onDemand else { return false }
    let streamStateKey = persistedCursorKey(identity: identity, syncMode: syncMode)
    return effectiveShapeTopology(shapeTopology, streamStateKey: streamStateKey)
      == .staticallySimple
  }

  @discardableResult
  private func withAsyncSpan<T: Sendable>(
    name: String,
    attributes: [String: String] = [:],
    operation: (_ span: any ElectricSyncSpan) async throws -> T
  ) async throws -> T {
    try await withElectricAsyncSpan(
      tracer: tracer,
      name: name,
      attributes: attributes,
      operation: operation
    )
  }

  /// Holds the process-wide legacy-bootstrap slot across both fetch and the
  /// caller's GRDB publication. Resumable exact rows, fresh installs, and
  /// unanimously proven v1 adoption bypass admission. An exact reset row with
  /// rollback evidence still requires the slot because it will full-bootstrap.
  public func withLegacyBootstrapAdmission<Output: Sendable>(
    identity: ElectricReplicaIdentity?,
    stage: String,
    syncMode: ElectricCollectionSyncMode? = nil,
    expectsExactCursorAdvance: Bool = true,
    operation: @Sendable () async throws -> Output
  ) async throws -> Output {
    guard let identity else {
      return try await operation()
    }
    if ElectricLegacyBootstrapScope.admission?.identity == identity {
      return try await operation()
    }
    guard try requiresLegacyBootstrapAdmission(identity: identity, syncMode: syncMode) else {
      return try await operation()
    }
    let legacyCursorCount = try legacyCursorEvidenceCount(identity: identity)

    return try await withAsyncSpan(
      name: "electric.legacy_bootstrap",
      attributes: [
        "stage": stage,
        "collection": identity.modelIdentifier,
        "table": identity.tableName,
        "legacy_bootstrap.legacy_cursor_count": "\(legacyCursorCount)",
      ]
    ) { span in
      guard let controller = legacyBootstrapAdmissionController else {
        span.setAttribute(key: "legacy_bootstrap.result", value: "rejected")
        throw ElectricSyncError.legacyExactMissBootstrapDisabled
      }

      let admission: ElectricLegacyBootstrapAdmission
      do {
        admission = try await controller.acquire(
          identity: identity,
          legacyCursorCount: legacyCursorCount
        )
      } catch {
        Self.annotateLegacyBootstrapMetrics(await controller.metricsSnapshot(), span: span)
        span.setAttribute(
          key: "legacy_bootstrap.result",
          value: error is CancellationError ? "cancelled" : "rejected"
        )
        throw error
      }

      if try !requiresLegacyBootstrapAdmission(identity: identity, syncMode: syncMode) {
        let metrics = await controller.finish(admission, outcome: .supersededByExactState)
        Self.annotateLegacyBootstrapMetrics(metrics, span: span)
        span.setAttribute(key: "legacy_bootstrap.result", value: "superseded")
        return try await operation()
      }

      do {
        let output = try await ElectricLegacyBootstrapScope.$admission.withValue(admission) {
          try await operation()
        }
        let cursorAdvanced = try {
          if isExactCursorCutoverEnabled {
            return try exactSyncState(identity: identity)?.canResumeWithoutFullBootstrap == true
          }
          guard let syncMode else { return false }
          return try metadataProvider.getSyncState(
            collectionId: identity.legacyPersistedCursorKey(syncMode: syncMode),
            transaction: nil
          )?.canResumeWithoutFullBootstrap == true
        }()
        let outcome: ElectricLegacyBootstrapOutcome =
          expectsExactCursorAdvance
          ? .completed(exactCursorAdvanced: cursorAdvanced)
          : .completedWithoutCursorAdvance
        let metrics = await controller.finish(admission, outcome: outcome)
        Self.annotateLegacyBootstrapMetrics(metrics, span: span)
        span.setAttribute(key: "legacy_bootstrap.result", value: "completed")
        return output
      } catch is CancellationError {
        let metrics = await controller.finish(admission, outcome: .cancelled)
        Self.annotateLegacyBootstrapMetrics(metrics, span: span)
        span.setAttribute(key: "legacy_bootstrap.result", value: "cancelled")
        throw CancellationError()
      } catch {
        let metrics = await controller.finish(admission, outcome: .failed)
        Self.annotateLegacyBootstrapMetrics(metrics, span: span)
        span.setAttribute(key: "legacy_bootstrap.result", value: "failed")
        throw error
      }
    }
  }

  public func requiresLegacyBootstrapAdmission(
    identity: ElectricReplicaIdentity,
    syncMode: ElectricCollectionSyncMode? = nil
  ) throws -> Bool {
    if !isExactCursorCutoverEnabled {
      if let syncMode,
        try metadataProvider.getSyncState(
          collectionId: identity.legacyPersistedCursorKey(syncMode: syncMode),
          transaction: nil
        )?.canResumeWithoutFullBootstrap == true
      {
        return false
      }
      return try legacyCursorEvidenceCount(identity: identity) > 0
    }
    let exactState = try exactSyncState(identity: identity)
    if exactState?.canResumeWithoutFullBootstrap == true { return false }
    if exactState == nil, try provenLegacyResumeState(identity: identity) != nil { return false }
    return try legacyCursorEvidenceCount(identity: identity) > 0
  }

  private nonisolated static func annotateLegacyBootstrapMetrics(
    _ metrics: ElectricLegacyBootstrapMetricsSnapshot,
    span: any ElectricSyncSpan
  ) {
    span.setAttribute(key: "legacy_bootstrap.queue_depth", value: "\(metrics.queued)")
    span.setAttribute(key: "legacy_bootstrap.in_flight", value: "\(metrics.inFlight)")
    span.setAttribute(key: "legacy_bootstrap.admitted_total", value: "\(metrics.admitted)")
    span.setAttribute(key: "legacy_bootstrap.completed_total", value: "\(metrics.completed)")
    span.setAttribute(key: "legacy_bootstrap.failed_total", value: "\(metrics.failed)")
    span.setAttribute(key: "legacy_bootstrap.cancelled_total", value: "\(metrics.cancelled)")
    span.setAttribute(key: "legacy_bootstrap.rejected_total", value: "\(metrics.rejected)")
    span.setAttribute(
      key: "legacy_bootstrap.exact_cursor_advanced_total",
      value: "\(metrics.exactCursorAdvanced)"
    )
    span.setAttribute(
      key: "legacy_bootstrap.decoded_payload_bytes_total",
      value: "\(metrics.decodedPayloadBytes)"
    )
  }

  public func awaitTxId(
    _ txid: Int64,
    timeout: TimeInterval = 5.0
  ) async throws -> Bool {
    if seenTxids.contains(txid) { return true }
    if isVisibleInAnySnapshot(txid) { return true }

    return try await withCheckedThrowingContinuation { continuation in
      let id = runtimeProvider.makeUUID()
      let timeoutTask = Task {
        try? await runtimeProvider.sleep(for: .seconds(timeout))
        await self.timeoutTxidWaiter(id: id)
      }
      pendingTxidWaiters[id] = TxidWaiter(
        txid: txid,
        continuation: continuation,
        timeoutTask: timeoutTask
      )
    }
  }

  public func awaitMatch(
    timeout: TimeInterval = 3.0,
    match: @escaping @Sendable (ElectricMessage) -> Bool
  ) async throws -> Bool {
    if currentBatchMessages.contains(where: match),
      currentBatchMessages.contains(where: Self.isUpToDateControl)
    {
      return true
    }

    let immediateMatch = currentBatchMessages.contains(where: match)

    return try await withCheckedThrowingContinuation { continuation in
      let id = runtimeProvider.makeUUID()
      let timeoutTask = Task {
        try? await runtimeProvider.sleep(for: .seconds(timeout))
        await self.timeoutMatchWaiter(id: id)
      }
      pendingMatchWaiters[id] = MatchWaiter(
        match: match,
        matched: immediateMatch,
        continuation: continuation,
        timeoutTask: timeoutTask
      )
    }
  }

  /// Requests an on-demand snapshot through the owning shape stream.
  ///
  /// Catch-up and subset messages are retained in protocol order. The owning
  /// transaction advances resume metadata only when the terminal subset
  /// boundary commits. `restartOnDemandFromNow` is reserved for callers that
  /// have already armed an atomic replacement of the owner's stale generation.
  public func requestSnapshot<T>(
    _ type: T.Type,
    basePredicate: SQLExpression?,
    shapeTopology: ElectricShapeTopology = .dnf,
    descriptor: QueryDescriptor,
    syncMode: ElectricCollectionSyncMode,
    coverageSyncMode: ElectricCollectionSyncMode? = nil,
    ignorePersistedSyncState: Bool = false,
    restartOnDemandFromNow: Bool = false,
    consultFetchCoverage: Bool = true,
    recordAsObservation: Bool = false,
    replicaIdentity: ElectricReplicaIdentity? = nil
  ) async throws -> SyncBatch<T>? where T: ElectricCollectionModel {
    let resolvedReplicaIdentity = try resolvedReplicaIdentity(
      for: T.self,
      basePredicate: basePredicate,
      provided: replicaIdentity
    )
    let spanAttributes: [String: String] = [
      "stage": "request_snapshot",
      "table": T.tableName,
      "collection": resolvedReplicaIdentity.modelIdentifier,
      "sync.mode": electricSyncModeLabel(syncMode),
      "has_base_predicate": "\(basePredicate != nil)",
      "has_cursor": "\(descriptor.cursor != nil)",
      "ignore_persisted_sync_state": "\(ignorePersistedSyncState)",
      "restart_on_demand_from_now": "\(restartOnDemandFromNow)",
      "thread.is_main": electricThreadIsMainValue(),
    ]

    return try await withAsyncSpan(
      name: "electric.request_snapshot",
      attributes: spanAttributes
    ) { span in
      let protocolSemanticEpoch = protocolCapabilityPolicy.semanticEpoch()
      let cursor = descriptor.cursor
      let coveragePredicate = ElectricFetchTracker.combinedCoveragePredicate(
        scope: basePredicate,
        requested: descriptor.predicate
      )
      guard !restartOnDemandFromNow || syncMode == .onDemand else {
        throw ElectricSyncError.fetchFailed(
          "restartOnDemandFromNow requires an on-demand collection"
        )
      }

      let fetchPlan: FetchPlan? =
        if consultFetchCoverage, !restartOnDemandFromNow, cursor == nil {
          try await fetchTracker.computeMissing(
            table: T.tableName,
            requested: descriptor.predicate,
            scope: basePredicate,
            orderBy: descriptor.orderBy,
            limit: descriptor.limit
          )
        } else {
          nil
        }

      if let fetchPlan {
        span.setAttribute(key: "fetch_plan.needs_fetch", value: "\(fetchPlan.needsFetch)")
      }

      let effectiveCoverageSyncMode = coverageSyncMode ?? syncMode

      // Progressive subset queries (with limit/orderBy) must always re-fetch
      // because the subset composition changes as new data arrives through
      // the stream. The scoped metadata from a prior fetch is stale.
      let isProgressiveSubset =
        effectiveCoverageSyncMode == .progressive && descriptor.limit != nil

      if let fetchPlan, !fetchPlan.needsFetch, !isProgressiveSubset {
        span.setAttribute(key: "result", value: "skipped_fetch_plan_complete")
        return nil
      }

      let replicaIdentity = resolvedReplicaIdentity
      let streamStateKey = persistedCursorKey(
        identity: replicaIdentity,
        syncMode: syncMode
      )
      let effectiveShapeTopology = effectiveShapeTopology(
        shapeTopology,
        streamStateKey: streamStateKey
      )

      let fetchDescriptor: QueryDescriptor =
        if descriptor.cursor != nil {
          // Cursor-based loads must not share the same metadata key as non-cursor loads,
          // or we'd incorrectly mark "limit N" as complete after the first page.
          descriptor
        } else if basePredicate != nil {
          QueryDescriptor(
            predicate: descriptor.predicate,
            orderBy: descriptor.orderBy,
            limit: descriptor.limit
          )
        } else {
          QueryDescriptor(
            predicate: fetchPlan?.predicate ?? descriptor.predicate,
            orderBy: descriptor.orderBy,
            limit: descriptor.limit
          )
        }

      let fetchMetadataKey = ElectricFetchTracker.metadataKey(
        predicate: ElectricFetchTracker.combinedCoveragePredicate(
          scope: basePredicate,
          requested: fetchDescriptor.predicate
        ),
        orderBy: fetchDescriptor.orderBy,
        limit: fetchDescriptor.limit,
        cursor: fetchDescriptor.cursor
      )
      if consultFetchCoverage, cursor != nil {
        let unscopedKey = ElectricFetchTracker.metadataKey(
          predicate: coveragePredicate,
          orderBy: [],
          limit: nil
        ).predicateHash
        if try metadataProvider.hasFetched(
          table: T.tableName,
          predicate: unscopedKey,
          transaction: nil
        ) {
          span.setAttribute(key: "result", value: "skipped_cursor_unscoped_cached")
          return nil
        }

        if try metadataProvider.hasFetched(
          table: T.tableName,
          predicate: fetchMetadataKey.predicateHash,
          transaction: nil
        ) {
          span.setAttribute(key: "result", value: "skipped_cursor_scoped_cached")
          return nil
        }
      }

      // A retry after an observed truncate must not resume from the persisted
      // stream identity: this batch does not own that state (the live stream
      // owner resets it), so re-reading it would replay the same invalidated
      // offset/handle/cursor and repeat the truncate. Fetch like a first load.
      let resumedSyncState = try resumeSyncState(
        identity: replicaIdentity,
        syncMode: syncMode
      )
      span.setAttribute(key: "resume.source", value: resumedSyncState.source.rawValue)
      let admitsFreshOnDemandStaticSimple =
        if restartOnDemandFromNow {
          false
        } else {
          try admitsFreshOnDemandStaticSimple(
            T.self,
            resumedState: resumedSyncState,
            syncMode: syncMode,
            shapeTopology: effectiveShapeTopology
          )
        }
      // `resumeSyncState` represents a fresh install with a synthetic `-1`
      // state so recovery paths can force a bootstrap. This direct on-demand
      // subset is not recovery, so begin at the normal `now` snapshot point.
      let syncState =
        ignorePersistedSyncState || restartOnDemandFromNow || admitsFreshOnDemandStaticSimple
        ? nil
        : resumedSyncState.state
      let tracker = moveOutTracker(streamStateKey: streamStateKey)
      if !ignorePersistedSyncState, !restartOnDemandFromNow {
        _ = try rebuildSimpleTrackerIfAdmissible(
          T.self,
          identity: replicaIdentity,
          resumedState: resumedSyncState,
          shapeTopology: effectiveShapeTopology,
          syncMode: syncMode,
          tracker: tracker,
          semanticEpoch: protocolSemanticEpoch
        )
      }
      guard
        !requiresSemanticEpochReset(
          syncState: resumedSyncState.persistedState,
          tracker: tracker,
          semanticEpoch: protocolSemanticEpoch
        ),
        !resumedSyncState.hasPersistedFullBootstrap || ignorePersistedSyncState
      else {
        throw ElectricSyncError.capabilitySemanticEpochTransitionDeferred
      }
      let shouldRecordFetchMetadata = shouldRecordFetchMetadata(
        syncMode: effectiveCoverageSyncMode,
        syncState: syncState,
        descriptor: descriptor
      )

      let initialOffset = initialOffsetForStream(syncMode: syncMode)
      let requestOffset =
        ignorePersistedSyncState
        ? "-1"
        : restartOnDemandFromNow ? "now" : syncState?.offset ?? initialOffset
      var requestHandle = syncState?.handle

      if requestOffset == "-1" {
        requestHandle = cacheBustingHandle(base: requestHandle)
      }

      var request = T.createIdentifiedShapeRequest(
        where: basePredicate,
        orderBy: [],
        limit: nil,
        offset: requestOffset,
        handle: requestHandle,
        cursor: syncState?.cursor,
        live: false
      ).with(wireIdentity: replicaIdentity.wireIdentity)

      request = request.with(log: logModeFor(syncMode: syncMode))

      let messages: [ElectricMessage]
      let fetchCallCount: Int
      if let cursor {
        let whereCurrentPredicate = combinedSubsetPredicate(
          base: descriptor.predicate,
          cursorExpression: cursor.whereCurrent
        )
        let whereFromPredicate = combinedSubsetPredicate(
          base: descriptor.predicate,
          cursorExpression: cursor.whereFrom
        )

        let tiesSubset = try subsetRequest(
          predicate: whereCurrentPredicate,
          orderBy: descriptor.orderBy,
          limit: nil
        )
        let pageSubset = try subsetRequest(
          predicate: whereFromPredicate,
          orderBy: descriptor.orderBy,
          limit: descriptor.limit
        )

        let tiesMessages = try await fetchBufferedMessages(
          initialRequest: request.with(subset: tiesSubset),
          completionBoundary: .subsetEnd,
          stage: "request_snapshot_cursor_ties",
          syncMode: syncMode,
          protocolSemanticEpoch: protocolSemanticEpoch
        )
        let pageMessages = try await fetchBufferedMessages(
          initialRequest: request.with(subset: pageSubset),
          completionBoundary: .subsetEnd,
          stage: "request_snapshot_cursor_page",
          syncMode: syncMode,
          protocolSemanticEpoch: protocolSemanticEpoch
        )
        messages = tiesMessages + pageMessages
        fetchCallCount = 2
      } else {
        let subset = try subsetRequest(
          predicate: fetchDescriptor.predicate,
          orderBy: descriptor.orderBy,
          limit: descriptor.limit
        )
        let subsetRequest = request.with(subset: subset)
        messages = try await fetchBufferedMessages(
          initialRequest: subsetRequest,
          completionBoundary: .subsetEnd,
          stage: "request_snapshot",
          syncMode: syncMode,
          protocolSemanticEpoch: protocolSemanticEpoch
        )
        fetchCallCount = 1
      }

      span.setAttribute(key: "http_fetch_call_count", value: "\(fetchCallCount)")
      for (key, value) in electricMessageAttributes(messages) {
        span.setAttribute(key: key, value: value)
      }
      span.setAttribute(key: "result", value: "fetched")

      recordMessages(messages)

      return SyncBatch(
        collectionIdentifier: replicaIdentity.modelIdentifier,
        streamStateKey: streamStateKey,
        rollbackStreamStateKeys: resumedSyncState.rollbackStreamStateKeys,
        invalidationStreamStateKeys: resumedSyncState.invalidationStreamStateKeys,
        basePredicateHash: PredicateHash(from: basePredicate),
        fetchPredicate: ElectricFetchTracker.combinedCoveragePredicate(
          scope: basePredicate,
          requested: fetchDescriptor.predicate
        ),
        fetchDescriptor: fetchDescriptor,
        shouldRecordFetch: shouldRecordFetchMetadata && !recordAsObservation,
        shouldRecordObservation: recordAsObservation,
        shouldPersistSyncState: true,
        persistSyncStateOnlyAtTerminalBoundary: true,
        shouldUseMoveOutTombstones: false,
        shouldRemoveExpiredMoveOutTombstones: false,
        messages: messages,
        metadataProvider: metadataProvider,
        moveOutTracker: tracker,
        eventHandler: eventHandler,
        tracer: tracer,
        logger: logger,
        cursorOwnershipDiagnostics: cursorOwnershipDiagnostics,
        cursorWriterClientId: ObjectIdentifier(self),
        isRollbackDualWriteEnabled: isRollbackDualWriteEnabled,
        protocolCapabilityPolicy: protocolCapabilityPolicy,
        protocolSemanticEpoch: protocolSemanticEpoch,
        runtimeProvider: runtimeProvider,
        shapeTopology: effectiveShapeTopology,
        shapeTopologyLatch: shapeTopologyLatch
      )
    }
  }

  func latestSubsetObservation(
    table: String,
    basePredicate: SQLExpression?,
    descriptor: QueryDescriptor
  ) throws -> SubsetObservation? {
    let predicate = ElectricFetchTracker.combinedCoveragePredicate(
      scope: basePredicate,
      requested: descriptor.predicate
    )
    let metadataKey = ElectricFetchTracker.metadataKey(
      predicate: predicate,
      orderBy: descriptor.orderBy,
      limit: descriptor.limit,
      cursor: descriptor.cursor
    )
    return try metadataProvider.getLatestObservation(
      table: table,
      predicate: metadataKey.predicateHash,
      transaction: nil
    )
  }

  /// Fetches a progressive initial snapshot independently of owner resume
  /// metadata. Callers must generation-check the result before publication.
  public func fetchSnapshot<T>(
    _ type: T.Type,
    basePredicate: SQLExpression?,
    shapeTopology: ElectricShapeTopology = .dnf,
    descriptor: QueryDescriptor,
    syncMode: ElectricCollectionSyncMode,
    replicaIdentity: ElectricReplicaIdentity? = nil
  ) async throws -> SyncBatch<T>? where T: ElectricCollectionModel {
    let resolvedReplicaIdentity = try resolvedReplicaIdentity(
      for: T.self,
      basePredicate: basePredicate,
      provided: replicaIdentity
    )
    let spanAttributes: [String: String] = [
      "stage": "fetch_snapshot",
      "table": T.tableName,
      "collection": resolvedReplicaIdentity.modelIdentifier,
      "sync.mode": electricSyncModeLabel(syncMode),
      "has_base_predicate": "\(basePredicate != nil)",
      "has_cursor": "\(descriptor.cursor != nil)",
      "thread.is_main": electricThreadIsMainValue(),
    ]

    return try await withAsyncSpan(
      name: "electric.fetch_snapshot",
      attributes: spanAttributes
    ) { span in
      let protocolSemanticEpoch = protocolCapabilityPolicy.semanticEpoch()
      let replicaIdentity = resolvedReplicaIdentity
      let streamStateKey = persistedCursorKey(
        identity: replicaIdentity,
        syncMode: syncMode
      )
      let effectiveShapeTopology = effectiveShapeTopology(
        shapeTopology,
        streamStateKey: streamStateKey
      )
      let tracker = moveOutTracker(streamStateKey: streamStateKey)
      guard
        !requiresSemanticEpochReset(
          syncState: nil,
          tracker: tracker,
          semanticEpoch: protocolSemanticEpoch
        )
      else {
        throw ElectricSyncError.capabilitySemanticEpochTransitionDeferred
      }

      let initialOffset = initialOffsetForStream(syncMode: syncMode)

      var request = T.createIdentifiedShapeRequest(
        where: basePredicate,
        orderBy: [],
        limit: nil,
        offset: initialOffset,
        handle: nil,
        cursor: nil,
        live: false
      ).with(wireIdentity: replicaIdentity.wireIdentity)

      request = request.with(log: logModeFor(syncMode: syncMode))

      let messages: [ElectricMessage]
      let fetchCallCount: Int
      if let cursor = descriptor.cursor {
        let whereCurrentPredicate = combinedSubsetPredicate(
          base: descriptor.predicate,
          cursorExpression: cursor.whereCurrent
        )
        let whereFromPredicate = combinedSubsetPredicate(
          base: descriptor.predicate,
          cursorExpression: cursor.whereFrom
        )

        let tiesSubset = try subsetRequest(
          predicate: whereCurrentPredicate,
          orderBy: descriptor.orderBy,
          limit: nil
        )
        let pageSubset = try subsetRequest(
          predicate: whereFromPredicate,
          orderBy: descriptor.orderBy,
          limit: descriptor.limit
        )

        let tiesMessages = try await fetchBufferedMessages(
          initialRequest: request.with(subset: tiesSubset),
          completionBoundary: .subsetEnd,
          stage: "fetch_snapshot_cursor_ties",
          syncMode: syncMode,
          protocolSemanticEpoch: protocolSemanticEpoch
        )
        let pageMessages = try await fetchBufferedMessages(
          initialRequest: request.with(subset: pageSubset),
          completionBoundary: .subsetEnd,
          stage: "fetch_snapshot_cursor_page",
          syncMode: syncMode,
          protocolSemanticEpoch: protocolSemanticEpoch
        )
        messages = tiesMessages + pageMessages
        fetchCallCount = 2
      } else {
        let subset = try subsetRequest(
          predicate: descriptor.predicate,
          orderBy: descriptor.orderBy,
          limit: descriptor.limit
        )
        let subsetRequest = request.with(subset: subset)
        messages = try await fetchBufferedMessages(
          initialRequest: subsetRequest,
          completionBoundary: .subsetEnd,
          stage: "fetch_snapshot",
          syncMode: syncMode,
          protocolSemanticEpoch: protocolSemanticEpoch
        )
        fetchCallCount = 1
      }

      let containsSubsetRows = messages.contains { $0.isSubsetSnapshot && !$0.payload.isEmpty }

      span.setAttribute(key: "http_fetch_call_count", value: "\(fetchCallCount)")
      for (key, value) in electricMessageAttributes(messages) {
        span.setAttribute(key: key, value: value)
      }
      span.setAttribute(
        key: "result", value: containsSubsetRows ? "fetched" : "authoritative_empty")

      recordMessages(messages)

      return SyncBatch(
        collectionIdentifier: replicaIdentity.modelIdentifier,
        streamStateKey: streamStateKey,
        rollbackStreamStateKeys: [],
        basePredicateHash: PredicateHash(from: basePredicate),
        fetchPredicate: ElectricFetchTracker.combinedCoveragePredicate(
          scope: basePredicate,
          requested: descriptor.predicate
        ),
        fetchDescriptor: descriptor,
        shouldRecordFetch: false,
        shouldRecordObservation: false,
        shouldPersistSyncState: false,
        shouldUseMoveOutTombstones: false,
        shouldRemoveExpiredMoveOutTombstones: false,
        messages: messages,
        metadataProvider: metadataProvider,
        moveOutTracker: tracker,
        eventHandler: eventHandler,
        tracer: tracer,
        logger: logger,
        cursorOwnershipDiagnostics: cursorOwnershipDiagnostics,
        cursorWriterClientId: ObjectIdentifier(self),
        isRollbackDualWriteEnabled: isRollbackDualWriteEnabled,
        protocolCapabilityPolicy: protocolCapabilityPolicy,
        protocolSemanticEpoch: protocolSemanticEpoch,
        runtimeProvider: runtimeProvider,
        shapeTopology: effectiveShapeTopology,
        shapeTopologyLatch: shapeTopologyLatch
      )
    }
  }

  private func combinedSubsetPredicate(
    base: SQLExpression?,
    cursorExpression: SyncPredicateExpression
  ) -> SQLExpression {
    if let basePredicate = base?.predicate {
      return SQLExpression(predicate: .and([basePredicate, cursorExpression]))
    }
    return SQLExpression(predicate: cursorExpression)
  }

  public func pollStream<T>(
    _ type: T.Type,
    basePredicate: SQLExpression?,
    shapeTopology: ElectricShapeTopology = .dnf,
    syncMode: ElectricCollectionSyncMode,
    live: Bool,
    forceFullBootstrap: Bool = false,
    replicaIdentity: ElectricReplicaIdentity? = nil
  ) async throws -> SyncBatch<T>? where T: ElectricCollectionModel {
    let resolvedReplicaIdentity = try resolvedReplicaIdentity(
      for: T.self,
      basePredicate: basePredicate,
      provided: replicaIdentity
    )
    let spanAttributes: [String: String] = [
      "stage": "poll_stream",
      "table": T.tableName,
      "collection": resolvedReplicaIdentity.modelIdentifier,
      "sync.mode": electricSyncModeLabel(syncMode),
      "transport": "http_poll",
      "live_requested": "\(live)",
      "force_full_bootstrap": "\(forceFullBootstrap)",
      "has_base_predicate": "\(basePredicate != nil)",
      "thread.is_main": electricThreadIsMainValue(),
    ]

    return try await withAsyncSpan(
      name: "electric.poll_stream",
      attributes: spanAttributes
    ) { span in
      let protocolSemanticEpoch = protocolCapabilityPolicy.semanticEpoch()
      let replicaIdentity = resolvedReplicaIdentity
      let streamStateKey = persistedCursorKey(
        identity: replicaIdentity,
        syncMode: syncMode
      )
      let effectiveShapeTopology = effectiveShapeTopology(
        shapeTopology,
        streamStateKey: streamStateKey
      )
      if forceFullBootstrap {
        // A forced full bootstrap discards incremental resume; the stale
        // process-local membership tracker must never survive into the
        // replacement generation.
        moveOutTrackers[streamStateKey] = makeMoveOutTracker()
      }
      let resumedSyncState = try resumeSyncState(
        identity: replicaIdentity,
        syncMode: syncMode
      )
      span.setAttribute(key: "resume.source", value: resumedSyncState.source.rawValue)
      let tracker = moveOutTracker(streamStateKey: streamStateKey)
      if !forceFullBootstrap {
        _ = try rebuildSimpleTrackerIfAdmissible(
          T.self,
          identity: replicaIdentity,
          resumedState: resumedSyncState,
          shapeTopology: effectiveShapeTopology,
          syncMode: syncMode,
          tracker: tracker,
          semanticEpoch: protocolSemanticEpoch
        )
      }
      if requiresSemanticEpochReset(
        syncState: resumedSyncState.persistedState,
        tracker: tracker,
        semanticEpoch: protocolSemanticEpoch
      ) {
        span.setAttribute(key: "result", value: "semantic_epoch_reset")
        return SyncBatch(
          collectionIdentifier: replicaIdentity.modelIdentifier,
          streamStateKey: streamStateKey,
          rollbackStreamStateKeys: resumedSyncState.rollbackStreamStateKeys,
          invalidationStreamStateKeys: resumedSyncState.invalidationStreamStateKeys,
          basePredicateHash: PredicateHash(from: basePredicate),
          fetchPredicate: nil,
          fetchDescriptor: nil,
          shouldRecordFetch: false,
          shouldRecordObservation: false,
          shouldPersistSyncState: true,
          messages: [],
          metadataProvider: metadataProvider,
          moveOutTracker: tracker,
          eventHandler: eventHandler,
          tracer: tracer,
          logger: logger,
          cursorOwnershipDiagnostics: cursorOwnershipDiagnostics,
          cursorWriterClientId: ObjectIdentifier(self),
          isRollbackDualWriteEnabled: isRollbackDualWriteEnabled,
          protocolCapabilityPolicy: protocolCapabilityPolicy,
          protocolSemanticEpoch: protocolSemanticEpoch,
          runtimeProvider: runtimeProvider,
          requiresSemanticEpochReset: true,
          shapeTopology: effectiveShapeTopology,
          shapeTopologyLatch: shapeTopologyLatch
        )
      }
      // `requiresFullBootstrapForTrackerContinuity` converts a pristine-admission
      // provider failure into this forced recovery. Do not re-run that optional
      // admission while preparing the recovery request, or the same provider
      // error could escape and kill the owner instead of fetching its baseline.
      let admitsFreshOnDemandStaticSimple =
        if forceFullBootstrap {
          false
        } else {
          try admitsFreshOnDemandStaticSimple(
            T.self,
            resumedState: resumedSyncState,
            syncMode: syncMode,
            shapeTopology: effectiveShapeTopology
          )
        }
      let syncState =
        forceFullBootstrap || admitsFreshOnDemandStaticSimple
        ? nil
        : resumedSyncState.state

      let initialOffset = initialOffsetForStream(syncMode: syncMode)
      let hasValidOffset: Bool = {
        if let offset = syncState?.offset { return offset != "-1" }
        if let offset = initialOffset { return offset != "-1" }
        return true
      }()
      let liveAllowed = live && hasValidOffset && (syncState?.isUpToDate ?? false)
      span.setAttribute(key: "live_allowed", value: "\(liveAllowed)")

      let requestOffset = forceFullBootstrap ? "-1" : syncState?.offset ?? initialOffset
      var requestHandle = syncState?.handle

      if requestOffset == "-1" {
        requestHandle = cacheBustingHandle(base: requestHandle)
      }

      var request = T.createIdentifiedShapeRequest(
        where: basePredicate,
        orderBy: [],
        limit: nil,
        offset: requestOffset,
        handle: requestHandle,
        cursor: syncState?.cursor,
        live: liveAllowed
      ).with(wireIdentity: replicaIdentity.wireIdentity)

      // This client never requests Electric experimental_compaction: rebuilding
      // continuity depends on the complete, ordered tag protocol.
      request = request.with(
        log: logModeFor(syncMode: syncMode, forceFullBootstrap: forceFullBootstrap)
      )

      let messages = try await fetchBufferedMessages(
        initialRequest: request,
        completionBoundary: .upToDate,
        stage: "poll_stream",
        syncMode: syncMode,
        protocolSemanticEpoch: protocolSemanticEpoch
      )

      for (key, value) in electricMessageAttributes(messages) {
        span.setAttribute(key: key, value: value)
      }

      recordMessages(messages)

      return SyncBatch(
        collectionIdentifier: replicaIdentity.modelIdentifier,
        streamStateKey: streamStateKey,
        rollbackStreamStateKeys: resumedSyncState.rollbackStreamStateKeys,
        invalidationStreamStateKeys: resumedSyncState.invalidationStreamStateKeys,
        basePredicateHash: PredicateHash(from: basePredicate),
        fetchPredicate: nil,
        fetchDescriptor: nil,
        shouldRecordFetch: false,
        shouldRecordObservation: false,
        shouldPersistSyncState: true,
        messages: messages,
        metadataProvider: metadataProvider,
        moveOutTracker: tracker,
        eventHandler: eventHandler,
        tracer: tracer,
        logger: logger,
        cursorOwnershipDiagnostics: cursorOwnershipDiagnostics,
        cursorWriterClientId: ObjectIdentifier(self),
        isRollbackDualWriteEnabled: isRollbackDualWriteEnabled,
        protocolCapabilityPolicy: protocolCapabilityPolicy,
        protocolSemanticEpoch: protocolSemanticEpoch,
        runtimeProvider: runtimeProvider,
        shapeTopology: effectiveShapeTopology,
        shapeTopologyLatch: shapeTopologyLatch
      )
    }
  }

  /// Open a long-lived stream (e.g. SSE) for live updates and yield SyncBatch items when an
  /// up-to-date boundary is received. Returns nil if live streaming is not currently allowed.
  ///
  /// This is intentionally separate from `pollStream` since SSE is an infinite stream.
  public func liveBatchStream<T>(
    _ type: T.Type,
    basePredicate: SQLExpression?,
    shapeTopology: ElectricShapeTopology = .dnf,
    syncMode: ElectricCollectionSyncMode,
    replicaIdentity: ElectricReplicaIdentity? = nil,
    streamController: ElectricLiveStreamController? = nil
  ) async throws -> AsyncThrowingStream<SyncBatch<T>, Error>? where T: ElectricCollectionModel {
    let resolvedReplicaIdentity = try resolvedReplicaIdentity(
      for: T.self,
      basePredicate: basePredicate,
      provided: replicaIdentity
    )
    let connectAttributes: [String: String] = [
      "stage": "live_stream_connect",
      "table": T.tableName,
      "collection": resolvedReplicaIdentity.modelIdentifier,
      "sync.mode": electricSyncModeLabel(syncMode),
      "transport": "sse",
      "has_base_predicate": "\(basePredicate != nil)",
      "thread.is_main": electricThreadIsMainValue(),
    ]

    return try await withAsyncSpan(
      name: "electric.live_stream.connect",
      attributes: connectAttributes
    ) { connectSpan in
      let connectSemanticEpoch = protocolCapabilityPolicy.semanticEpoch()
      guard let httpStreamClient else {
        throw ElectricSyncError.fetchFailed("No HTTP stream client configured")
      }

      let replicaIdentity = resolvedReplicaIdentity
      let streamStateKey = persistedCursorKey(
        identity: replicaIdentity,
        syncMode: syncMode
      )
      let effectiveShapeTopology = effectiveShapeTopology(
        shapeTopology,
        streamStateKey: streamStateKey
      )
      let resumedSyncState = try resumeSyncState(
        identity: replicaIdentity,
        syncMode: syncMode
      )
      connectSpan.setAttribute(key: "resume.source", value: resumedSyncState.source.rawValue)
      let syncState = resumedSyncState.state
      let tracker = moveOutTracker(streamStateKey: streamStateKey)
      _ = try rebuildSimpleTrackerIfAdmissible(
        T.self,
        identity: replicaIdentity,
        resumedState: resumedSyncState,
        shapeTopology: effectiveShapeTopology,
        syncMode: syncMode,
        tracker: tracker,
        semanticEpoch: connectSemanticEpoch
      )
      guard
        !requiresSemanticEpochReset(
          syncState: resumedSyncState.persistedState,
          tracker: tracker,
          semanticEpoch: connectSemanticEpoch
        )
      else {
        connectSpan.setAttribute(key: "result", value: "semantic_epoch_reset_required")
        return nil
      }

      let initialOffset = initialOffsetForStream(syncMode: syncMode)
      let hasValidOffset: Bool = {
        if let offset = syncState?.offset { return offset != "-1" }
        if let offset = initialOffset { return offset != "-1" }
        return true
      }()
      let liveAllowed = hasValidOffset && (syncState?.isUpToDate ?? false)
      connectSpan.setAttribute(key: "live_allowed", value: "\(liveAllowed)")
      guard liveAllowed else {
        connectSpan.setAttribute(key: "result", value: "not_allowed")
        return nil
      }

      let requestOffset = syncState?.offset ?? initialOffset
      let requestHandle = syncState?.handle

      var request = T.createIdentifiedShapeRequest(
        where: basePredicate,
        orderBy: [],
        limit: nil,
        offset: requestOffset,
        handle: requestHandle,
        cursor: syncState?.cursor,
        live: true
      ).with(wireIdentity: replicaIdentity.wireIdentity)
      request = request.with(log: logModeFor(syncMode: syncMode))

      let messageStream: AsyncThrowingStream<ElectricMessage, Error>
      do {
        messageStream = try await httpStreamClient.stream(request)
      } catch {
        throw quarantiningProtocolError(error)
      }
      connectSpan.setAttribute(key: "result", value: "connected")

      return AsyncThrowingStream { continuation in
        let metadataProvider = self.metadataProvider
        let eventHandler = self.eventHandler
        let streamStateKey = streamStateKey
        let maxBufferedMessagesPerSync = self.maxBufferedMessagesPerSync
        let tracer = self.tracer
        let cursorOwnershipDiagnostics = self.cursorOwnershipDiagnostics
        let cursorWriterClientId = ObjectIdentifier(self)
        let isRollbackDualWriteEnabled = self.isRollbackDualWriteEnabled
        let shapeTopology = effectiveShapeTopology
        let shapeTopologyLatch = self.shapeTopologyLatch
        let syncModeLabel = electricSyncModeLabel(syncMode)

        let task = Task { [weak self] in
          guard let self else { return }

          var buffered: [ElectricMessage] = []
          var batchIndex = 0

          func yieldBatch(_ messages: [ElectricMessage]) async throws {
            guard !messages.isEmpty else { return }
            batchIndex += 1
            let protocolSemanticEpoch = protocolCapabilityPolicy.semanticEpoch()

            let attributes = mergeTraceAttributes(
              [
                "stage": "live_stream_batch",
                "table": T.tableName,
                "collection": replicaIdentity.modelIdentifier,
                "sync.mode": syncModeLabel,
                "transport": "sse",
                "batch.index": "\(batchIndex)",
                "thread.is_main": electricThreadIsMainValue(),
              ],
              electricMessageAttributes(messages)
            )
            let batchSpan = tracer.startSpan(
              name: "electric.live_stream.batch",
              attributes: attributes
            )
            await self.recordMessages(messages)
            let batch = SyncBatch<T>(
              collectionIdentifier: replicaIdentity.modelIdentifier,
              streamStateKey: streamStateKey,
              rollbackStreamStateKeys: resumedSyncState.rollbackStreamStateKeys,
              invalidationStreamStateKeys: resumedSyncState.invalidationStreamStateKeys,
              basePredicateHash: PredicateHash(from: basePredicate),
              fetchPredicate: nil,
              fetchDescriptor: nil,
              shouldRecordFetch: false,
              shouldRecordObservation: false,
              shouldPersistSyncState: true,
              messages: messages,
              metadataProvider: metadataProvider,
              moveOutTracker: tracker,
              eventHandler: eventHandler,
              tracer: tracer,
              logger: logger,
              cursorOwnershipDiagnostics: cursorOwnershipDiagnostics,
              cursorWriterClientId: cursorWriterClientId,
              isRollbackDualWriteEnabled: isRollbackDualWriteEnabled,
              protocolCapabilityPolicy: protocolCapabilityPolicy,
              protocolSemanticEpoch: protocolSemanticEpoch,
              runtimeProvider: runtimeProvider,
              shapeTopology: shapeTopology,
              shapeTopologyLatch: shapeTopologyLatch
            )
            try batch.preflightSupportedEvents()
            continuation.yield(batch)
            batchSpan.end(status: .success)
          }

          do {
            for try await message in messageStream {
              if Task.isCancelled { break }

              buffered.append(message)
              if buffered.count > maxBufferedMessagesPerSync {
                throw ElectricSyncError.fetchFailed(
                  "Electric SSE stream exceeded max buffered messages (\(maxBufferedMessagesPerSync))"
                )
              }

              if message.kind == .truncate || message.control == .mustRefetch {
                let batch = buffered
                buffered.removeAll(keepingCapacity: true)
                try await yieldBatch(batch)
                continue
              }

              if Self.isUpToDateControl(message) {
                let batch = buffered
                buffered.removeAll(keepingCapacity: true)
                try await yieldBatch(batch)
              }
            }

            continuation.finish()
          } catch {
            continuation.finish(throwing: self.quarantiningProtocolError(error))
          }
        }
        streamController?.install(task: task)

        continuation.onTermination = { _ in
          task.cancel()
        }
      }
    }
  }

  /// Manually mark a predicate as fetched/complete so `ElectricFetchTracker` can
  /// skip future remote loads (useful for migrations/backfills).
  public func markFetched<T: ElectricCollectionModel>(
    _ type: T.Type,
    where predicate: SQLExpression?,
    collectionIdentifier _: String? = nil
  ) async throws {
    let predicateHash = PredicateHash(from: predicate)
    let predicateJSON = predicate?.encodedPredicateJSON() ?? predicate?.normalized()

    try metadataProvider.recordFetch(
      table: type.tableName,
      predicate: predicateHash,
      predicateJSON: predicateJSON,
      snapshotBoundary: nil,
      outcome: .present,
      isComplete: true,
      transaction: nil
    )
  }

  internal static func syncStateKey(
    collectionIdentifier: String,
    predicate: SQLExpression?
  ) -> String {
    let predicateHash = PredicateHash(from: predicate)
    return syncStateKey(collectionIdentifier: collectionIdentifier, predicateHash: predicateHash)
  }

  internal static func syncStateKey(
    collectionIdentifier: String,
    predicateHash: PredicateHash
  ) -> String {
    "\(collectionIdentifier)|\(predicateHash.value)"
  }

  /// Provide a stable cache buster for initial offset=-1 requests so proxies/browsers
  /// don't serve a stale initial snapshot/handle. Electric ignores the handle for
  /// offset=-1, but caches include it in the key.
  private func cacheBustingHandle(base: String?) -> String {
    if let base {
      return "\(base)-next"
    }
    return "cachebust-\(runtimeProvider.makeUUID().uuidString)"
  }

  private enum BufferedFetchCompletionBoundary: String {
    case subsetEnd = "subset_end"
    case upToDate = "up_to_date"

    func isPresent(in messages: [ElectricMessage]) -> Bool {
      switch self {
      case .subsetEnd:
        messages.contains { $0.control == .subsetEnd }
      case .upToDate:
        messages.contains(where: ElectricSyncClientImpl.isUpToDateControl)
      }
    }
  }

  private func fetchBufferedMessages(
    initialRequest: ElectricShapeRequest,
    completionBoundary: BufferedFetchCompletionBoundary,
    stage: String,
    syncMode: ElectricCollectionSyncMode,
    protocolSemanticEpoch: ElectricProtocolSemanticEpoch
  ) async throws -> [ElectricMessage] {
    let fetchAttributes: [String: String] = [
      "stage": stage,
      "table": initialRequest.table,
      "transport": "http_poll",
      "sync.mode": electricSyncModeLabel(syncMode),
      "completion_boundary": completionBoundary.rawValue,
      "request.live": "\(initialRequest.live)",
      "request.has_subset": "\(initialRequest.subset != nil)",
      "request.has_predicate": "\(initialRequest.predicate != nil)",
      "thread.is_main": electricThreadIsMainValue(),
    ]

    return try await withAsyncSpan(
      name: "electric.fetch_buffered_messages",
      attributes: fetchAttributes
    ) { span in
      var buffered: [ElectricMessage] = []
      var requestOffset = initialRequest.offset
      var requestHandle = initialRequest.handle
      var requestCursor = initialRequest.cursor

      var hasReachedBoundary = false
      var attemptsUsed = 0
      var fetchedMessageCount = 0

      for attempt in 0..<maxHTTPFetchesPerSync {
        attemptsUsed = attempt + 1
        let request = initialRequest.updating(
          offset: requestOffset,
          handle: requestHandle,
          cursor: requestCursor
        )

        let attemptAttributes: [String: String] = [
          "stage": stage,
          "table": initialRequest.table,
          "transport": "http_poll",
          "sync.mode": electricSyncModeLabel(syncMode),
          "attempt.index": "\(attempt + 1)",
          "attempt.max": "\(maxHTTPFetchesPerSync)",
          "completion_boundary": completionBoundary.rawValue,
          "thread.is_main": electricThreadIsMainValue(),
        ]

        let page = try await withAsyncSpan(
          name: "electric.fetch_buffered_messages.attempt",
          attributes: attemptAttributes
        ) { attemptSpan in
          let page: [ElectricMessage]
          do {
            page = try await httpClient.fetch(request)
          } catch {
            throw quarantiningProtocolError(error)
          }
          try preflightProtocolMessages(page, semanticEpoch: protocolSemanticEpoch)
          for (key, value) in electricMessageAttributes(page) {
            attemptSpan.setAttribute(key: key, value: value)
          }
          return page
        }
        guard !page.isEmpty else {
          throw ElectricSyncError.fetchFailed(
            "Electric fetch returned no messages before reaching a safe boundary"
          )
        }

        fetchedMessageCount += page.count
        if fetchedMessageCount > maxBufferedMessagesPerSync {
          throw ElectricSyncError.fetchFailed(
            "Electric fetch exceeded max buffered messages (\(maxBufferedMessagesPerSync))"
          )
        }

        buffered.append(contentsOf: page)

        let hasTruncate = page.contains(where: Self.isStreamReset)
        if hasTruncate {
          span.setAttribute(key: "attempt.count", value: "\(attemptsUsed)")
          span.setAttribute(key: "message.fetched.count", value: "\(fetchedMessageCount)")
          span.setAttribute(key: "boundary.kind", value: "truncate")
          for (key, value) in electricMessageAttributes(buffered) {
            span.setAttribute(key: key, value: value)
          }
          return buffered
        }

        let boundaryInPage = completionBoundary.isPresent(in: page)

        if boundaryInPage {
          hasReachedBoundary = true
          span.setAttribute(
            key: "boundary.kind",
            value: completionBoundary.rawValue
          )
          break
        }

        let lastOffset = page.reversed().compactMap { $0.offset }.first
        let lastHandle = page.reversed().compactMap { $0.handle }.first
        let lastCursor = page.reversed().compactMap { $0.cursor }.first

        let nextOffset = lastOffset ?? requestOffset
        let nextHandle = lastHandle ?? requestHandle
        let nextCursor = lastCursor ?? requestCursor

        if attempt > 0,
          nextOffset == requestOffset,
          nextHandle == requestHandle,
          nextCursor == requestCursor
        {
          throw ElectricSyncError.fetchFailed(
            "Electric fetch did not advance offset/handle/cursor before reaching a safe boundary"
          )
        }

        requestOffset = nextOffset
        requestHandle = nextHandle
        requestCursor = nextCursor
      }

      guard hasReachedBoundary else {
        throw ElectricSyncError.fetchFailed(
          "Electric fetch did not reach a safe boundary within \(maxHTTPFetchesPerSync) fetches"
        )
      }

      span.setAttribute(key: "attempt.count", value: "\(attemptsUsed)")
      span.setAttribute(key: "message.fetched.count", value: "\(fetchedMessageCount)")
      for (key, value) in electricMessageAttributes(buffered) {
        span.setAttribute(key: key, value: value)
      }
      return buffered
    }
  }

  nonisolated func protocolQuarantine(for error: Error) -> ElectricProtocolQuarantine? {
    protocolCapabilityPolicy.quarantine(for: error)
  }

  private nonisolated func preflightProtocolMessages(
    _ messages: [ElectricMessage],
    semanticEpoch: ElectricProtocolSemanticEpoch
  ) throws {
    if let quarantine = protocolCapabilityPolicy.quarantine(
      for: messages,
      semanticEpoch: semanticEpoch
    ) {
      throw ElectricSyncError.protocolQuarantined(quarantine)
    }
  }

  private nonisolated func quarantiningProtocolError(_ error: Error) -> Error {
    guard let quarantine = protocolQuarantine(for: error) else { return error }
    return ElectricSyncError.protocolQuarantined(quarantine)
  }

  private nonisolated static func isStreamReset(_ message: ElectricMessage) -> Bool {
    message.kind == .truncate || message.control == .mustRefetch
  }

  private nonisolated static func isUpToDateControl(_ message: ElectricMessage) -> Bool {
    if let control = message.control {
      return control == .upToDate
    }
    return message.isUpToDate
  }

  private func initialOffsetForStream(syncMode: ElectricCollectionSyncMode) -> String? {
    switch syncMode {
    case .onDemand:
      return "now"
    case .eager, .progressive:
      return nil
    }
  }

  private func logModeFor(
    syncMode: ElectricCollectionSyncMode,
    forceFullBootstrap: Bool = false
  ) -> ElectricLogMode? {
    // `changes_only` at `offset=-1` does not supply the baseline row keys an
    // authoritative replacement needs. Keep the on-demand tail incremental,
    // but let a forced bootstrap request Electric's normal full snapshot.
    guard !forceFullBootstrap else { return nil }
    switch syncMode {
    case .onDemand:
      return .changesOnly
    case .eager, .progressive:
      return nil
    }
  }

  private func shouldRecordFetchMetadata(
    syncMode: ElectricCollectionSyncMode,
    syncState: SyncState?,
    descriptor: QueryDescriptor
  ) -> Bool {
    // Progressive subset queries (with limit) must not be cached as complete
    // because the subset composition changes when new rows arrive. The stream
    // keeps the full shape up to date; subset snapshots must re-evaluate
    // against the latest stream offset on every query.
    if syncMode == .progressive, descriptor.limit != nil {
      return false
    }
    // An initial on-demand unscoped load starts a live tail from "now"; it is not
    // a complete backfill and must not satisfy later full-fetch checks.
    guard syncMode == .onDemand else { return true }
    guard syncState == nil else { return true }
    guard descriptor.cursor == nil else { return true }
    guard descriptor.predicate == nil else { return true }
    guard descriptor.orderBy.isEmpty else { return true }
    guard descriptor.limit == nil else { return true }
    return false
  }

  private func subsetRequest(
    predicate: SQLExpression?,
    orderBy: [OrderBy],
    limit: Int?
  ) throws -> ElectricSubsetRequest {
    let compiler = SubsetSQLCompiler()

    let whereClause: String = try {
      if let structured = predicate?.predicate {
        let compilation = try compiler.compile(structured)
        return compilation.whereClause
      }
      return "TRUE"
    }()

    let paramsJSON: String? = try {
      guard let structured = predicate?.predicate else { return nil }
      let compilation = try compiler.compile(structured)
      return try compilation.encodedParamsJSON()
    }()

    let orderByClause: String? = try {
      guard !orderBy.isEmpty else { return nil }
      return try compiler.compileOrderBy(orderBy)
    }()

    return ElectricSubsetRequest(
      whereClause: whereClause,
      paramsJSON: paramsJSON,
      orderByClause: orderByClause,
      limit: limit,
      offset: nil
    )
  }

  internal nonisolated static func streamStateKey<T: ElectricCollectionModel>(
    for type: T.Type,
    basePredicate: SQLExpression?,
    modelIdentifier: String? = nil,
    wireIdentity: ElectricShapeWireIdentity? = nil
  ) -> String {
    ElectricReplicaIdentity(
      modelType: type,
      modelIdentifier: modelIdentifier ?? type.collectionIdentifier,
      basePredicate: basePredicate,
      wireIdentity: wireIdentity
    ).persistedCursorKey
  }

  nonisolated func persistedCursorKey(
    identity: ElectricReplicaIdentity,
    syncMode: ElectricCollectionSyncMode
  ) -> String {
    if isExactCursorCutoverEnabled {
      return identity.persistedCursorKey
    }
    return identity.legacyPersistedCursorKey(syncMode: syncMode)
  }

  private func resolvedReplicaIdentity<T: ElectricCollectionModel>(
    for type: T.Type,
    basePredicate: SQLExpression?,
    provided: ElectricReplicaIdentity?
  ) throws -> ElectricReplicaIdentity {
    guard let provided else {
      return ElectricReplicaIdentity(
        modelType: type,
        modelIdentifier: type.collectionIdentifier,
        basePredicate: basePredicate
      )
    }

    let expected = ElectricReplicaIdentity(
      modelType: type,
      modelIdentifier: provided.modelIdentifier,
      basePredicate: basePredicate,
      replicaMode: provided.replicaMode,
      shapeDefinitionVersion: provided.shapeDefinitionVersion
    )
    guard expected == provided else {
      throw ElectricSyncError.fetchFailed(
        "Replica identity does not match the requested model/base shape"
      )
    }
    return provided
  }

  private func exactSyncState(identity: ElectricReplicaIdentity) throws -> SyncState? {
    try metadataProvider.getSyncState(
      collectionId: identity.persistedCursorKey,
      transaction: nil
    )
  }

  private func legacyCursorCount(identity: ElectricReplicaIdentity) throws -> Int {
    try identity.legacyPersistedCursorKeys.reduce(into: 0) { count, key in
      if try metadataProvider.getSyncState(collectionId: key, transaction: nil)?
        .canResumeWithoutFullBootstrap == true
      {
        count += 1
      }
    }
  }

  private func legacyCursorEvidenceCount(identity: ElectricReplicaIdentity) throws -> Int {
    try identity.legacyPersistedCursorKeys.reduce(into: 0) { count, key in
      if try metadataProvider.getSyncState(collectionId: key, transaction: nil) != nil {
        count += 1
      }
    }
  }

  private func provenLegacyResumeState(identity: ElectricReplicaIdentity) throws -> SyncState? {
    guard !identity.provenLegacyPersistedCursorKeys.isEmpty else { return nil }
    let states = try identity.provenLegacyPersistedCursorKeys.compactMap { key in
      try metadataProvider.getSyncState(collectionId: key, transaction: nil)
    }
    guard let first = states.first, first.canResumeWithoutFullBootstrap else { return nil }
    guard states.dropFirst().allSatisfy({ $0.hasSameResumeIdentity(as: first) }) else {
      return nil
    }
    return first
  }

  private func rollbackStreamStateKeys(
    identity: ElectricReplicaIdentity,
    matching exactState: SyncState
  ) throws -> [String] {
    guard !identity.provenLegacyPersistedCursorKeys.isEmpty else { return [] }
    let keyedStates = try identity.provenLegacyPersistedCursorKeys.compactMap { key in
      try metadataProvider.getSyncState(collectionId: key, transaction: nil).map { (key, $0) }
    }
    guard let first = keyedStates.first?.1, first.hasSameResumeIdentity(as: exactState) else {
      return []
    }
    guard keyedStates.dropFirst().allSatisfy({ $0.1.hasSameResumeIdentity(as: first) }) else {
      return []
    }
    return keyedStates.map(\.0)
  }

  private func invalidationStreamStateKeys(identity: ElectricReplicaIdentity) throws -> [String] {
    try identity.provenLegacyPersistedCursorKeys.filter {
      try metadataProvider.getSyncState(collectionId: $0, transaction: nil) != nil
    }
  }

  private func resumeSyncState(
    identity: ElectricReplicaIdentity,
    syncMode: ElectricCollectionSyncMode
  ) throws -> ResumedSyncState {
    guard isExactCursorCutoverEnabled else {
      let state = try metadataProvider.getSyncState(
        collectionId: identity.legacyPersistedCursorKey(syncMode: syncMode),
        transaction: nil
      )
      // A bridge attestation upgrades the classification: the state under this
      // mode's key was atomically migrated from another compatible mode by the
      // application, so the simple tracker rebuild may treat it like exact
      // continuity for statically simple shapes.
      let source: ResumeSource = state?.bridgedFromSyncMode != nil ? .legacyBridged : .legacyPreCutover
      return ResumedSyncState(
        state: state,
        source: source,
        rollbackStreamStateKeys: [],
        invalidationStreamStateKeys: []
      )
    }

    if let state = try exactSyncState(identity: identity) {
      return ResumedSyncState(
        state: state,
        source: .exact,
        rollbackStreamStateKeys: try rollbackStreamStateKeys(
          identity: identity,
          matching: state
        ),
        invalidationStreamStateKeys: try invalidationStreamStateKeys(identity: identity)
      )
    }

    if !identity.provenLegacyPersistedCursorKeys.isEmpty,
      let adopted = try metadataProvider.adoptSyncState(
        collectionId: identity.persistedCursorKey,
        legacyCollectionIds: identity.provenLegacyPersistedCursorKeys,
        transaction: nil
      )
    {
      return ResumedSyncState(
        state: adopted,
        source: .legacyAdopted,
        rollbackStreamStateKeys: try rollbackStreamStateKeys(
          identity: identity,
          matching: adopted
        ),
        invalidationStreamStateKeys: try invalidationStreamStateKeys(identity: identity)
      )
    }

    let legacyCursorCount = try legacyCursorCount(identity: identity)
    let source: ResumeSource
    if legacyCursorCount > 0 {
      guard ElectricLegacyBootstrapScope.admission?.identity == identity else {
        throw ElectricSyncError.legacyExactMissBootstrapDisabled
      }
      source = .legacyExactMiss
    } else {
      source = .fresh
    }

    // A conflicting or unproven legacy state is detection-only. Both fresh
    // installs and an explicitly enabled exact-key miss start at offset -1.
    return ResumedSyncState(
      state: SyncState(
        offset: "-1",
        handle: nil,
        cursor: nil,
        isUpToDate: false,
        lastSyncedAt: nil
      ),
      source: source,
      rollbackStreamStateKeys: [],
      invalidationStreamStateKeys: []
    )
  }

  private func recordMessages(_ messages: [ElectricMessage]) {
    currentBatchMessages = messages
    ElectricLegacyBootstrapScope.admission?.recordDecodedPayloadBytes(
      messages.reduce(0) { $0 + $1.payload.count }
    )

    if messages.contains(where: Self.isStreamReset) {
      seenTxids.removeAll(keepingCapacity: true)
      seenSnapshots.removeAll(keepingCapacity: true)
    }

    for message in messages {
      if let txids = message.txids {
        seenTxids.formUnion(txids)
      }
      if let snapshot = message.postgresSnapshot {
        seenSnapshots.append(snapshot)
      }
    }

    resolveTxidWaitersIfPossible()

    for (id, waiter) in pendingMatchWaiters {
      if waiter.matched { continue }
      var updated = waiter
      updated.matched = messages.contains(where: updated.match)
      pendingMatchWaiters[id] = updated
    }

    if messages.contains(where: Self.isUpToDateControl) {
      resolveMatchedWaiters()
    }
  }

  private func resolveTxidWaitersIfPossible() {
    var resolved: [UUID] = []
    for (id, waiter) in pendingTxidWaiters {
      if seenTxids.contains(waiter.txid) || isVisibleInAnySnapshot(waiter.txid) {
        waiter.timeoutTask.cancel()
        waiter.continuation.resume(returning: true)
        resolved.append(id)
      }
    }
    for id in resolved {
      pendingTxidWaiters.removeValue(forKey: id)
    }
  }

  private func resolveMatchedWaiters() {
    var resolved: [UUID] = []
    for (id, waiter) in pendingMatchWaiters where waiter.matched {
      waiter.timeoutTask.cancel()
      waiter.continuation.resume(returning: true)
      resolved.append(id)
    }
    for id in resolved {
      pendingMatchWaiters.removeValue(forKey: id)
    }
  }

  private func timeoutTxidWaiter(id: UUID) async {
    guard let waiter = pendingTxidWaiters.removeValue(forKey: id) else { return }
    waiter.continuation.resume(throwing: ElectricSyncAwaitError.timeoutWaitingForTxId(waiter.txid))
  }

  private func timeoutMatchWaiter(id: UUID) async {
    guard let waiter = pendingMatchWaiters.removeValue(forKey: id) else { return }
    waiter.continuation.resume(throwing: ElectricSyncAwaitError.timeoutWaitingForMatch)
  }

  private func isVisibleInAnySnapshot(_ txid: Int64) -> Bool {
    for snapshot in seenSnapshots {
      if snapshot.isVisible(transactionId: txid) { return true }
    }
    return false
  }
}
