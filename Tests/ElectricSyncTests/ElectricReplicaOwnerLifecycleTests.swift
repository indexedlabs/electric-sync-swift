import Foundation
import Testing

@testable import ElectricSync

struct ElectricReplicaOwnerLifecycleTests {

  // MARK: - Exact replica identity

  private func identity<Model: ElectricCollectionModel>(
    _ modelType: Model.Type,
    replicaMode: ElectricReplicaMode = .default,
    shapeDefinitionVersion: String? = nil
  ) -> ElectricReplicaIdentity {
    ElectricReplicaIdentity(
      modelType: modelType,
      modelIdentifier: Model.collectionIdentifier,
      basePredicate: nil,
      replicaMode: replicaMode,
      shapeDefinitionVersion: shapeDefinitionVersion
    )
  }

  @Test
  func defaultReplicaModeAndShapeDefinitionVersionKeepFrozenV2Identity() {
    let implicitDefaults = ElectricReplicaIdentity(
      modelType: ReplicaTestRecord.self,
      modelIdentifier: ReplicaTestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let explicitDefaults = identity(
      ReplicaTestRecord.self,
      replicaMode: .default,
      shapeDefinitionVersion: "1"
    )

    #expect(
      implicitDefaults.serializedShapeIdentity
        == "1:226:replica_owner_test_records35:ElectricSyncTests.ReplicaTestRecord34:/shapes/replica_owner_test_records10:2:id4:name0:26:replica_owner_test_records3:all0:3:nil3:nil3:nil"
    )
    #expect(implicitDefaults == explicitDefaults)
    #expect(implicitDefaults.persistedCursorKey == explicitDefaults.persistedCursorKey)
    #expect(!implicitDefaults.serializedShapeIdentity.contains("replica:"))
    #expect(!implicitDefaults.serializedShapeIdentity.contains("shape-definition:"))
    #expect(implicitDefaults.replicaMode == .default)
    #expect(implicitDefaults.shapeDefinitionVersion == "1")
  }

  @Test
  func replicaModeAndShapeDefinitionVersionIsolateReplicaIdentity() {
    let defaultIdentity = identity(ReplicaTestRecord.self)
    let fullIdentity = identity(ReplicaTestRecord.self, replicaMode: .full)
    let versionedIdentity = identity(ReplicaTestRecord.self, shapeDefinitionVersion: "2")

    #expect(fullIdentity != defaultIdentity)
    #expect(versionedIdentity != defaultIdentity)
    #expect(versionedIdentity != fullIdentity)
    #expect(fullIdentity.persistedCursorKey != defaultIdentity.persistedCursorKey)
    #expect(versionedIdentity.persistedCursorKey != defaultIdentity.persistedCursorKey)
    #expect(fullIdentity.serializedShapeIdentity.contains("replica:full"))
    #expect(versionedIdentity.serializedShapeIdentity.contains("shape-definition:2"))
  }

  @Test
  func nonDefaultReplicaModeNeverAdoptsProvenLegacyCursors() {
    #expect(!identity(LegacyMappedTestRecord.self).provenLegacyPersistedCursorKeys.isEmpty)
    #expect(
      identity(LegacyMappedTestRecord.self, replicaMode: .full)
        .provenLegacyPersistedCursorKeys.isEmpty
    )
    #expect(
      identity(LegacyMappedTestRecord.self, shapeDefinitionVersion: "2")
        .provenLegacyPersistedCursorKeys.isEmpty
    )
  }

  @Test
  func widenedIdentityIgnoresUnprovenLegacyStateAndMaterializesNewFields()
    async throws
  {
    let sessionController = TestSessionController()
    do {
      let enrichedRecord = WidenedReplicaTestRecord(
        id: "existing",
        name: "Existing",
        enrichment: "illustrations/calendar-event.json"
      )
      let harness = ReplicaHarness<WidenedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              enrichedRecord,
              offset: "bootstrap-offset"
            ),
            ElectricMessage.replicaUpToDate(offset: "bootstrap-offset"),
          ]
        ],
        syncMode: .eager,
        isExactCursorCutoverEnabled: true,
        blocksFirstFetchUntilResumed: true,
        legacyBootstrapAdmissionController: ElectricLegacyBootstrapAdmissionController(
          enabled: true
        ),
        // A widened wire identity has no compatible legacy cursor and must
        // exercise the full DNF bootstrap path, not the simple-shape fixture
        // default used by the other lifecycle cases.
        shapeTopology: .dnf
      )
      let replica = harness.collection.replica
      #expect(replica.identity.provenLegacyPersistedCursorKeys.isEmpty)

      harness.store.upsert(id: enrichedRecord.id)
      let shippedCursorKey = replica.identity.legacyPersistedCursorKey(syncMode: .eager)
      try harness.metadata.updateSyncState(
        collectionId: shippedCursorKey,
        state: SyncState(
          offset: "shipped-offset",
          handle: "shipped-handle",
          cursor: "shipped-cursor",
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )

      let bootstrapTask = Task {
        try await harness.client.withLegacyBootstrapAdmission(
          identity: replica.identity,
          stage: "widened_identity_test",
          syncMode: .eager
        ) {
          try await harness.client.pollStream(
            WidenedReplicaTestRecord.self,
            basePredicate: nil,
            shapeTopology: .dnf,
            syncMode: .eager,
            live: false,
            forceFullBootstrap: true,
            replicaIdentity: replica.identity
          )
        }
      }

      try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }
      let bootstrapRequest = try #require(await harness.http.capturedRequests().first)
      #expect(bootstrapRequest.offset == "-1")
      #expect(bootstrapRequest.cursor == nil)
      #expect(harness.store.storedIDs() == [enrichedRecord.id])
      #expect(harness.store.enrichment(id: enrichedRecord.id) == nil)

      await harness.http.resumeFirstFetch()
      let batch = try #require(await bootstrapTask.value)
      _ = try batch.apply(in: harness.store)

      #expect(harness.store.storedIDs() == [enrichedRecord.id])
      #expect(harness.store.enrichment(id: enrichedRecord.id) == enrichedRecord.enrichment)
      let exactState = try #require(
        try harness.metadata.getSyncState(
          collectionId: replica.identity.persistedCursorKey,
          transaction: nil
        )
      )
      #expect(exactState.offset == "bootstrap-offset")
    }
  }

  // MARK: - Explicit owner lifecycle states

  @Test
  func ownerStateMovesThroughDormantActiveIdleGraceAndBackToActive() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [ElectricMessage.replicaUpToDate(offset: "offset-1")],
          [ElectricMessage.replicaUpToDate(offset: "offset-2")],
        ],
        syncMode: .eager,
        gcTime: 60
      )
      let replica = harness.collection.replica
      #expect(replica.ownerState == .dormant)

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let firstToken = harness.collection.keepSynced(session: snapshot)
      let secondToken = harness.collection.keepSynced(session: snapshot)
      try await waitUntilTrue { replica.ownerState == .active }
      #expect(replica.liveOwnerCount == 1)

      firstToken.cancel()
      #expect(replica.ownerState == .active)

      secondToken.cancel()
      try await waitUntilTrue { replica.ownerState == .idleGrace }

      let reacquiredToken = harness.collection.keepSynced(session: snapshot)
      try await waitUntilTrue { replica.ownerState == .active }
      #expect(replica.liveOwnerCount == 1)
      reacquiredToken.cancel()
    }
  }

  @Test
  func ownerStateReportsDormantAfterEvictionAndSuspendedAfterFence() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [[ElectricMessage.replicaUpToDate(offset: "offset-1")]],
        syncMode: .eager,
        gcTime: 0
      )
      let replica = harness.collection.replica

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let token = harness.collection.keepSynced(session: snapshot)
      try await waitUntilTrue { replica.ownerState == .active }

      token.cancel()
      try await waitUntilTrue { replica.ownerState == .dormant }

      await replica.cancelAndWait()
      #expect(replica.ownerState == .suspended)
    }
  }

  @Test
  func idleEvictionJoinsBlockedOwnerBeforeImmediateReacquisitionStartsSuccessor() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [ElectricMessage.replicaUpToDate(offset: "offset-old")],
          [ElectricMessage.replicaUpToDate(offset: "offset-new")],
        ],
        syncMode: .eager,
        gcTime: 0,
        blocksFirstFetchUntilResumed: true
      )
      let replica = harness.collection.replica
      let snapshot = try #require(sessionController.captureAuthenticatedSession())

      let firstToken = harness.collection.keepSynced(session: snapshot)
      try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }

      firstToken.cancel()
      let successorToken = harness.collection.keepSynced(session: snapshot)
      defer { successorToken.cancel() }

      #expect(replica.liveOwnerCount == 1)
      #expect(await harness.http.requestCount() == 1)

      await harness.http.resumeFirstFetch()
      try await waitUntilTrueAsync { await harness.http.requestCount() >= 2 }
      #expect(replica.liveOwnerCount == 1)
    }
  }

  // MARK: - Collection sync mode

  @Test
  func streamUsesCollectionConfiguredMode() async throws {
    let sessionController = TestSessionController()
    do {
      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let responses = [
        [ElectricMessage.replicaUpToDate(offset: "offset-1")],
        [ElectricMessage.replicaUpToDate(offset: "offset-2")],
      ]

      let onDemand = ReplicaHarness<ReplicaTestRecord>(
        responses: responses,
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        admitsFreshOnDemandPristineOwner: true
      )
      let onDemandToken = onDemand.collection.keepSynced(session: snapshot)
      defer { onDemandToken.cancel() }
      try await waitUntilTrueAsync { await onDemand.http.requestCount() >= 1 }
      let onDemandRequest = try #require(await onDemand.http.capturedRequests().first)
      #expect(onDemandRequest.offset == "now")
      #expect(onDemandRequest.log == .changesOnly)

      let eager = ReplicaHarness<ReplicaTestRecord>(
        responses: responses,
        syncMode: .eager,
        isExactCursorCutoverEnabled: true
      )
      let eagerToken = eager.collection.keepSynced(session: snapshot)
      defer { eagerToken.cancel() }
      try await waitUntilTrueAsync { await eager.http.requestCount() >= 1 }
      let eagerRequest = try #require(await eager.http.capturedRequests().first)
      #expect(eagerRequest.log == nil)
    }
  }

  // MARK: - Compatible-mode bridge admission

  @Test
  func bridgedEagerCursorResumesWithoutBootstrapWhenExactDisabled() async throws {
    let sessionController = TestSessionController()
    let snapshot = try #require(sessionController.captureAuthenticatedSession())
    let policy = ElectricProtocolCapabilityPolicy.enabled
    let legacyKey = ElectricReplicaIdentity(
      modelType: ReplicaTestRecord.self,
      modelIdentifier: ReplicaTestRecord.collectionIdentifier,
      basePredicate: nil
    ).legacyPersistedCursorKey(syncMode: .eager)
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [[ElectricMessage.replicaUpToDate(offset: "offset-after-bridge")]],
      syncMode: .eager,
      isExactCursorCutoverEnabled: false,
      protocolCapabilityPolicy: policy,
      trackerRebuildOwnership: [
        ElectricReplicaIdentity(
          modelType: ReplicaTestRecord.self,
          modelIdentifier: ReplicaTestRecord.collectionIdentifier,
          basePredicate: nil
        ).persistedCursorKey: ["row-1", "row-2"]
      ]
    )
    try harness.metadata.updateSyncState(
      collectionId: legacyKey,
      state: SyncState(
        offset: "legacy-progressive-offset",
        handle: "legacy-handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 1_000),
        protocolSemanticEpoch: policy.semanticEpoch(),
        bridgedFromSyncMode: .progressive
      ),
      transaction: nil
    )

    let token = harness.collection.keepSynced(session: snapshot)
    defer { token.cancel() }
    try await waitUntilTrueAsync { await harness.http.requestCount() >= 1 }

    let request = try #require(await harness.http.capturedRequests().first)
    #expect(request.offset == "legacy-progressive-offset")
    #expect(request.handle == "legacy-handle")
  }

  @Test
  func bridgedCursorOnDNFTopologyStillRequiresFullBootstrap() async throws {
    let sessionController = TestSessionController()
    let snapshot = try #require(sessionController.captureAuthenticatedSession())
    let policy = ElectricProtocolCapabilityPolicy.enabled
    let legacyKey = ElectricReplicaIdentity(
      modelType: ReplicaTestRecord.self,
      modelIdentifier: ReplicaTestRecord.collectionIdentifier,
      basePredicate: nil
    ).legacyPersistedCursorKey(syncMode: .eager)
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [[ElectricMessage.replicaUpToDate(offset: "offset-bootstrap")]],
      syncMode: .eager,
      isExactCursorCutoverEnabled: false,
      protocolCapabilityPolicy: policy,
      shapeTopology: .dnf,
      trackerRebuildOwnership: [legacyKey: ["row-1"]]
    )
    try harness.metadata.updateSyncState(
      collectionId: legacyKey,
      state: SyncState(
        offset: "legacy-progressive-offset",
        handle: "legacy-handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 1_000),
        protocolSemanticEpoch: policy.semanticEpoch(),
        bridgedFromSyncMode: .progressive
      ),
      transaction: nil
    )

    let token = harness.collection.keepSynced(session: snapshot)
    defer { token.cancel() }
    try await waitUntilTrueAsync { await harness.http.requestCount() >= 1 }

    let request = try #require(await harness.http.capturedRequests().first)
    #expect(request.offset == "-1")
  }

  @Test
  func unattestedLegacyCursorKeepsPreCutoverBootstrapBehavior() async throws {
    let sessionController = TestSessionController()
    let snapshot = try #require(sessionController.captureAuthenticatedSession())
    let policy = ElectricProtocolCapabilityPolicy.enabled
    let legacyKey = ElectricReplicaIdentity(
      modelType: ReplicaTestRecord.self,
      modelIdentifier: ReplicaTestRecord.collectionIdentifier,
      basePredicate: nil
    ).legacyPersistedCursorKey(syncMode: .eager)
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [[ElectricMessage.replicaUpToDate(offset: "offset-bootstrap")]],
      syncMode: .eager,
      isExactCursorCutoverEnabled: false,
      protocolCapabilityPolicy: policy,
      trackerRebuildOwnership: [legacyKey: ["row-1"]]
    )
    try harness.metadata.updateSyncState(
      collectionId: legacyKey,
      state: SyncState(
        offset: "legacy-progressive-offset",
        handle: "legacy-handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 1_000),
        protocolSemanticEpoch: policy.semanticEpoch()
      ),
      transaction: nil
    )

    let token = harness.collection.keepSynced(session: snapshot)
    defer { token.cancel() }
    try await waitUntilTrueAsync { await harness.http.requestCount() >= 1 }

    let request = try #require(await harness.http.capturedRequests().first)
    #expect(request.offset == "-1")
  }

  @Test
  func bridgedCursorNeverAdmitsIntoOnDemand() async throws {
    let sessionController = TestSessionController()
    let snapshot = try #require(sessionController.captureAuthenticatedSession())
    let policy = ElectricProtocolCapabilityPolicy.enabled
    let legacyKey = ElectricReplicaIdentity(
      modelType: ReplicaTestRecord.self,
      modelIdentifier: ReplicaTestRecord.collectionIdentifier,
      basePredicate: nil
    ).legacyPersistedCursorKey(syncMode: .onDemand)
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [[ElectricMessage.replicaUpToDate(offset: "offset-tail")]],
      syncMode: .onDemand,
      isExactCursorCutoverEnabled: false,
      protocolCapabilityPolicy: policy,
      trackerRebuildOwnership: [legacyKey: ["row-1"]],
      admitsFreshOnDemandPristineOwner: true
    )
    try harness.metadata.updateSyncState(
      collectionId: legacyKey,
      state: SyncState(
        offset: "legacy-progressive-offset",
        handle: "legacy-handle",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 1_000),
        protocolSemanticEpoch: policy.semanticEpoch(),
        bridgedFromSyncMode: .progressive
      ),
      transaction: nil
    )

    let token = harness.collection.keepSynced(session: snapshot)
    defer { token.cancel() }
    try await waitUntilTrueAsync { await harness.http.requestCount() >= 1 }

    let request = try #require(await harness.http.capturedRequests().first)
    #expect(request.offset != "legacy-progressive-offset" || request.log == .changesOnly)
  }

  // MARK: - Tracker continuity

  @Test
  func taggedOwnerDiscardsIncrementalResumeWhenTrackerContinuityIsUnavailable() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [ElectricMessage.replicaUpToDate(offset: "offset-boot")],
          [ElectricMessage.replicaUpToDate(offset: "offset-live")],
          [ElectricMessage.replicaUpToDate(offset: "offset-live-2")],
        ],
        syncMode: .eager,
        supportsDurableRowOwnership: false,
        gcTime: 0,
        shapeTopology: .dnf
      )
      let replica = harness.collection.replica
      let streamStateKey = replica.identity.legacyPersistedCursorKey(syncMode: .eager)
      try harness.metadata.updateSyncState(
        collectionId: streamStateKey,
        state: SyncState(
          offset: "persisted-offset",
          handle: "persisted-handle",
          cursor: nil,
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )
      #expect(replica.isTrackerContinuityUnavailable)

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let token = harness.collection.keepSynced(session: snapshot)

      try await waitUntilTrueAsync { await harness.http.requestCount() >= 2 }
      let requests = await harness.http.capturedRequests()
      // Continuity was unavailable (fresh process): the first poll must
      // discard the persisted incremental resume and full-bootstrap.
      #expect(requests[0].offset == "-1")
      // Once continuity is established, the owner resumes incrementally.
      try await waitUntilTrue { !replica.isTrackerContinuityUnavailable }
      #expect(requests[1].offset == "offset-boot")

      // Runtime-owner eviction invalidates process-local tracker continuity.
      token.cancel()
      try await waitUntilTrue { replica.isTrackerContinuityUnavailable }
    }
  }

  @Test
  func untaggedOwnerResumesIncrementallyWithoutTrackerContinuity() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [ElectricMessage.replicaUpToDate(offset: "offset-live")],
          [ElectricMessage.replicaUpToDate(offset: "offset-live-2")],
        ],
        syncMode: .eager
      )
      let replica = harness.collection.replica
      let streamStateKey = replica.identity.legacyPersistedCursorKey(syncMode: .eager)
      try harness.metadata.updateSyncState(
        collectionId: streamStateKey,
        state: SyncState(
          offset: "persisted-offset",
          handle: "persisted-handle",
          cursor: nil,
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let token = harness.collection.keepSynced(session: snapshot)
      defer { token.cancel() }

      try await waitUntilTrueAsync { await harness.http.requestCount() >= 1 }
      let request = try #require(await harness.http.capturedRequests().first)
      #expect(request.offset == "persisted-offset")
    }
  }

  // MARK: - fetchSnapshot / requestSnapshot subset split

  @Test
  func requestSnapshotStagesOwnerResumeMetadataOnlyAtTerminalSubsetChunk() async throws {
    let sessionController = TestSessionController()
    do {
      let subsetMessages =
        (0..<201).map { index in
          ElectricMessage.replicaRecord(
            ReplicaTestRecord(id: "\(index)", name: "Record \(index)"),
            offset: "offset-\(index)",
            isSubsetSnapshot: true
          )
        }
        + [ElectricMessage.replicaSubsetEnd(offset: "offset-201")]
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [subsetMessages],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true
      )
      let exactKey = harness.collection.replica.identity.persistedCursorKey
      try harness.metadata.updateSyncState(
        collectionId: exactKey,
        state: SyncState(
          offset: "offset-5",
          handle: "owner-handle",
          cursor: nil,
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )

      let batch = try #require(
        try await harness.collection.replica.client.requestSnapshot(
          ReplicaTestRecord.self,
          basePredicate: nil,
          descriptor: QueryDescriptor(predicate: SQLExpression("id = '1'")),
          syncMode: .onDemand,
          replicaIdentity: harness.collection.replica.identity
        )
      )
      let chunks = batch.chunked(maxMessages: 200)
      #expect(chunks.count == 2)
      _ = try chunks[0].apply(in: harness.store)

      let preserved = try #require(
        try harness.metadata.getSyncState(collectionId: exactKey, transaction: nil)
      )
      #expect(preserved.offset == "offset-5")
      #expect(preserved.handle == "owner-handle")

      _ = try chunks[1].apply(in: harness.store)
      let advanced = try #require(
        try harness.metadata.getSyncState(collectionId: exactKey, transaction: nil)
      )
      #expect(advanced.offset == "offset-201")
      #expect(advanced.handle == "handle-offset-201")
      #expect(harness.store.storedIDs().count == 201)
    }
  }

  @Test
  func progressiveOwnerSubsetDemandStagesOnlyTheCanonicalLegacyCursor() async throws {
    let sessionController = TestSessionController()
    do {
      let subsetMessages =
        (0..<201).map { index in
          ElectricMessage.replicaRecord(
            ReplicaTestRecord(id: "\(index)", name: "Record \(index)"),
            offset: "offset-\(index)",
            isSubsetSnapshot: true
          )
        }
        + [ElectricMessage.replicaSubsetEnd(offset: "offset-201")]
      let transactionGate = ReplicaTransactionGate(
        blockedInvocation: 1,
        failsAfterRelease: false
      )
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [subsetMessages],
        syncMode: .progressive,
        transactionGate: transactionGate
      )
      let identity = harness.collection.replica.identity
      let progressiveKey = identity.legacyPersistedCursorKey(syncMode: .progressive)
      let onDemandKey = identity.legacyPersistedCursorKey(syncMode: .onDemand)
      try harness.metadata.updateSyncState(
        collectionId: progressiveKey,
        state: SyncState(
          offset: "offset-5",
          handle: "owner-handle",
          cursor: nil,
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let demand = Task {
        try await harness.collection.ensureSubset(
          where: SQLExpression("id IS NOT NULL"),
          session: snapshot
        )
      }
      try await waitUntilTrue {
        transactionGate.hasReachedBlockedInvocation()
      }

      let preserved = try #require(
        try harness.metadata.getSyncState(collectionId: progressiveKey, transaction: nil)
      )
      #expect(preserved.offset == "offset-5")
      #expect(preserved.handle == "owner-handle")
      #expect(
        try harness.metadata.getSyncState(collectionId: onDemandKey, transaction: nil) == nil
      )
      // The 201-row subset and terminal owner cursor are one commitment
      // boundary. Neither rows nor metadata publish while its writer
      // transaction is gated.
      #expect(harness.store.storedIDs().isEmpty)

      transactionGate.release()
      let result = try await demand.value

      #expect(result.appliedRecords.count == 201)
      let advanced = try #require(
        try harness.metadata.getSyncState(collectionId: progressiveKey, transaction: nil)
      )
      #expect(advanced.offset == "offset-201")
      #expect(advanced.handle == "handle-offset-201")
      #expect(
        try harness.metadata.getSyncState(collectionId: onDemandKey, transaction: nil) == nil
      )
      let request = try #require(await harness.http.capturedRequests().first)
      #expect(request.offset == "offset-5")
    }
  }

  @Test
  func ownerSubsetDemandApplies409AndRetriesThroughActualSubsetEnd() async throws {
    let sessionController = TestSessionController()
    do {
      let recoveredRecord = ReplicaTestRecord(id: "recovered", name: "Recovered")
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [.replicaTruncate()],
          [
            .replicaRecord(
              recoveredRecord,
              offset: "offset-record",
              isSubsetSnapshot: true
            ),
            .replicaUpToDate(offset: "offset-up-to-date"),
          ],
          [.replicaSnapshotEnd(offset: "offset-snapshot-end")],
          [.replicaSubsetEnd(offset: "offset-subset-end")],
        ],
        syncMode: .onDemand
      )
      let stateKey = harness.collection.replica.identity.legacyPersistedCursorKey(
        syncMode: .onDemand
      )
      try harness.metadata.updateSyncState(
        collectionId: stateKey,
        state: SyncState(
          offset: "offset-stale",
          handle: "handle-stale",
          cursor: "cursor-stale",
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let result = try await harness.collection.ensureSubset(
        where: SQLExpression("id = 'recovered'"),
        session: snapshot
      )

      #expect(result.appliedRecords == [recoveredRecord])
      #expect(harness.store.storedIDs() == ["recovered"])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 4)
      #expect(requests[0].offset == "offset-stale")
      #expect(requests[1].offset == "-1")
      #expect(requests[2].offset == "offset-up-to-date")
      #expect(requests[3].offset == "offset-snapshot-end")
      let recoveredState = try #require(
        try harness.metadata.getSyncState(collectionId: stateKey, transaction: nil)
      )
      #expect(recoveredState.offset == "offset-subset-end")
      #expect(recoveredState.handle == "handle-offset-subset-end")
    }
  }

  @Test
  func emptyOwnerSubsetFinalizerFencesInterleavedLivePublication() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [[.replicaSubsetEnd(offset: "offset-empty")]],
        syncMode: .progressive
      )
      let finalizerGate = ReplicaOperationGate()
      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      harness.store.upsert(id: "locally-created")

      let demand = Task {
        try await harness.collection.ensureSubset(
          where: SQLExpression("id = 'locally-created'"),
          session: snapshot
        ) { result in
          #expect(result.appliedRecords.isEmpty)
          harness.store.delete(id: "locally-created")
          await finalizerGate.enterAndWait()
        }
      }
      try await waitUntilTrueAsync { await finalizerGate.hasEntered }

      let interleavedPublication = Task {
        try await harness.collection.replica.withStreamPublication {
          harness.store.upsert(id: "locally-created")
        }
      }
      try await waitUntilTrueAsync {
        await harness.collection.replica.publicationWaiterCount == 1
      }
      #expect(harness.store.storedIDs().isEmpty)

      await finalizerGate.release()
      let result = try await demand.value
      #expect(result.appliedRecords.isEmpty)
      try await interleavedPublication.value
      #expect(harness.store.storedIDs() == ["locally-created"])
    }
  }

  @Test
  func ownerSubsetDemandFailsAfterBoundedRepeated409Recovery() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [.replicaTruncate()],
          [.replicaTruncate()],
          [.replicaTruncate()],
          [.replicaTruncate()],
        ],
        syncMode: .onDemand
      )
      let stateKey = harness.collection.replica.identity.legacyPersistedCursorKey(
        syncMode: .onDemand
      )
      try harness.metadata.updateSyncState(
        collectionId: stateKey,
        state: SyncState(
          offset: "offset-stale",
          handle: "handle-stale",
          cursor: "cursor-stale",
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      await #expect(throws: ElectricSyncError.self) {
        try await harness.collection.ensureSubset(
          where: SQLExpression("id = 'missing'"),
          session: snapshot
        )
      }

      #expect(await harness.http.requestCount() == 4)
      let fencedState = try #require(
        try harness.metadata.getSyncState(collectionId: stateKey, transaction: nil)
      )
      #expect(!fencedState.canResumeWithoutFullBootstrap)
      #expect(fencedState.offset == "-1")
    }
  }

  @Test
  func liveOwnerHydratesMissingRowWithoutCancellingItsOwnTransport() async throws {
    let sessionController = TestSessionController()
    do {
      let hydratedRecord = ReplicaTestRecord(id: "hydrated", name: "Hydrated")
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            .replicaRecord(
              ReplicaTestRecord(id: "hydrated", name: "__requires_hydration__"),
              offset: "offset-live",
              key: "hydrated"
            ),
            .replicaUpToDate(offset: "offset-live"),
          ],
          [
            .replicaRecord(
              hydratedRecord,
              offset: "offset-hydrated",
              key: "hydrated",
              isSubsetSnapshot: true
            ),
            .replicaSubsetSnapshotEnd(
              offset: "offset-hydrated",
              snapshot: PostgresSnapshot(xmin: "200", xmax: "300", xipList: [])
            ),
            .replicaSubsetEnd(offset: "offset-hydrated"),
          ],
          [
            .replicaRecord(
              ReplicaTestRecord(id: "hydrated", name: "Visible in hydration snapshot"),
              offset: "offset-duplicate",
              key: "hydrated",
              txids: [100]
            ),
            .replicaUpToDate(offset: "offset-duplicate"),
          ],
        ],
        syncMode: .eager,
        cancellationAwareBlockedRequestIndex: 4
      )

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let token = harness.collection.keepSynced(session: snapshot)
      defer { token.cancel() }

      try await waitUntilTrueAsync {
        await harness.http.requestCount() >= 4
      }
      #expect(harness.store.storedIDs() == ["hydrated"])
      #expect(harness.store.name(id: "hydrated") == hydratedRecord.name)
      #expect(harness.collection.replica.liveOwnerCount == 1)
      #expect(await harness.http.cancelledFetchCount() == 0)
      let requests = await harness.http.capturedRequests()
      #expect(requests[0].subset == nil)
      #expect(requests[1].subset != nil)
      #expect(requests[2].subset == nil)
      #expect(requests[2].offset == "offset-hydrated")

      token.cancel()
      try await waitUntilTrueAsync {
        await harness.http.cancelledFetchCount() == 1
      }
    }
  }

  @Test
  func progressiveInitialSubsetNeverAdvancesOwnerResumeMetadata() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              ReplicaTestRecord(id: "1", name: "Alpha"),
              offset: "offset-9",
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-9"),
          ]
        ],
        syncMode: .progressive,
        isExactCursorCutoverEnabled: true
      )
      let exactKey = harness.collection.replica.identity.persistedCursorKey
      try harness.metadata.updateSyncState(
        collectionId: exactKey,
        state: SyncState(
          offset: "offset-5",
          handle: "owner-handle",
          cursor: nil,
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )

      _ = try await harness.collection.query(where: SQLExpression("id = '1'"))

      let preserved = try #require(
        try harness.metadata.getSyncState(collectionId: exactKey, transaction: nil)
      )
      #expect(preserved.offset == "offset-5")
      #expect(preserved.handle == "owner-handle")
    }
  }

  @Test
  func requestSnapshotEstablishesOwnerResumeMetadataWithoutExistingState() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              ReplicaTestRecord(id: "1", name: "Alpha"),
              offset: "offset-9",
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-9"),
          ]
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        admitsFreshOnDemandPristineOwner: true
      )
      let exactKey = harness.collection.replica.identity.persistedCursorKey

      _ = try await harness.collection.query(where: SQLExpression("id = '1'"))

      let established = try #require(
        try harness.metadata.getSyncState(collectionId: exactKey, transaction: nil)
      )
      #expect(established.offset == "offset-9")
      #expect(established.handle == "handle-offset-9")
    }
  }

  @Test
  func progressiveFetchSnapshotIgnoresCompletionFromFinishedGeneration() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              ReplicaTestRecord(id: "late", name: "Late"),
              offset: "offset-late",
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-late"),
          ]
        ],
        syncMode: .progressive,
        blocksFirstFetchUntilResumed: true
      )

      let query = Task {
        try await harness.collection.query(where: SQLExpression("id = 'late'"))
      }
      try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }

      harness.collection.replica.finishProgressiveInitialBuffering()
      await harness.http.resumeFirstFetch()

      #expect(try await query.value.isEmpty)
      #expect(harness.store.storedIDs().isEmpty)
    }
  }

  @Test
  func terminalLivePublicationFinishesProgressiveGenerationBeforeReleasingWaiter() async throws {
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [],
      syncMode: .progressive
    )
    let replica = harness.collection.replica
    let generation = try #require(replica.progressiveSnapshotGeneration())
    let terminalOperationGate = ReplicaOperationGate()

    let terminalPublication = Task {
      try await replica.withStreamPublication(
        finishesProgressiveInitialBuffering: true
      ) {
        await terminalOperationGate.enterAndWait()
      }
    }
    try await waitUntilTrueAsync { await terminalOperationGate.hasEntered }
    let progressiveWaiter = Task {
      try await replica.withStreamPublication {
        guard replica.isProgressiveSnapshotGenerationCurrent(generation) else {
          return false
        }
        harness.store.upsert(id: "late")
        return true
      }
    }
    try await waitUntilTrueAsync { await replica.publicationWaiterCount == 1 }

    await terminalOperationGate.release()
    try await terminalPublication.value

    #expect(try await progressiveWaiter.value == false)
    #expect(harness.store.storedIDs().isEmpty)
  }

  @Test
  func taggedOwnerEvictionUsesOneAtomicFullBootstrapBeforeOwnerSnapshotDemand() async throws {
    let sessionController = TestSessionController()
    do {
      let fullRecord = TaggedReplicaTestRecord(id: "full", name: "Full")
      let subsetRecord = TaggedReplicaTestRecord(id: "subset", name: "Subset")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              fullRecord,
              offset: "offset-bootstrap",
              tags: ["shape"]
            ),
            ElectricMessage.replicaUpToDate(offset: "offset-bootstrap"),
          ],
          [
            ElectricMessage.replicaRecord(
              subsetRecord,
              offset: "offset-subset",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-subset"),
          ],
        ],
        syncMode: .onDemand,
        supportsDurableRowOwnership: false,
        gcTime: 0.01,
        shapeTopology: .dnf
      )
      let replica = harness.collection.replica
      let stateKey = replica.identity.legacyPersistedCursorKey(syncMode: .onDemand)
      try harness.metadata.updateSyncState(
        collectionId: stateKey,
        state: SyncState(
          offset: "offset-stale",
          handle: "handle-stale",
          cursor: "cursor-stale",
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )

      replica.markTrackerContinuityEstablished()
      let token = replica.acquireStream(syncMode: .onDemand) {
        Task {}
      }
      token.cancel()
      try await waitUntilTrue { replica.isTrackerContinuityUnavailable }
      // A full replacement must remove rows from the abandoned generation in
      // the same transaction that applies the authoritative bootstrap.
      harness.store.upsert(id: "stale")

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let result = try await harness.collection.ensureSubset(
        where: SQLExpression("id = 'subset'"),
        session: snapshot
      )

      #expect(result.appliedRecords == [subsetRecord])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 2)
      #expect(requests[0].offset == "-1")
      #expect(requests[0].subset == nil)
      #expect(requests[1].offset == "offset-bootstrap")
      #expect(requests[1].subset != nil)
      #expect(harness.store.storedIDs() == [fullRecord.id, subsetRecord.id])
      #expect(!replica.isTrackerContinuityUnavailable)
    }
  }

  @Test
  func cancellingKnownTrackerLossBootstrapLeavesStaleGenerationAndFailsClosed() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [],
        syncMode: .onDemand,
        supportsDurableRowOwnership: false,
        cancellationAwareBlockedRequestIndex: 1,
        shapeTopology: .dnf
      )
      let replica = harness.collection.replica
      let stateKey = replica.identity.legacyPersistedCursorKey(syncMode: .onDemand)
      try harness.metadata.updateSyncState(
        collectionId: stateKey,
        state: SyncState(
          offset: "offset-stale",
          handle: "handle-stale",
          cursor: "cursor-stale",
          isUpToDate: true,
          lastSyncedAt: Date()
        ),
        transaction: nil
      )
      harness.store.upsert(id: "stale", name: "Stale generation")
      #expect(replica.isTrackerContinuityUnavailable)

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let subsetTask = Task {
        try await harness.collection.ensureSubset(
          where: SQLExpression("id = 'subset'"),
          session: snapshot
        )
      }
      try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }
      let bootstrapRequest = try #require(await harness.http.capturedRequests().first)
      #expect(bootstrapRequest.offset == "-1")
      #expect(bootstrapRequest.subset == nil)
      #expect(replica.ownerState == .replacing)

      await harness.http.cancelBlockedFetchForTest()
      try await waitUntilTrueAsync { await harness.http.cancelledFetchCount() == 1 }
      await #expect(throws: CancellationError.self) {
        try await subsetTask.value
      }

      try await waitUntilTrue { replica.ownerState != .replacing }
      #expect(harness.store.storedIDs() == ["stale"])
      #expect(replica.isTrackerContinuityUnavailable)
    }
  }

  @Test
  func cancellingDemandedSubsetResetLeavesStaleGenerationAndNeverRequestsFullHistory()
    async throws
  {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        cancellationAwareBlockedRequestIndex: 1,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        trackerRebuildAdmissionError: true
      )
      let replica = harness.collection.replica
      try harness.metadata.updateSyncState(
        collectionId: replica.identity.persistedCursorKey,
        state: SyncState(
          offset: "offset-stale",
          handle: "handle-stale",
          cursor: "cursor-stale",
          isUpToDate: true,
          lastSyncedAt: Date(),
          protocolSemanticEpoch: .taggedShape1_7_7
        ),
        transaction: nil
      )
      harness.store.upsert(id: "stale", name: "Stale generation")
      harness.metadata.seedOwnedRow(
        table: TaggedReplicaTestRecord.tableName,
        rowKey: "stale"
      )

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let subsetTask = Task {
        try await harness.collection.ensureSubset(
          where: SQLExpression("id = 'today'"),
          session: snapshot
        )
      }
      try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }
      let request = try #require(await harness.http.capturedRequests().first)
      #expect(request.offset == "now")
      #expect(request.subset != nil)
      #expect(request.log == .changesOnly)
      #expect(replica.ownerState == .replacing)

      await harness.http.cancelBlockedFetchForTest()
      try await waitUntilTrueAsync { await harness.http.cancelledFetchCount() == 1 }
      await #expect(throws: CancellationError.self) {
        try await subsetTask.value
      }

      try await waitUntilTrue { replica.ownerState != .replacing }
      #expect(harness.store.storedIDs() == ["stale"])
      #expect(replica.isTrackerContinuityUnavailable)
      let requests = await harness.http.capturedRequests()
      #expect(!requests.contains { $0.subset == nil && $0.log == nil })
    }
  }

  @Test
  func sharedTableColdOwnerSubsetUsesExactPersistedCursorWithoutFullBootstrap() async throws {
    let sessionController = TestSessionController()
    do {
      let subsetRecord = SharedReplicaTestRecord(id: "subset", name: "Subset")
      let harness = ReplicaHarness<SharedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              subsetRecord,
              offset: "offset-subset",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-subset"),
          ]
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        trackerRebuildOwnership: [:]
      )
      let replica = harness.collection.replica
      try harness.metadata.updateSyncState(
        collectionId: replica.identity.persistedCursorKey,
        state: SyncState(
          offset: "offset-resumed",
          handle: "handle-resumed",
          cursor: "cursor-resumed",
          isUpToDate: true,
          lastSyncedAt: Date(),
          protocolSemanticEpoch: .taggedShape1_7_7
        ),
        transaction: nil
      )

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let subset = try await harness.collection.ensureSubset(
        where: SQLExpression("id = 'subset'"),
        session: snapshot
      )

      #expect(subset.appliedRecords == [subsetRecord])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 1)
      let request = try #require(requests.first)
      #expect(request.offset == "offset-resumed")
      #expect(request.handle == "handle-resumed")
      #expect(request.cursor == "cursor-resumed")
      #expect(request.subset != nil)
      #expect(!requests.contains(where: { $0.offset == "-1" && $0.subset == nil }))
    }
  }

  @Test
  func freshOnDemandStaticSimpleOwnerRequestsSubsetWithoutFullBootstrap() async throws {
    let sessionController = TestSessionController()
    do {
      let todayRecord = TaggedReplicaTestRecord(id: "today", name: "Today")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              todayRecord,
              offset: "offset-today",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-today"),
            ElectricMessage.replicaUpToDate(offset: "offset-today"),
          ]
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        admitsFreshOnDemandPristineOwner: true
      )
      let snapshot = try #require(sessionController.captureAuthenticatedSession())

      let subset = try await harness.collection.ensureSubset(
        where: SQLExpression("id = 'today'"),
        session: snapshot
      )

      #expect(subset.appliedRecords == [todayRecord])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 1)
      let request = try #require(requests.first)
      #expect(request.offset == "now")
      #expect(request.subset != nil)
      #expect(request.log == .changesOnly)
      #expect(!requests.contains(where: { $0.offset == "-1" && $0.subset == nil }))
    }
  }

  @Test
  func freshOnDemandStaticSimpleOwnerUsesDemandedSubsetWhenPristineAdmissionRefuses()
    async throws
  {
    let sessionController = TestSessionController()
    do {
      let subsetRecord = TaggedReplicaTestRecord(id: "today", name: "Today")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              subsetRecord,
              offset: "offset-today",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-today"),
          ]
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        pristineOwnerAdmissionResults: [.admit, .refuse]
      )
      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let todayPredicate = SQLExpression("id = 'today'")
      let staleCoverage = PredicateHash(from: SQLExpression("id = 'stale'"))
      // The first admission sees an empty table. Simulate old-generation
      // artifacts arriving before its just-in-time revalidation refuses.
      harness.metadata.afterNextPristineOwnerAdmission {
        harness.store.upsert(id: "stale", name: "Stale generation")
        harness.metadata.seedOwnedRow(table: TaggedReplicaTestRecord.tableName, rowKey: "stale")
        harness.metadata.seedFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage
        )
      }

      let subset = try await harness.collection.ensureSubset(
        where: todayPredicate,
        session: snapshot
      )

      #expect(subset.appliedRecords == [subsetRecord])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 1)
      #expect(requests[0].offset == "now")
      #expect(requests[0].subset != nil)
      #expect(requests[0].log == .changesOnly)
      #expect(!requests.contains { $0.subset == nil && $0.log == nil })
      #expect(harness.store.storedIDs() == [subsetRecord.id])
      #expect(
        try !harness.metadata.hasFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage,
          transaction: nil
        )
      )
    }
  }

  @Test
  func freshOnDemandStaticSimpleOwnerUsesDemandedSubsetWhenPristineRevalidationErrors()
    async throws
  {
    let sessionController = TestSessionController()
    do {
      let subsetRecord = TaggedReplicaTestRecord(id: "today", name: "Today")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              subsetRecord,
              offset: "offset-today",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-today"),
          ]
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        pristineOwnerAdmissionResults: [.admit, .error]
      )
      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let staleCoverage = PredicateHash(from: SQLExpression("id = 'stale'"))
      harness.metadata.afterNextPristineOwnerAdmission {
        harness.store.upsert(id: "stale", name: "Stale generation")
        harness.metadata.seedOwnedRow(table: TaggedReplicaTestRecord.tableName, rowKey: "stale")
        harness.metadata.seedFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage
        )
      }
      let subset = try await harness.collection.ensureSubset(
        where: SQLExpression("id = 'today'"),
        session: snapshot
      )

      #expect(subset.appliedRecords == [subsetRecord])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 1)
      #expect(requests[0].offset == "now")
      #expect(requests[0].subset != nil)
      #expect(requests[0].log == .changesOnly)
      #expect(!requests.contains { $0.subset == nil && $0.log == nil })
      #expect(harness.store.storedIDs() == [subsetRecord.id])
      #expect(
        try !harness.metadata.hasFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage,
          transaction: nil
        )
      )
    }
  }

  @Test
  func freshPristineRevalidationErrorFallsBackToForcedBootstrap() async throws {
    let sessionController = TestSessionController()
    do {
      let bootstrapRecord = TaggedReplicaTestRecord(id: "bootstrap", name: "Bootstrap")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              bootstrapRecord, offset: "offset-bootstrap", tags: ["shape"]),
            ElectricMessage.replicaUpToDate(offset: "offset-bootstrap"),
          ]
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        pristineOwnerAdmissionResults: [.admit, .error]
      )
      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let staleCoverage = PredicateHash(from: SQLExpression("id = 'stale'"))
      harness.metadata.afterNextPristineOwnerAdmission {
        harness.store.upsert(id: "stale", name: "Stale generation")
        harness.metadata.seedOwnedRow(table: TaggedReplicaTestRecord.tableName, rowKey: "stale")
        harness.metadata.seedFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage
        )
      }
      let token = harness.collection.keepSynced(session: snapshot)
      defer { token.cancel() }

      // The admission read is only an optimization. Its failure must not
      // escape the owner task; it must begin the conservative replacement.
      try await waitUntilTrueAsync { await harness.http.requestCount() >= 1 }
      let request = try #require(await harness.http.capturedRequests().first)
      #expect(request.offset == "-1")
      #expect(request.subset == nil)
      #expect(request.log == nil)
      try await waitUntilTrue { harness.store.storedIDs() == [bootstrapRecord.id] }
      #expect(
        try !harness.metadata.hasFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage,
          transaction: nil
        )
      )
    }
  }

  @Test
  func freshPristineRevalidationRefusalFallsBackToForcedBootstrap() async throws {
    let sessionController = TestSessionController()
    do {
      let bootstrapRecord = TaggedReplicaTestRecord(id: "bootstrap", name: "Bootstrap")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              bootstrapRecord, offset: "offset-bootstrap", tags: ["shape"]),
            ElectricMessage.replicaUpToDate(offset: "offset-bootstrap"),
          ]
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        pristineOwnerAdmissionResults: [.admit, .refuse]
      )
      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let staleCoverage = PredicateHash(from: SQLExpression("id = 'stale'"))
      harness.metadata.afterNextPristineOwnerAdmission {
        harness.store.upsert(id: "stale", name: "Stale generation")
        harness.metadata.seedOwnedRow(table: TaggedReplicaTestRecord.tableName, rowKey: "stale")
        harness.metadata.seedFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage
        )
      }
      let token = harness.collection.keepSynced(session: snapshot)
      defer { token.cancel() }

      try await waitUntilTrueAsync { await harness.http.requestCount() >= 1 }
      let request = try #require(await harness.http.capturedRequests().first)
      #expect(request.offset == "-1")
      #expect(request.subset == nil)
      #expect(request.log == nil)
      try await waitUntilTrue { harness.store.storedIDs() == [bootstrapRecord.id] }
      #expect(
        try !harness.metadata.hasFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage,
          transaction: nil
        )
      )
    }
  }

  @Test
  func failedProtocolForcedBootstrapStaysLatchedUntilReplacementApplies() async throws {
    let sessionController = TestSessionController()
    do {
      let bootstrapRecord = TaggedReplicaTestRecord(id: "bootstrap", name: "Bootstrap")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [],
          [
            ElectricMessage.replicaRecord(
              bootstrapRecord, offset: "offset-bootstrap", tags: ["shape"]),
            ElectricMessage.replicaUpToDate(offset: "offset-bootstrap"),
          ],
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        admitsFreshOnDemandPristineOwner: true,
        protocolErrorRequestIndexes: [1]
      )
      let staleCoverage = PredicateHash(from: SQLExpression("id = 'stale'"))
      await harness.http.afterNextProtocolError {
        harness.store.upsert(id: "stale", name: "Stale generation")
        harness.metadata.seedOwnedRow(table: TaggedReplicaTestRecord.tableName, rowKey: "stale")
        harness.metadata.seedFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage
        )
      }

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let token = harness.collection.keepSynced(session: snapshot)
      defer { token.cancel() }

      try await waitUntilTrueAsync { await harness.http.requestCount() >= 3 }
      let requests = await harness.http.capturedRequests()
      #expect(requests[0].offset == "now")
      #expect(requests[0].log == .changesOnly)
      #expect(requests[1].offset == "-1")
      #expect(requests[1].subset == nil)
      #expect(requests[1].log == nil)
      #expect(requests[2].offset == "-1")
      #expect(requests[2].subset == nil)
      #expect(requests[2].log == nil)
      try await waitUntilTrue { harness.store.storedIDs() == [bootstrapRecord.id] }
      #expect(
        try !harness.metadata.hasFetched(
          table: TaggedReplicaTestRecord.tableName,
          predicate: staleCoverage,
          transaction: nil
        )
      )
    }
  }

  @Test
  func freshSharedOnDemandLocalProjectionRequestsSubsetWithoutFullBootstrap() async throws {
    let sessionController = TestSessionController()
    do {
      let todayRecord = SharedReplicaTestRecord(id: "today", name: "Today")
      let harness = ReplicaHarness<SharedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              todayRecord,
              offset: "offset-today",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-today"),
          ]
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        admitsFreshOnDemandPristineOwner: true
      )
      harness.store.upsert(id: "local-projection", name: "Local projection")
      let snapshot = try #require(sessionController.captureAuthenticatedSession())

      let subset = try await harness.collection.ensureSubset(
        where: SQLExpression("id = 'today'"),
        session: snapshot
      )

      #expect(subset.appliedRecords == [todayRecord])
      let request = try #require(await harness.http.capturedRequests().first)
      #expect(request.offset == "now")
      #expect(request.subset != nil)
      #expect(request.log == .changesOnly)
    }
  }

  @Test
  func declaredSimpleTaggedOnDemandPreloadRecoversThroughDemandedSubsetOnly()
    async throws
  {
    let sessionController = TestSessionController()
    let logger = RecordingLogProvider()
    do {
      let staleRecord = TaggedReplicaTestRecord(id: "stale", name: "Stale")
      let subsetRecord = TaggedReplicaTestRecord(id: "subset", name: "Subset")
      let tailRecord = TaggedReplicaTestRecord(id: "tail", name: "Tail")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              subsetRecord,
              offset: "offset-subset",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-subset"),
          ],
          [
            ElectricMessage.replicaRecord(
              tailRecord,
              offset: "offset-tail",
              tags: ["shape"]
            ),
            ElectricMessage.replicaUpToDate(offset: "offset-tail"),
          ],
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple,
        trackerRebuildAdmissionError: true,
        logger: logger
      )
      let replica = harness.collection.replica
      let stateKey = replica.identity.persistedCursorKey
      try harness.metadata.updateSyncState(
        collectionId: stateKey,
        state: SyncState(
          offset: "offset-resumed-tail",
          handle: "handle-resumed-tail",
          cursor: "cursor-resumed-tail",
          isUpToDate: true,
          lastSyncedAt: Date(),
          protocolSemanticEpoch: .taggedShape1_7_7
        ),
        transaction: nil
      )
      harness.store.upsert(id: staleRecord.id, name: staleRecord.name)
      harness.metadata.seedOwnedRow(
        table: TaggedReplicaTestRecord.tableName,
        rowKey: staleRecord.id
      )
      let subsetPredicate = SQLExpression(
        predicate: .equals(field: "id", value: .string("subset"))
      )
      // This row and coverage belong to the stale generation. Recovery must
      // replace them atomically with the demanded subset, without fetching the
      // collection's unscoped history first.
      try await harness.client.markFetched(TaggedReplicaTestRecord.self, where: subsetPredicate)
      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      _ = try await harness.collection.query(
        where: subsetPredicate,
        session: snapshot
      )

      #expect(!replica.isTrackerContinuityUnavailable)
      #expect(harness.store.storedIDs() == [subsetRecord.id])
      let recoveryRequests = await harness.http.capturedRequests()
      #expect(recoveryRequests.count == 1)
      #expect(recoveryRequests[0].offset == "now")
      #expect(recoveryRequests[0].subset?.whereClause == "id = $1")
      #expect(recoveryRequests[0].subset?.paramsJSON == #"{"1":"subset"}"#)
      #expect(recoveryRequests[0].log == .changesOnly)
      #expect(!recoveryRequests.contains { $0.subset == nil && $0.log == nil })

      let token = harness.collection.keepSynced(session: snapshot)
      defer { token.cancel() }
      try await waitUntilTrueAsync { harness.store.storedIDs().contains(tailRecord.id) }

      let requests = await harness.http.capturedRequests()
      #expect(requests.count >= 2)
      #expect(requests[1].offset == "offset-subset")
      #expect(requests[1].log == .changesOnly)
      #expect(!requests.contains { $0.subset == nil && $0.log == nil })
      #expect(
        logger.entries().contains(
          .init(
            level: .warning,
            message: "electric_tracker_rebuild_admission_failed_subset_reset",
            metadata: [
              "table": TaggedReplicaTestRecord.tableName,
              "collection": TaggedReplicaTestRecord.collectionIdentifier,
              "reason": "admission_error",
              "local_table_ownership": "exclusive",
              "error_type": "ElectricSyncError",
            ]
          )
        )
      )
    }
  }

  @Test
  func optedInExclusiveDNFOwnerReplacesWorkingSetFromSubsetBeforeTail() async throws {
    let sessionController = TestSessionController()
    do {
      let staleRecord = ReplicaTestRecord(id: "stale", name: "Stale")
      let seedRecord = ReplicaTestRecord(id: "seed", name: "Seed")
      let tailRecord = ReplicaTestRecord(id: "tail", name: "Tail")
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              seedRecord,
              offset: "offset-seed",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-seed"),
          ],
          [
            ElectricMessage.replicaRecord(tailRecord, offset: "offset-tail", tags: ["shape"]),
            ElectricMessage.replicaUpToDate(offset: "offset-tail"),
          ],
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .dnf,
        trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
      )
      let replica = harness.collection.replica
      try harness.metadata.updateSyncState(
        collectionId: replica.identity.persistedCursorKey,
        state: SyncState(
          offset: "offset-stale",
          handle: "handle-stale",
          cursor: "cursor-stale",
          isUpToDate: true,
          lastSyncedAt: Date(),
          protocolSemanticEpoch: .taggedShape1_7_7
        ),
        transaction: nil
      )
      harness.store.upsert(id: staleRecord.id, name: staleRecord.name)
      harness.metadata.seedOwnedRow(table: ReplicaTestRecord.tableName, rowKey: staleRecord.id)
      let session = try #require(sessionController.captureAuthenticatedSession())

      let activation = try await harness.collection.activateDemand(
        where: SQLExpression(predicate: .equals(field: "id", value: .string("seed"))),
        session: session
      )
      defer { activation.lease.cancel() }

      #expect(!replica.isTrackerContinuityUnavailable)
      #expect(activation.result.appliedRecords.map(\.id) == [seedRecord.id])
      #expect(activation.result.localRecords.map(\.id) == [seedRecord.id])
      #expect(harness.store.storedIDs() == [seedRecord.id])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 1)
      #expect(requests[0].offset == "now")
      #expect(requests[0].subset?.whereClause == "id = $1")
      #expect(requests[0].log == .changesOnly)
      #expect(!requests.contains { $0.offset == "-1" && $0.subset == nil })

      let token = harness.collection.keepSynced(session: session)
      defer { token.cancel() }
      try await waitUntilTrueAsync { harness.store.storedIDs().contains(tailRecord.id) }
      let tailRequest = try #require((await harness.http.capturedRequests()).dropFirst().first)
      #expect(tailRequest.offset == "offset-seed")
      #expect(tailRequest.log == .changesOnly)
    }
  }

  @Test
  func optedInDNFWorkingSetParksRawTailUntilAnActivationSeedsIt() async throws {
    let sessionController = TestSessionController()
    let seed = ReplicaTestRecord(id: "seed", name: "Seed")
    let tail = ReplicaTestRecord(id: "tail", name: "Tail")
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          ElectricMessage.replicaRecord(seed, offset: "seed-offset", tags: ["shape"], isSubsetSnapshot: true),
          ElectricMessage.replicaSubsetEnd(offset: "seed-offset"),
        ],
        [
          ElectricMessage.replicaRecord(tail, offset: "tail-offset", tags: ["shape"]),
          ElectricMessage.replicaUpToDate(offset: "tail-offset"),
        ],
      ],
      syncMode: .onDemand,
      isExactCursorCutoverEnabled: true,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let session = try #require(sessionController.captureAuthenticatedSession())
    let rawTail = harness.collection.keepSynced(session: session)
    await Task.yield()
    #expect(await harness.http.requestCount() == 0)
    rawTail.cancel()

    let activation = try await harness.collection.activateDemand(
      where: SQLExpression(predicate: .equals(field: "id", value: .string("seed"))), session: session
    )
    defer { activation.lease.cancel() }
    try await waitUntilTrueAsync { harness.store.storedIDs().contains(tail.id) }
    let requests = await harness.http.capturedRequests()
    #expect(requests.prefix(2).map(\.offset) == ["now", "seed-offset"])
    #expect(!requests.contains { $0.offset == "-1" })
  }

  @Test
  func concurrentDNFActivationsSeedStableLeaseInventoryBeforeTail() async throws {
    let sessionController = TestSessionController()
    let first = ReplicaTestRecord(id: "first", name: "First")
    let second = ReplicaTestRecord(id: "second", name: "Second")
    let tail = ReplicaTestRecord(id: "tail", name: "Tail")
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          ElectricMessage.replicaRecord(first, offset: "first-offset", tags: ["shape"], isSubsetSnapshot: true),
          ElectricMessage.replicaSubsetEnd(offset: "first-offset"),
        ],
        [
          ElectricMessage.replicaRecord(second, offset: "second-offset", tags: ["shape"], isSubsetSnapshot: true),
          ElectricMessage.replicaSubsetEnd(offset: "second-offset"),
        ],
        [
          ElectricMessage.replicaRecord(tail, offset: "tail-offset", tags: ["shape"]),
          ElectricMessage.replicaUpToDate(offset: "tail-offset"),
        ],
      ],
      syncMode: .onDemand,
      isExactCursorCutoverEnabled: true,
      blocksFirstFetchUntilResumed: true,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let session = try #require(sessionController.captureAuthenticatedSession())
    let firstTask = Task {
      try await harness.collection.activateDemand(
        where: SQLExpression(predicate: .equals(field: "id", value: .string("first"))),
        session: session
      )
    }
    try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }
    let secondTask = Task {
      try await harness.collection.activateDemand(
        where: SQLExpression(predicate: .equals(field: "id", value: .string("second"))),
        session: session
      )
    }
    try await waitUntilTrueAsync {
      await harness.collection.replica.activeDemandDescriptors().count == 2
    }
    await harness.http.resumeFirstFetch()
    let firstActivation = try await firstTask.value
    let secondActivation = try await secondTask.value
    defer {
      firstActivation.lease.cancel()
      secondActivation.lease.cancel()
    }
    try await waitUntilTrueAsync { harness.store.storedIDs().contains(tail.id) }
    let requests = await harness.http.capturedRequests()
    #expect(requests.count >= 3)
    #expect(requests[0].offset == "now")
    #expect(requests[1].offset == "first-offset")
    #expect(requests[2].offset == "second-offset")
    #expect(requests[0].subset != nil && requests[1].subset != nil && requests[2].subset == nil)
    #expect(!requests.contains { $0.offset == "-1" })
  }

  @Test
  func releasedLeaseDuringWorkingSetRecoveryDoesNotStrandTailOrRequireReleasedSubset() async throws {
    let sessionController = TestSessionController()
    let first = ReplicaTestRecord(id: "first", name: "First")
    let tail = ReplicaTestRecord(id: "tail", name: "Tail")
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          ElectricMessage.replicaRecord(first, offset: "first-offset", tags: ["shape"], isSubsetSnapshot: true),
          ElectricMessage.replicaSubsetEnd(offset: "first-offset"),
        ],
        [
          ElectricMessage.replicaRecord(tail, offset: "tail-offset", tags: ["shape"]),
          ElectricMessage.replicaUpToDate(offset: "tail-offset"),
        ],
      ],
      syncMode: .onDemand,
      isExactCursorCutoverEnabled: true,
      blocksFirstFetchUntilResumed: true,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let session = try #require(sessionController.captureAuthenticatedSession())
    let firstTask = Task {
      try await harness.collection.activateDemand(
        where: SQLExpression(predicate: .equals(field: "id", value: .string("first"))),
        session: session
      )
    }
    try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }
    let releasedTask = Task {
      try await harness.collection.activateDemand(
        where: SQLExpression(predicate: .equals(field: "id", value: .string("released"))),
        session: session
      )
    }
    try await waitUntilTrueAsync {
      await harness.collection.replica.activeDemandDescriptors().count == 2
    }
    releasedTask.cancel()
    await #expect(throws: CancellationError.self) { try await releasedTask.value }
    try await waitUntilTrueAsync {
      await harness.collection.replica.activeDemandDescriptors().count == 1
    }
    await harness.http.resumeFirstFetch()
    let firstActivation = try await firstTask.value
    defer { firstActivation.lease.cancel() }
    try await waitUntilTrueAsync { harness.store.storedIDs().contains(tail.id) }
    let requests = await harness.http.capturedRequests()
    #expect(requests.count >= 2)
    #expect(requests[0].offset == "now")
    #expect(requests[1].offset == "first-offset")
    #expect(requests[1].subset == nil)
    #expect(!requests.contains { $0.offset == "-1" })
  }

  @Test
  func failedWorkingSetSeedLeavesTailParkedAndLaterActivationRetriesFromNow() async throws {
    let sessionController = TestSessionController()
    let recovered = ReplicaTestRecord(id: "recovered", name: "Recovered")
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          ElectricMessage.replicaRecord(
            recovered, offset: "recovered-offset", tags: ["shape"], isSubsetSnapshot: true
          ),
          ElectricMessage.replicaSubsetEnd(offset: "recovered-offset"),
        ],
      ],
      syncMode: .onDemand,
      isExactCursorCutoverEnabled: true,
      cancellationAwareBlockedRequestIndex: 1,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let session = try #require(sessionController.captureAuthenticatedSession())
    let failedActivation = Task {
      try await harness.collection.activateDemand(
        where: SQLExpression(predicate: .equals(field: "id", value: .string("failed"))),
        session: session
      )
    }
    try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }
    await harness.http.cancelBlockedFetchForTest()
    await #expect(throws: CancellationError.self) { try await failedActivation.value }

    let recoveredActivation = try await harness.collection.activateDemand(
      where: SQLExpression(predicate: .equals(field: "id", value: .string("recovered"))),
      session: session
    )
    defer { recoveredActivation.lease.cancel() }
    #expect(recoveredActivation.result.appliedRecords == [recovered])
    let requests = await harness.http.capturedRequests()
    #expect(requests.count >= 2)
    #expect(requests[0].offset == "now")
    #expect(requests[1].offset == "now")
    #expect(!requests.contains { $0.offset == "-1" })
  }

  @Test
  func staleWorkingSetGenerationReturnsTypedCompletionInsteadOfCancellation() async throws {
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [],
      syncMode: .onDemand,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let descriptor = QueryDescriptor(
      predicate: SQLExpression(predicate: .equals(field: "id", value: .string("seed")))
    )
    let leaseID = await harness.collection.replica.registerActiveDemand(descriptor)
    defer { harness.collection.replica.releaseActiveDemand(leaseID) }
    let token = try #require(await harness.collection.replica.startWorkingSetRecoveryIfNeeded())
    let snapshot = await harness.collection.replica.workingSetRecoverySnapshot()

    // This is the narrow generation race: loss wins after the stable inventory
    // snapshot but before completion. The actor must report stale generation,
    // which recoverActiveWorkingSet maps to ElectricWorkingSetRecoveryRetry
    // rather than CancellationError (so activateDemand retains its lease).
    await harness.collection.replica.invalidateWorkingSetTracker()
    let completion = await harness.collection.replica.completeWorkingSetRecoveryIfStable(
      revision: snapshot.revision,
      epoch: token.epoch,
      lossGeneration: token.lossGeneration
    )
    #expect(completion == .staleGeneration)
    #expect(
      await harness.collection.replica.activeDemandDescriptors() == [descriptor]
    )
  }

  @Test
  func returnedLeaseCancellationFencesRetainedRawTailDuringRecovery() async throws {
    let sessionController = TestSessionController()
    let seed = ReplicaTestRecord(id: "seed", name: "Seed")
    let reseeded = ReplicaTestRecord(id: "seed", name: "Reseeded")
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          ElectricMessage.replicaRecord(seed, offset: "seed-offset", tags: ["shape"], isSubsetSnapshot: true),
          ElectricMessage.replicaSubsetEnd(offset: "seed-offset"),
        ],
        [ElectricMessage.replicaUpToDate(offset: "tail-before-loss")],
        [
          ElectricMessage.replicaRecord(
            reseeded, offset: "reseed-offset", tags: ["shape"], isSubsetSnapshot: true
          ),
          ElectricMessage.replicaSubsetEnd(offset: "reseed-offset"),
        ],
      ],
      syncMode: .onDemand,
      isExactCursorCutoverEnabled: true,
      blocksRequestIndexUntilResumed: 3,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let session = try #require(sessionController.captureAuthenticatedSession())
    // This token survives the returned activation lease. It proves the
    // zero-demand fence rather than merely cancellation of the lease's own
    // keepSynced token.
    let rawToken = harness.collection.keepSynced(session: session)
    defer { rawToken.cancel() }

    let activation = try await harness.collection.activateDemand(
      where: SQLExpression(predicate: .equals(field: "id", value: .string("seed"))), session: session
    )
    try await waitUntilTrueAsync { await harness.http.requestCount() >= 2 }
    await harness.collection.replica.invalidateWorkingSetTracker()
    try await waitUntilTrueAsync { await harness.http.requestCount() == 3 }

    activation.lease.cancel()
    await harness.http.resumeBlockedFetch()
    for _ in 0..<20 { await Task.yield() }

    #expect(await harness.http.requestCount() == 3)
    let requests = await harness.http.capturedRequests()
    #expect(!requests.contains { $0.offset == "-1" })
  }

  @Test
  func taggedTailTrackerLossReplaysActiveLeaseWithoutFullBootstrap() async throws {
    let sessionController = TestSessionController()
    let seed = ReplicaTestRecord(id: "seed", name: "Seed")
    let replayedSeed = ReplicaTestRecord(id: "seed", name: "Seed replayed")
    let postRecoveryTail = ReplicaTestRecord(id: "post-recovery", name: "Post recovery")
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          ElectricMessage.replicaRecord(seed, offset: "seed-offset", tags: ["shape"], isSubsetSnapshot: true),
          ElectricMessage.replicaSubsetEnd(offset: "seed-offset"),
        ],
        [ElectricMessage.replicaUpToDate(offset: "tail-before-loss")],
        [
          ElectricMessage.replicaRecord(
            ReplicaTestRecord(id: "discarded", name: "Discarded"),
            offset: "lost-offset",
            tags: ["shape"]
          ),
          ElectricMessage.replicaUpToDate(offset: "lost-offset"),
        ],
        [
          ElectricMessage.replicaRecord(
            replayedSeed, offset: "reseed-offset", tags: ["shape"], isSubsetSnapshot: true
          ),
          ElectricMessage.replicaSubsetEnd(offset: "reseed-offset"),
        ],
        [
          ElectricMessage.replicaRecord(postRecoveryTail, offset: "tail-after-loss", tags: ["shape"]),
          ElectricMessage.replicaUpToDate(offset: "tail-after-loss"),
        ],
      ],
      syncMode: .onDemand,
      isExactCursorCutoverEnabled: true,
      blocksRequestIndexUntilResumed: 3,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let session = try #require(sessionController.captureAuthenticatedSession())
    let activation = try await harness.collection.activateDemand(
      where: SQLExpression(predicate: .equals(field: "id", value: .string("seed"))), session: session
    )
    defer { activation.lease.cancel() }

    try await waitUntilTrueAsync { await harness.http.requestCount() >= 3 }
    await harness.client.invalidateProcessLocalTracker(
      identity: harness.collection.replica.identity,
      syncMode: .onDemand
    )
    await harness.http.resumeBlockedFetch()
    try await waitUntilTrueAsync {
      await harness.http.requestCount() >= 5
    }
    let requests = await harness.http.capturedRequests()
    #expect(requests[3].offset == "now")
    #expect(requests[3].subset != nil)
    #expect(requests[4].offset == "reseed-offset")
    #expect(requests[4].subset == nil)
    #expect(!requests.contains { $0.offset == "-1" })
    try await waitUntilTrueAsync { harness.store.storedIDs().contains(postRecoveryTail.id) }
    #expect(harness.store.storedIDs() == [replayedSeed.id, postRecoveryTail.id])
  }

  @Test
  func invalidWorkingSetPolicyAndDisguisedTrueDemandFailBeforeHTTP() async throws {
    let sessionController = TestSessionController()
    let session = try #require(sessionController.captureAuthenticatedSession())
    let invalidConfiguration = ReplicaHarness<ReplicaTestRecord>(
      responses: [],
      syncMode: .onDemand,
      supportsDurableRowOwnership: false,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    await #expect(throws: ElectricSyncError.self) {
      try await invalidConfiguration.collection.activateDemand(
        where: SQLExpression(predicate: .equals(field: "id", value: .string("x"))),
        session: session
      )
    }
    #expect(await invalidConfiguration.http.requestCount() == 0)
    await #expect(throws: ElectricSyncError.self) {
      try await invalidConfiguration.collection.ensureSubset(
        where: SQLExpression(predicate: .equals(field: "id", value: .string("x"))),
        session: session
      )
    }
    let rawToken = invalidConfiguration.collection.keepSynced(session: session)
    await Task.yield()
    rawToken.cancel()
    #expect(await invalidConfiguration.http.requestCount() == 0)

    let eligibleConfiguration = ReplicaHarness<ReplicaTestRecord>(
      responses: [],
      syncMode: .onDemand,
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    await #expect(throws: ElectricSyncError.self) {
      try await eligibleConfiguration.collection.activateDemand(
        where: SQLExpression(predicate: .not(.constant(false))),
        session: session
      )
    }
    #expect(await eligibleConfiguration.http.requestCount() == 0)
  }

  @Test
  func semanticEpochTransitionStillFullBootstrapsBeforeOnDemandSubset() async throws {
    let sessionController = TestSessionController()
    do {
      let bootstrapRecord = TaggedReplicaTestRecord(id: "bootstrap", name: "Bootstrap")
      let subsetRecord = TaggedReplicaTestRecord(id: "subset", name: "Subset")
      let harness = ReplicaHarness<TaggedReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              bootstrapRecord,
              offset: "offset-bootstrap",
              tags: ["shape"]
            ),
            ElectricMessage.replicaUpToDate(offset: "offset-bootstrap"),
          ],
          [
            ElectricMessage.replicaRecord(
              subsetRecord,
              offset: "offset-subset",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-subset"),
          ],
        ],
        syncMode: .onDemand,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: .enabled,
        shapeTopology: .staticallySimple
      )
      let replica = harness.collection.replica
      try harness.metadata.updateSyncState(
        collectionId: replica.identity.persistedCursorKey,
        state: SyncState(
          offset: "offset-legacy",
          handle: "handle-legacy",
          cursor: "cursor-legacy",
          isUpToDate: true,
          lastSyncedAt: Date(),
          protocolSemanticEpoch: .legacy
        ),
        transaction: nil
      )
      let snapshot = try #require(sessionController.captureAuthenticatedSession())

      let subset = try await harness.collection.ensureSubset(
        where: SQLExpression("id = 'subset'"),
        session: snapshot
      )

      #expect(subset.appliedRecords == [subsetRecord])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 2)
      #expect(requests[0].offset == "-1")
      #expect(requests[0].subset == nil)
      #expect(requests[0].log == nil)
      #expect(requests[1].offset == "offset-bootstrap")
      #expect(requests[1].subset != nil)
      #expect(requests[1].log == .changesOnly)
    }
  }

  @Test
  func forcedFullBootstrapAppliesDNFBaselineInsteadOfLocalTrackerLoss() async throws {
    let baseline = ReplicaTestRecord(id: "baseline", name: "Baseline")
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [[
        ElectricMessage.replicaRecord(baseline, offset: "baseline-offset", tags: ["shape"]),
        ElectricMessage.replicaUpToDate(offset: "baseline-offset"),
      ]],
      syncMode: .onDemand,
      legacyBootstrapAdmissionController: ElectricLegacyBootstrapAdmissionController(enabled: true),
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let replica = harness.collection.replica
    let stateKey = replica.identity.legacyPersistedCursorKey(syncMode: .onDemand)
    try harness.metadata.updateSyncState(
      collectionId: stateKey,
      state: SyncState(
        offset: "resumed-offset",
        handle: "resumed-handle",
        cursor: "resumed-cursor",
        isUpToDate: true,
        lastSyncedAt: Date(),
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    await harness.client.invalidateProcessLocalTracker(
      identity: replica.identity,
      syncMode: .onDemand
    )

    let batch = try #require(
      try await harness.client.withLegacyBootstrapAdmission(
        identity: replica.identity,
        stage: "forced_dnf_baseline_test",
        syncMode: .onDemand
      ) {
        try await harness.client.pollStream(
          ReplicaTestRecord.self,
          basePredicate: nil,
          shapeTopology: .dnf,
          syncMode: .onDemand,
          live: false,
          forceFullBootstrap: true,
          replicaIdentity: replica.identity
        )
      }
    )
    let request = try #require(await harness.http.capturedRequests().first)
    #expect(request.offset == "-1")
    #expect(request.log == nil)
    let output = try batch.apply(in: harness.store)
    #expect(!output.encounteredTruncate)
    #expect(output.recoveryCause == nil)
    #expect(harness.store.storedIDs() == [baseline.id])
    #expect(
      try harness.metadata.getSyncState(
        collectionId: stateKey,
        transaction: nil
      )?.offset == "baseline-offset"
    )
  }

  @Test
  func wireTruncateOutranksLostDNFTrackerClassification() async throws {
    let tagged = ReplicaTestRecord(id: "tagged", name: "Tagged")
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [[
        ElectricMessage.replicaRecord(tagged, offset: "tagged-offset", tags: ["shape"]),
        ElectricMessage.replicaTruncate(),
      ]],
      syncMode: .onDemand,
      legacyBootstrapAdmissionController: ElectricLegacyBootstrapAdmissionController(enabled: true),
      protocolCapabilityPolicy: .enabled,
      shapeTopology: .dnf,
      trackerContinuityRecoveryPolicy: .replaceExclusiveWorkingSetFromDemandedSubsets
    )
    let replica = harness.collection.replica
    let stateKey = replica.identity.legacyPersistedCursorKey(syncMode: .onDemand)
    try harness.metadata.updateSyncState(
      collectionId: stateKey,
      state: SyncState(
        offset: "resumed-offset",
        handle: "resumed-handle",
        cursor: "resumed-cursor",
        isUpToDate: true,
        lastSyncedAt: Date(),
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    await harness.client.invalidateProcessLocalTracker(
      identity: replica.identity,
      syncMode: .onDemand
    )
    let batch = try #require(
      try await harness.client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        shapeTopology: .dnf,
        syncMode: .onDemand,
        live: false,
        replicaIdentity: replica.identity
      )
    )
    let output = try batch.apply(in: harness.store)
    #expect(output.encounteredTruncate)
    #expect(output.recoveryCause == nil)
  }

  /// The tagged-shape capability gate alone requires process-local tracker
  /// continuity: this model carries no move-out tombstones and would resume
  /// incrementally under legacy segment semantics. The progressive owner still
  /// discards its persisted resume identity and full-bootstraps before the
  /// initial subset demand is served, then resumes from the bootstrap cursor.
  @Test
  func enabledProtocolPolicyFullBootstrapsProgressiveOwnerBeforeSubsetDemand() async throws {
    let sessionController = TestSessionController()
    do {
      let fullRecord = ReplicaTestRecord(id: "full", name: "Full")
      let subsetRecord = ReplicaTestRecord(id: "subset", name: "Subset")
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              fullRecord,
              offset: "offset-bootstrap",
              tags: ["shape"]
            ),
            ElectricMessage.replicaUpToDate(offset: "offset-bootstrap"),
          ],
          [
            ElectricMessage.replicaRecord(
              subsetRecord,
              offset: "offset-subset",
              tags: ["shape"],
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-subset"),
          ],
        ],
        syncMode: .progressive,
        supportsDurableRowOwnership: false,
        protocolCapabilityPolicy: .enabled
      )
      let replica = harness.collection.replica
      let stateKey = replica.identity.legacyPersistedCursorKey(syncMode: .progressive)
      try harness.metadata.updateSyncState(
        collectionId: stateKey,
        state: SyncState(
          offset: "offset-stale",
          handle: "handle-stale",
          cursor: "cursor-stale",
          isUpToDate: true,
          lastSyncedAt: Date(),
          protocolSemanticEpoch: .taggedShape1_7_7
        ),
        transaction: nil
      )
      #expect(replica.isTrackerContinuityUnavailable)

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let result = try await harness.collection.ensureSubset(
        where: SQLExpression("id = 'subset'"),
        session: snapshot
      )

      #expect(result.appliedRecords == [subsetRecord])
      let requests = await harness.http.capturedRequests()
      #expect(requests.count == 2)
      #expect(requests[0].offset == "-1")
      #expect(requests[0].subset == nil)
      #expect(requests[1].offset == "offset-bootstrap")
      #expect(requests[1].subset != nil)
      #expect(!replica.isTrackerContinuityUnavailable)
    }
  }

  @Test
  func nonterminalOwnerSnapshotChunkDurablyStagesTrackerLossFence() async throws {
    let taggedMessages =
      (0..<201).map { index in
        ElectricMessage.replicaRecord(
          TaggedReplicaTestRecord(id: "\(index)", name: "Tagged \(index)"),
          offset: "offset-\(index)",
          tags: ["shape"],
          isSubsetSnapshot: true
        )
      }
      + [ElectricMessage.replicaSubsetEnd(offset: "offset-201")]
    let harness = ReplicaHarness<TaggedReplicaTestRecord>(
      responses: [taggedMessages],
      syncMode: .onDemand,
      supportsDurableRowOwnership: false,
      shapeTopology: .dnf
    )
    let replica = harness.collection.replica
    let stateKey = replica.identity.legacyPersistedCursorKey(syncMode: .onDemand)
    try harness.metadata.updateSyncState(
      collectionId: stateKey,
      state: SyncState(
        offset: "offset-stale",
        handle: "handle-stale",
        cursor: "cursor-stale",
        isUpToDate: true,
        lastSyncedAt: Date()
      ),
      transaction: nil
    )

    let batch = try #require(
      try await replica.client.requestSnapshot(
        TaggedReplicaTestRecord.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(predicate: SQLExpression("id IS NOT NULL")),
        syncMode: .onDemand,
        replicaIdentity: replica.identity
      )
    )
    let chunks = batch.chunked(maxMessages: 200)
    #expect(chunks.count == 2)

    let output = try chunks[0].apply(in: harness.store)

    #expect(output.encounteredTruncate)
    #expect(output.requiresReplacementSwap)
    #expect(harness.store.storedIDs().isEmpty)
    let fencedState = try #require(
      try harness.metadata.getSyncState(collectionId: stateKey, transaction: nil)
    )
    #expect(!fencedState.canResumeWithoutFullBootstrap)
    #expect(fencedState.offset == "-1")
  }

  @Test
  func equivalentOwnerSnapshotDemandSurvivesOneCallerCancellation() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              ReplicaTestRecord(id: "1", name: "Shared"),
              offset: "offset-shared",
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-shared"),
          ]
        ],
        syncMode: .onDemand,
        blocksFirstFetchUntilResumed: true
      )

      let first = Task {
        try await harness.collection.query(where: SQLExpression("id = '1'"))
      }
      try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }
      let second = Task {
        try await harness.collection.query(where: SQLExpression("id = '1'"))
      }
      await Task.yield()
      first.cancel()
      await harness.http.resumeFirstFetch()

      _ = try await second.value
      #expect(harness.store.storedIDs() == ["1"])
      #expect(await harness.http.requestCount() == 1)
      _ = try? await first.value
    }
  }

  @Test
  func subsetOwnerSnapshotDemandReusesAnInflightSupersetRequest() async throws {
    let sessionController = TestSessionController()
    do {
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            ElectricMessage.replicaRecord(
              ReplicaTestRecord(id: "1", name: "Covered by wider request"),
              offset: "offset-superset",
              isSubsetSnapshot: true
            ),
            ElectricMessage.replicaSubsetEnd(offset: "offset-superset"),
          ]
        ],
        syncMode: .onDemand,
        blocksFirstFetchUntilResumed: true
      )

      let widerWindow = SQLExpression("id IS NOT NULL")
      let visibleDay = SQLExpression("id = '1'")
      let widerDemand = Task {
        try await harness.collection.query(where: widerWindow)
      }
      try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }

      let nestedDemand = Task {
        try await harness.collection.query(where: visibleDay)
      }
      await Task.yield()
      #expect(await harness.http.requestCount() == 1)

      await harness.http.resumeFirstFetch()
      _ = try await widerDemand.value
      _ = try await nestedDemand.value
      #expect(await harness.http.requestCount() == 1)
    }
  }

  @Test
  func progressiveQueryAndOwnerSnapshotDemandDoNotCoalesce() async throws {
    let sessionController = TestSessionController()
    do {
      let response = [
        ElectricMessage.replicaRecord(
          ReplicaTestRecord(id: "1", name: "Shared descriptor"),
          offset: "offset-shared",
          isSubsetSnapshot: true
        ),
        ElectricMessage.replicaSubsetEnd(offset: "offset-shared"),
      ]
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [response, response],
        syncMode: .progressive,
        blocksFirstFetchUntilResumed: true
      )
      let predicate = SQLExpression("id = '1'")
      let snapshot = try #require(sessionController.captureAuthenticatedSession())

      let progressiveQuery = Task {
        try await harness.collection.query(where: predicate)
      }
      try await waitUntilTrueAsync { await harness.http.requestCount() == 1 }

      let ownerSnapshotDemand = Task {
        try await harness.collection.ensureSubset(
          where: predicate,
          session: snapshot
        )
      }
      try await waitUntilTrueAsync { await harness.http.requestCount() == 2 }
      #expect(await harness.http.requestCount() == 2)

      await harness.http.resumeFirstFetch()
      _ = try await progressiveQuery.value
      _ = try await ownerSnapshotDemand.value
    }
  }

  @Test
  func snapshotFilteringUsesHighestTransactionIDAndKeepsUntrackedChanges() async throws {
    let trackedKey = #""public"."user"/"tracked""#
    let untrackedKey = #""public"."user"/"untracked""#
    let trackedVisibleChange = ElectricMessage.replicaRecord(
      ReplicaTestRecord(id: "tracked", name: "Already visible"),
      offset: "tracked-visible",
      key: trackedKey,
      txids: [10, 150]
    )
    let trackedChangeWithoutTransaction = ElectricMessage.replicaRecord(
      ReplicaTestRecord(id: "tracked", name: "No transaction metadata"),
      offset: "tracked-no-txid",
      key: trackedKey
    )
    let untrackedVisibleChange = ElectricMessage.replicaRecord(
      ReplicaTestRecord(id: "untracked", name: "Outside snapshot"),
      offset: "untracked-visible",
      key: untrackedKey,
      txids: [150]
    )
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          trackedVisibleChange,
          trackedChangeWithoutTransaction,
          untrackedVisibleChange,
          .replicaUpToDate(offset: "up-to-date"),
        ]
      ],
      syncMode: .progressive
    )
    let replica = harness.collection.replica
    await replica.installSnapshotTracker(messages: [
      .replicaRecord(
        ReplicaTestRecord(id: "tracked", name: "Subset row"),
        offset: "subset",
        key: trackedKey,
        isSubsetSnapshot: true
      ),
      .replicaSubsetSnapshotEnd(
        offset: "subset",
        snapshot: PostgresSnapshot(xmin: "50", xmax: "200", xipList: [])
      ),
      .replicaSubsetEnd(offset: "subset"),
    ])

    let batch = try #require(
      try await replica.client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        syncMode: .progressive,
        live: false,
        replicaIdentity: replica.identity
      )
    )
    let filtered = await replica.filterLiveBatch(batch)

    #expect(!filtered.messages.contains { $0.offset == trackedVisibleChange.offset })
    #expect(
      filtered.messages.contains { $0.offset == trackedChangeWithoutTransaction.offset }
    )
    #expect(filtered.messages.contains { $0.offset == untrackedVisibleChange.offset })
  }

  @Test
  func snapshotFilteringRetiresOnlyBoundariesPassedByLiveTransactions() async throws {
    let firstKey = #""public"."user"/"first""#
    let secondKey = #""public"."user"/"second""#
    let firstHistoricalChange = ElectricMessage.replicaRecord(
      ReplicaTestRecord(id: "first", name: "First historical"),
      offset: "first-historical",
      key: firstKey,
      txids: [60]
    )
    let secondHistoricalChange = ElectricMessage.replicaRecord(
      ReplicaTestRecord(id: "second", name: "Second historical"),
      offset: "second-historical",
      key: secondKey,
      txids: [60]
    )
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          .replicaRecord(
            ReplicaTestRecord(id: "other", name: "Advances first boundary"),
            offset: "boundary-advance",
            key: #""public"."user"/"other""#,
            txids: [250]
          ),
          firstHistoricalChange,
          secondHistoricalChange,
          .replicaUpToDate(offset: "up-to-date"),
        ]
      ],
      syncMode: .progressive
    )
    let replica = harness.collection.replica
    await replica.installSnapshotTracker(messages: [
      .replicaRecord(
        ReplicaTestRecord(id: "first", name: "First subset"),
        offset: "first-subset",
        key: firstKey,
        isSubsetSnapshot: true
      ),
      .replicaSubsetSnapshotEnd(
        offset: "first-subset",
        snapshot: PostgresSnapshot(xmin: "50", xmax: "200", xipList: [])
      ),
      .replicaSubsetEnd(offset: "first-subset"),
    ])
    await replica.installSnapshotTracker(messages: [
      .replicaRecord(
        ReplicaTestRecord(id: "second", name: "Second subset"),
        offset: "second-subset",
        key: secondKey,
        isSubsetSnapshot: true
      ),
      .replicaSubsetSnapshotEnd(
        offset: "second-subset",
        snapshot: PostgresSnapshot(xmin: "50", xmax: "400", xipList: [])
      ),
      .replicaSubsetEnd(offset: "second-subset"),
    ])

    let batch = try #require(
      try await replica.client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        syncMode: .progressive,
        live: false,
        replicaIdentity: replica.identity
      )
    )
    let filtered = await replica.filterLiveBatch(batch)

    #expect(filtered.messages.contains { $0.offset == firstHistoricalChange.offset })
    #expect(!filtered.messages.contains { $0.offset == secondHistoricalChange.offset })
  }

  @Test
  func progressiveBootstrapKeepsCatchUpCorrectionForHistoricalSnapshotRow() async throws {
    let rowKey = #""public"."user"/"current-user""#
    let temporaryAuthoritativeRow = ReplicaTestRecord(
      id: "current-user",
      name: "Authoritative onboarding null"
    )
    let historicalBootstrapRow = ReplicaTestRecord(
      id: "current-user",
      name: "Historical onboarded value"
    )
    let catchUpCorrection = ReplicaTestRecord(
      id: "current-user",
      name: "Catch-up onboarding null"
    )
    let historicalMessage = ElectricMessage.replicaRecord(
      historicalBootstrapRow,
      offset: "historical-snapshot",
      key: rowKey
    )
    let correctionMessage = ElectricMessage.replicaRecord(
      catchUpCorrection,
      offset: "catch-up-null",
      key: rowKey,
      txids: [100]
    )
    let harness = ReplicaHarness<ReplicaTestRecord>(
      responses: [
        [
          historicalMessage,
          correctionMessage,
          .replicaUpToDate(offset: "up-to-date"),
        ]
      ],
      syncMode: .progressive
    )
    let replica = harness.collection.replica
    let subsetMessage = ElectricMessage.replicaRecord(
      temporaryAuthoritativeRow,
      offset: "subset-current",
      key: rowKey,
      isSubsetSnapshot: true
    )
    let subsetEnd = ElectricMessage(
      payload: Data(),
      offset: "subset-current",
      handle: "handle-subset-current",
      kind: .snapshot,
      control: .subsetEnd,
      postgresSnapshot: PostgresSnapshot(xmin: "200", xmax: "300", xipList: []),
      isSubsetSnapshot: true
    )
    await replica.installSnapshotTracker(messages: [subsetMessage, subsetEnd])

    let batch = try #require(
      try await replica.client.pollStream(
        ReplicaTestRecord.self,
        basePredicate: nil,
        syncMode: .progressive,
        live: false,
        forceFullBootstrap: true,
        replicaIdentity: replica.identity
      )
    )
    #expect(batch.messages.contains { $0.offset == correctionMessage.offset })

    let liveFilteredBatch = await replica.filterLiveBatch(batch)
    #expect(liveFilteredBatch.messages.contains { $0.offset == historicalMessage.offset })
    #expect(!liveFilteredBatch.messages.contains { $0.offset == correctionMessage.offset })

    let replacementBatch = await replica.filterLiveBatch(
      batch.filteringMessages(
        [
          historicalMessage,
          .replicaSnapshotEnd(offset: "full-snapshot-end"),
          correctionMessage,
          .replicaUpToDate(offset: "up-to-date"),
        ]
      )
    )
    #expect(replacementBatch.messages.contains { $0.offset == historicalMessage.offset })
    #expect(replacementBatch.messages.contains { $0.offset == correctionMessage.offset })
  }

  @Test
  func liveBatchFiltersAfterEnteringPublicationFence() async throws {
    let sessionController = TestSessionController()
    do {
      let rowKey = #""public"."user"/"current-user""#
      let subsetRow = ReplicaTestRecord(id: "current-user", name: "Subset row")
      let historicalLiveRow = ReplicaTestRecord(
        id: "current-user",
        name: "Historical live row"
      )
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            .replicaRecord(
              historicalLiveRow,
              offset: "historical-live",
              key: rowKey,
              txids: [100]
            ),
            .replicaUpToDate(offset: "historical-live"),
          ]
        ],
        syncMode: .eager,
        cancellationAwareBlockedRequestIndex: 2
      )
      let replica = harness.collection.replica
      harness.store.upsert(id: subsetRow.id, name: subsetRow.name)

      let publicationHold = ReplicaOperationGate()
      let holdingTask = Task {
        try await replica.withStreamPublication {
          await publicationHold.enterAndWait()
        }
      }
      try await waitUntilTrueAsync { await publicationHold.hasEntered }

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let token = harness.collection.keepSynced(session: snapshot)
      defer { token.cancel() }
      try await waitUntilTrueAsync { await replica.publicationWaiterCount == 1 }

      await replica.installSnapshotTracker(messages: [
        .replicaRecord(
          subsetRow,
          offset: "subset-current",
          key: rowKey,
          isSubsetSnapshot: true
        ),
        .replicaSubsetSnapshotEnd(
          offset: "subset-current",
          snapshot: PostgresSnapshot(xmin: "200", xmax: "300", xipList: [])
        ),
        .replicaSubsetEnd(offset: "subset-current"),
      ])
      await publicationHold.release()
      _ = try await holdingTask.value

      try await waitUntilTrueAsync { await harness.http.requestCount() >= 2 }
      #expect(harness.store.name(id: subsetRow.id) == subsetRow.name)
    }
  }

  @Test
  func forcedOwnerReplacementPreservesCatchUpAndRetiresSupersededSnapshotTracker() async throws {
    let sessionController = TestSessionController()
    do {
      let rowKey = #""public"."user"/"current-user""#
      let historicalBootstrapRow = ReplicaTestRecord(
        id: "current-user",
        name: "Historical onboarded value"
      )
      let catchUpCorrection = ReplicaTestRecord(
        id: "current-user",
        name: "Catch-up onboarding null"
      )
      let postReplacementUpdate = ReplicaTestRecord(
        id: "current-user",
        name: "Post-replacement update"
      )
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [
            .replicaRecord(
              historicalBootstrapRow,
              offset: "historical-snapshot",
              tags: ["shape"],
              key: rowKey
            ),
            .replicaSnapshotEnd(offset: "full-snapshot-end"),
            .replicaRecord(
              catchUpCorrection,
              offset: "catch-up-null",
              tags: ["shape"],
              key: rowKey,
              txids: [100]
            ),
            .replicaUpToDate(offset: "replacement-up-to-date"),
          ],
          [
            .replicaRecord(
              postReplacementUpdate,
              offset: "post-replacement-update",
              tags: ["shape"],
              key: rowKey,
              txids: [110]
            ),
            .replicaUpToDate(offset: "post-replacement-up-to-date"),
          ],
        ],
        syncMode: .progressive,
        supportsDurableRowOwnership: false,
        cancellationAwareBlockedRequestIndex: 3,
        protocolCapabilityPolicy: .enabled
      )
      let replica = harness.collection.replica
      await replica.installSnapshotTracker(messages: [
        .replicaRecord(
          ReplicaTestRecord(id: "current-user", name: "Temporary subset row"),
          offset: "subset-current",
          key: rowKey,
          isSubsetSnapshot: true
        ),
        .replicaSubsetSnapshotEnd(
          offset: "subset-current",
          snapshot: PostgresSnapshot(xmin: "200", xmax: "300", xipList: [])
        ),
        .replicaSubsetEnd(offset: "subset-current"),
      ])

      let snapshot = try #require(sessionController.captureAuthenticatedSession())
      let token = harness.collection.keepSynced(session: snapshot)
      defer { token.cancel() }

      try await waitUntilTrueAsync { await harness.http.requestCount() >= 3 }
      #expect(harness.store.name(id: "current-user") == postReplacementUpdate.name)
      let requests = await harness.http.capturedRequests()
      #expect(requests[0].offset == "-1")
      #expect(requests[0].subset == nil)
      #expect(requests[1].offset == "replacement-up-to-date")
    }
  }

  @Test
  func replacementRollbackKeepsPreviousGenerationWhenLateChunkFails() async throws {
    let sessionController = TestSessionController()
    do {
      let replacementMessages =
        (0..<201).map { index in
          ElectricMessage.replicaRecord(
            ReplicaTestRecord(id: "\(index)", name: "Replacement \(index)"),
            offset: "replacement-\(index)",
            isSubsetSnapshot: true
          )
        }
        + [ElectricMessage.replicaSubsetEnd(offset: "replacement-201")]
      let transactionGate = ReplicaTransactionGate(
        blockedInvocation: 2,
        failsAfterRelease: true,
        blocksAfterOperation: true
      )
      let harness = ReplicaHarness<ReplicaTestRecord>(
        responses: [
          [ElectricMessage.replicaTruncate()],
          replacementMessages,
        ],
        syncMode: .onDemand,
        transactionGate: transactionGate
      )
      harness.store.upsert(id: "existing", name: "Existing generation")
      let preservedPredicate = PredicateHash(value: "preserved-coverage")
      transactionGate.beforeBlockedOperation {
        harness.metadata.seedFetched(table: ReplicaTestRecord.tableName, predicate: preservedPredicate)
        harness.metadata.seedOwnedRow(table: ReplicaTestRecord.tableName, rowKey: "existing")
        harness.metadata.seedOwnershipTags(
          table: ReplicaTestRecord.tableName,
          rowKey: "existing",
          tags: ["preserved-tag"]
        )
        try? harness.metadata.updateSyncState(
          collectionId: harness.collection.replica.identity.persistedCursorKey,
          state: SyncState(
            offset: "preserved-cursor",
            handle: "preserved-handle",
            cursor: "cursor",
            isUpToDate: true,
            lastSyncedAt: Date()
          ),
          transaction: nil
        )
      }

      let queryTask = Task {
        try await harness.collection.query(where: SQLExpression("id IS NOT NULL"))
      }
      try await waitUntilTrue {
        transactionGate.hasReachedBlockedInvocation()
      }

      #expect(harness.collection.replica.ownerState == .replacing)
      #expect(harness.store.storedIDs() == ["existing"])

      transactionGate.release()
      do {
        _ = try await queryTask.value
        Issue.record("Expected the later replacement chunk to fail")
      } catch {
        #expect(harness.collection.replica.ownerState == .dormant)
        #expect(harness.store.storedIDs() == ["existing"])
        #expect(
          try harness.metadata.hasFetched(
            table: ReplicaTestRecord.tableName,
            predicate: preservedPredicate,
            transaction: nil
          )
        )
        #expect(
          try harness.metadata.getSyncState(
            collectionId: harness.collection.replica.identity.persistedCursorKey,
            transaction: nil
          )?.offset == "preserved-cursor"
        )
        #expect(
          harness.metadata.ownershipTags(table: ReplicaTestRecord.tableName) == [
            "existing": ["preserved-tag"]
          ]
        )
      }
    }
  }
}

// MARK: - Harness

private final class ReplicaHarness<Model: ElectricCollectionModel>: @unchecked Sendable {
  let metadata: ReplicaInMemoryMetadataProvider
  let http: ReplicaInMemoryHTTPClientProvider
  let client: ElectricSyncClientImpl
  let store = ReplicaTestRecordStore()
  let collection: ElectricCollection<Model>

  init(
    responses: [[ElectricMessage]],
    syncMode: ElectricCollectionSyncMode,
    supportsDurableRowOwnership: Bool = true,
    isExactCursorCutoverEnabled: Bool = false,
    gcTime: TimeInterval = ElectricCollectionStreamManager.defaultGCTime,
    blocksFirstFetchUntilResumed: Bool = false,
    blocksRequestIndexUntilResumed: Int? = nil,
    cancellationAwareBlockedRequestIndex: Int? = nil,
    transactionGate: ReplicaTransactionGate? = nil,
    legacyBootstrapAdmissionController: ElectricLegacyBootstrapAdmissionController? = nil,
    protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy = .defaultOff,
    // Existing lifecycle fixtures model a subquery-free stream. Tagged/DNF
    // scenarios must opt in explicitly so the fixture cannot accidentally
    // assert an incremental-resume contract for DNF state.
    shapeTopology: ElectricShapeTopology = .staticallySimple,
    trackerContinuityRecoveryPolicy: ElectricTrackerContinuityRecoveryPolicy = .fullBootstrap,
    trackerRebuildAdmissionError: Bool = false,
    trackerRebuildOwnership: [String: [String]]? = nil,
    admitsFreshOnDemandPristineOwner: Bool = false,
    pristineOwnerAdmissionError: Bool = false,
    pristineOwnerAdmissionResults: [ReplicaPristineOwnerAdmissionResult] = [],
    protocolErrorRequestIndexes: Set<Int> = [],
    logger: any LogProvider = NoopLogProvider()
  ) {
    self.metadata = ReplicaInMemoryMetadataProvider(
      supportsDurableRowOwnership: supportsDurableRowOwnership,
      trackerRebuildAdmissionError: trackerRebuildAdmissionError,
      trackerRebuildOwnership: trackerRebuildOwnership,
      admitsFreshOnDemandPristineOwner: admitsFreshOnDemandPristineOwner,
      pristineOwnerAdmissionError: pristineOwnerAdmissionError,
      pristineOwnerAdmissionResults: pristineOwnerAdmissionResults
    )
    let metadata = self.metadata
    let http = ReplicaInMemoryHTTPClientProvider(
      responses: responses,
      blocksFirstFetchUntilResumed: blocksFirstFetchUntilResumed,
      blocksRequestIndexUntilResumed: blocksRequestIndexUntilResumed,
      cancellationAwareBlockedRequestIndex: cancellationAwareBlockedRequestIndex,
      protocolErrorRequestIndexes: protocolErrorRequestIndexes
    )
    self.http = http
    let client = ElectricSyncClientImpl(
      configuration: ElectricSyncClientConfiguration(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        logger: logger,
        isExactCursorCutoverEnabled: isExactCursorCutoverEnabled,
        legacyBootstrapAdmissionController: legacyBootstrapAdmissionController,
        protocolCapabilityPolicy: protocolCapabilityPolicy
      )
    )
    self.client = client
    let configuration = ElectricCollectionConfiguration(
      modelType: Model.self,
      syncMode: syncMode,
      shapeTopology: shapeTopology,
      trackerContinuityRecoveryPolicy: trackerContinuityRecoveryPolicy
    )
    let store = self.store
    let replica = ElectricShapeReplica<Model>(
      identity: ElectricReplicaIdentity(
        modelType: Model.self,
        modelIdentifier: Model.collectionIdentifier,
        basePredicate: nil
      ),
      basePredicate: nil,
      syncMode: syncMode,
      client: client,
      cacheProvider: ReplicaStoreBackedCacheProvider(store: store),
      transactionRunner: { operation in
        try await transactionGate?.beforeOperation()
        let transactionStore = store.transactionCopy()
        let transactionMetadata = metadata.transactionStorageCopy()
        try operation(
          ReplicaTransactionContext(
            recordStore: transactionStore,
            metadataStorage: transactionMetadata
          )
        )
        try await transactionGate?.afterOperation()
        store.replaceContents(with: transactionStore)
        metadata.replaceTransactionStorage(transactionMetadata)
      },
      gcTime: gcTime
    )
    self.collection = ElectricCollection(
      configuration: configuration,
      replica: replica
    )
  }
}

// MARK: - Test models

protocol ReplicaLifecycleTestModel: ElectricCollectionModel, Codable, Equatable {
  var id: String { get }
  var name: String { get }
  var enrichment: String? { get }
}

extension ReplicaLifecycleTestModel {
  var enrichment: String? { nil }

  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(endpoint: "/shapes/\(tableName)", selectedColumns: ["id", "name"])
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
  ) throws -> ProcessedMessage<Self> {
    guard !message.payload.isEmpty else {
      return ProcessedMessage(
        records: [],
        metadata: StoreMetadata(
          offset: message.offset,
          handle: message.handle,
          cursor: message.cursor,
          operation: .update
        )
      )
    }
    let record = try JSONDecoder().decode(Self.self, from: message.payload)
    if record.name == "__requires_hydration__" {
      return ProcessedMessage(
        records: [],
        metadata: StoreMetadata(
          offset: message.offset,
          handle: message.handle,
          cursor: message.cursor,
          operation: .update
        ),
        missingRowKeys: [message.key].compactMap { $0 }
      )
    }
    ((transaction as? ReplicaTransactionContext)?.recordStore
      ?? transaction as? ReplicaTestRecordStore)?.upsert(
      id: record.id,
      name: record.name,
      enrichment: record.enrichment
    )
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

  static func truncate(transaction: Any?) throws {
    ((transaction as? ReplicaTransactionContext)?.recordStore
      ?? transaction as? ReplicaTestRecordStore)?.clear()
  }

  static func deleteByKey(_ key: String, transaction: Any?) throws {
    ((transaction as? ReplicaTransactionContext)?.recordStore
      ?? transaction as? ReplicaTestRecordStore)?.delete(id: key)
  }
}

struct ReplicaTestRecord: ReplicaLifecycleTestModel {
  let id: String
  let name: String

  static var tableName: String { "replica_owner_test_records" }
}

private struct TaggedReplicaTestRecord: ReplicaLifecycleTestModel {
  let id: String
  let name: String

  static var tableName: String { "tagged_replica_owner_test_records" }
  static var moveOutTombstoneTimeToLive: TimeInterval? { 60 }
}

private struct SharedReplicaTestRecord: ReplicaLifecycleTestModel {
  let id: String
  let name: String

  static var tableName: String { "shared_replica_owner_test_records" }
  static var electricLocalTableOwnership: ElectricLocalTableOwnership { .shared }
}

private struct LegacyMappedTestRecord: ReplicaLifecycleTestModel {
  let id: String
  let name: String

  static var tableName: String { "legacy_mapped_test_records" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(
      endpoint: "/shapes/\(tableName)",
      selectedColumns: ["id", "name"],
      legacyCursorVersion: "1"
    )
  }
}

private struct WidenedReplicaTestRecord: ReplicaLifecycleTestModel {
  let id: String
  let name: String
  let enrichment: String?

  static var tableName: String { "widened_replica_test_records" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(
      endpoint: "/shapes/\(tableName)",
      selectedColumns: ["id", "name", "enrichment"],
      legacyCursorVersion: nil
    )
  }
}

// MARK: - Test infrastructure

private final class ReplicaTestRecordStore: @unchecked Sendable {
  private struct Snapshot: Sendable {
    let ids: Set<String>
    let names: [String: String]
    let enrichments: [String: String]
  }

  private let lock = NSLock()
  private var ids: Set<String> = []
  private var names: [String: String] = [:]
  private var enrichments: [String: String] = [:]

  func transactionCopy() -> ReplicaTestRecordStore {
    let copy = ReplicaTestRecordStore()
    copy.restore(snapshot())
    return copy
  }

  func replaceContents(with other: ReplicaTestRecordStore) {
    restore(other.snapshot())
  }

  func upsert(id: String, name: String? = nil, enrichment: String? = nil) {
    lock.withLock {
      ids.insert(id)
      names[id] = name
      enrichments[id] = enrichment
    }
  }

  func delete(id: String) {
    lock.withLock {
      ids.remove(id)
      names[id] = nil
      enrichments[id] = nil
    }
  }

  func clear() {
    lock.withLock {
      ids = []
      names = [:]
      enrichments = [:]
    }
  }

  func storedIDs() -> Set<String> {
    lock.withLock { ids }
  }

  func enrichment(id: String) -> String? {
    lock.withLock { enrichments[id] }
  }

  func name(id: String) -> String? {
    lock.withLock { names[id] }
  }

  private func snapshot() -> Snapshot {
    lock.withLock {
      Snapshot(ids: ids, names: names, enrichments: enrichments)
    }
  }

  private func restore(_ snapshot: Snapshot) {
    lock.withLock {
      ids = snapshot.ids
      names = snapshot.names
      enrichments = snapshot.enrichments
    }
  }
}

private actor ReplicaOperationGate {
  private var entered = false
  private var continuation: CheckedContinuation<Void, Never>?

  var hasEntered: Bool {
    entered
  }

  func enterAndWait() async {
    entered = true
    await withCheckedContinuation { continuation in
      self.continuation = continuation
    }
  }

  func release() {
    continuation?.resume()
    continuation = nil
  }
}

private struct ReplicaStoreBackedCacheProvider: DataCacheProvider {
  let store: ReplicaTestRecordStore

  func load<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> [T]
  where T: ElectricCollectionModel {
    []
  }

  func hasData<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> Bool
  where T: ElectricCollectionModel {
    !store.storedIDs().isEmpty
  }
}

private actor ReplicaInMemoryHTTPClientProvider: HTTPClientProvider {
  private var responses: [[ElectricMessage]]
  private var requests: [ElectricShapeRequest] = []
  private let blocksFirstFetchUntilResumed: Bool
  private let blocksRequestIndexUntilResumed: Int?
  private let cancellationAwareBlockedRequestIndex: Int?
  private let protocolErrorRequestIndexes: Set<Int>
  private var firstFetchReleaseRequested = false
  private var firstFetchContinuation: CheckedContinuation<Void, Never>?
  private var blockedFetchReleaseRequested = false
  private var blockedFetchReleaseContinuation: CheckedContinuation<Void, Never>?
  private var blockedFetchContinuation: CheckedContinuation<[ElectricMessage], Error>?
  private var cancelledFetches = 0
  private var afterNextProtocolError: (@Sendable () -> Void)?

  init(
    responses: [[ElectricMessage]],
    blocksFirstFetchUntilResumed: Bool = false,
    blocksRequestIndexUntilResumed: Int? = nil,
    cancellationAwareBlockedRequestIndex: Int? = nil,
    protocolErrorRequestIndexes: Set<Int> = []
  ) {
    self.responses = responses
    self.blocksFirstFetchUntilResumed = blocksFirstFetchUntilResumed
    self.blocksRequestIndexUntilResumed = blocksRequestIndexUntilResumed
    self.cancellationAwareBlockedRequestIndex = cancellationAwareBlockedRequestIndex
    self.protocolErrorRequestIndexes = protocolErrorRequestIndexes
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    requests.append(request)
    if protocolErrorRequestIndexes.contains(requests.count) {
      let action = afterNextProtocolError
      afterNextProtocolError = nil
      action?()
      throw ReplicaProtocolRecoveryError()
    }
    if blocksFirstFetchUntilResumed, requests.count == 1, !firstFetchReleaseRequested {
      await withCheckedContinuation { continuation in
        if firstFetchReleaseRequested {
          continuation.resume()
        } else {
          firstFetchContinuation = continuation
        }
      }
    }
    if requests.count == blocksRequestIndexUntilResumed, !blockedFetchReleaseRequested {
      await withCheckedContinuation { continuation in
        if blockedFetchReleaseRequested {
          continuation.resume()
        } else {
          blockedFetchReleaseContinuation = continuation
        }
      }
    }
    if requests.count == cancellationAwareBlockedRequestIndex {
      return try await withTaskCancellationHandler {
        try await withCheckedThrowingContinuation { continuation in
          blockedFetchContinuation = continuation
        }
      } onCancel: {
        Task {
          await self.cancelBlockedFetch()
        }
      }
    }
    guard !responses.isEmpty else { return [] }
    return responses.removeFirst()
  }

  private func cancelBlockedFetch() {
    cancelledFetches += 1
    blockedFetchContinuation?.resume(throwing: CancellationError())
    blockedFetchContinuation = nil
  }

  func cancelBlockedFetchForTest() {
    cancelBlockedFetch()
  }

  func afterNextProtocolError(_ action: @escaping @Sendable () -> Void) {
    afterNextProtocolError = action
  }

  func resumeFirstFetch() {
    firstFetchReleaseRequested = true
    firstFetchContinuation?.resume()
    firstFetchContinuation = nil
  }

  func resumeBlockedFetch() {
    blockedFetchReleaseRequested = true
    blockedFetchReleaseContinuation?.resume()
    blockedFetchReleaseContinuation = nil
  }

  func requestCount() -> Int {
    requests.count
  }

  func capturedRequests() -> [ElectricShapeRequest] {
    requests
  }

  func cancelledFetchCount() -> Int {
    cancelledFetches
  }
}

private final class ReplicaTransactionGate: @unchecked Sendable {
  private let lock = NSLock()
  private let blockedInvocation: Int
  private let failsAfterRelease: Bool
  private let blocksAfterOperation: Bool
  private var invocationCount = 0
  private var releaseRequested = false
  private var continuation: CheckedContinuation<Void, Never>?
  private var beforeBlockedOperationAction: (@Sendable () -> Void)?

  init(
    blockedInvocation: Int,
    failsAfterRelease: Bool,
    blocksAfterOperation: Bool = false
  ) {
    self.blockedInvocation = blockedInvocation
    self.failsAfterRelease = failsAfterRelease
    self.blocksAfterOperation = blocksAfterOperation
  }

  func beforeOperation() async throws {
    let invocation = lock.withLock {
      invocationCount += 1
      return invocationCount
    }
    if blocksAfterOperation, invocation == blockedInvocation {
      let action = lock.withLock { beforeBlockedOperationAction }
      action?()
      return
    }
    guard invocation == blockedInvocation else { return }

    try await waitAndMaybeFail()
  }

  func afterOperation() async throws {
    let invocation = lock.withLock { invocationCount }
    guard blocksAfterOperation, invocation == blockedInvocation else { return }

    try await waitAndMaybeFail()
  }

  private func waitAndMaybeFail() async throws {
    await withCheckedContinuation { continuation in
      let shouldResume = lock.withLock {
        if releaseRequested {
          return true
        }
        self.continuation = continuation
        return false
      }
      if shouldResume {
        continuation.resume()
      }
    }
    if failsAfterRelease {
      throw ElectricSyncError.fetchFailed("Injected later-chunk failure")
    }
  }

  func hasReachedBlockedInvocation() -> Bool {
    lock.withLock { invocationCount >= blockedInvocation }
  }

  func beforeBlockedOperation(_ action: @escaping @Sendable () -> Void) {
    lock.withLock { beforeBlockedOperationAction = action }
  }

  func release() {
    let continuation = lock.withLock {
      releaseRequested = true
      let continuation = self.continuation
      self.continuation = nil
      return continuation
    }
    continuation?.resume()
  }
}

private enum ReplicaPristineOwnerAdmissionResult {
  case admit
  case refuse
  case error
}

private struct ReplicaProtocolRecoveryError: ElectricProtocolIncompatibilityError {
  let electricProtocolQuarantine = ElectricProtocolQuarantine(
    reason: .control,
    detail: "simulated protocol recovery error",
    compatibilityMayChangeAfterFullBootstrap: true
  )
}

private final class ReplicaMetadataStorage: @unchecked Sendable {
  let lock = NSLock()
  var fetched: [String: [PredicateHash: FetchedPredicate]] = [:]
  var ranges: [String: [String: [FetchedRange]]] = [:]
  var syncStates: [String: SyncState] = [:]
  var ownedRowKeys: [String: Set<String>] = [:]
  var ownershipTags: [String: [String: [String]]] = [:]

  func copy() -> ReplicaMetadataStorage {
    let result = ReplicaMetadataStorage()
    lock.withLock {
      result.fetched = fetched
      result.ranges = ranges
      result.syncStates = syncStates
      result.ownedRowKeys = ownedRowKeys
      result.ownershipTags = ownershipTags
    }
    return result
  }

  func replaceContents(with other: ReplicaMetadataStorage) {
    let copied = other.copy()
    lock.withLock {
      fetched = copied.fetched
      ranges = copied.ranges
      syncStates = copied.syncStates
      ownedRowKeys = copied.ownedRowKeys
      ownershipTags = copied.ownershipTags
    }
  }
}

private final class ReplicaTransactionContext: @unchecked Sendable {
  let recordStore: ReplicaTestRecordStore
  let metadataStorage: ReplicaMetadataStorage

  init(recordStore: ReplicaTestRecordStore, metadataStorage: ReplicaMetadataStorage) {
    self.recordStore = recordStore
    self.metadataStorage = metadataStorage
  }
}

private final class ReplicaInMemoryMetadataProvider: MetadataProvider, @unchecked Sendable {
  // Defaults to durable row ownership: inheriting the protocol default (false)
  // would put the client in the continuity-required mode used only for tagged
  // capability, making first-request shape depend on
  // load/query vs subscribe-poll scheduling over the index-consumed scripted
  // responses. Tests that pin the process-local (non-durable) tracker
  // contract pass false explicitly.
  let supportsDurableRowOwnership: Bool
  var supportsExclusiveWorkingSetReset: Bool { supportsDurableRowOwnership }
  private let trackerRebuildAdmissionError: Bool
  private let trackerRebuildOwnershipTags: [String: [String]]?
  private let admitsFreshOnDemandPristineOwner: Bool
  private let pristineOwnerAdmissionError: Bool
  private var pristineOwnerAdmissionResults: [ReplicaPristineOwnerAdmissionResult]
  private var afterNextPristineOwnerAdmission: (@Sendable () -> Void)?

  init(
    supportsDurableRowOwnership: Bool = true,
    trackerRebuildAdmissionError: Bool = false,
    trackerRebuildOwnership: [String: [String]]? = nil,
    admitsFreshOnDemandPristineOwner: Bool = false,
    pristineOwnerAdmissionError: Bool = false,
    pristineOwnerAdmissionResults: [ReplicaPristineOwnerAdmissionResult] = []
  ) {
    self.supportsDurableRowOwnership = supportsDurableRowOwnership
    self.trackerRebuildAdmissionError = trackerRebuildAdmissionError
    self.trackerRebuildOwnershipTags = trackerRebuildOwnership
    self.admitsFreshOnDemandPristineOwner = admitsFreshOnDemandPristineOwner
    self.pristineOwnerAdmissionError = pristineOwnerAdmissionError
    self.pristineOwnerAdmissionResults = pristineOwnerAdmissionResults
  }

  private let lock = NSLock()
  private let storage = ReplicaMetadataStorage()

  private func storage(for transaction: Any?) -> ReplicaMetadataStorage {
    (transaction as? ReplicaTransactionContext)?.metadataStorage ?? storage
  }

  func transactionStorageCopy() -> ReplicaMetadataStorage { storage.copy() }
  func replaceTransactionStorage(_ transactionStorage: ReplicaMetadataStorage) {
    storage.replaceContents(with: transactionStorage)
  }

  func afterNextPristineOwnerAdmission(_ action: @escaping @Sendable () -> Void) {
    lock.withLock {
      afterNextPristineOwnerAdmission = action
    }
  }

  func seedFetched(table: String, predicate: PredicateHash) {
    storage.lock.withLock {
      var tablePredicates = storage.fetched[table] ?? [:]
      tablePredicates[predicate] = FetchedPredicate(
        predicateHash: predicate,
        predicateJSON: nil,
        snapshotBoundary: nil,
        outcome: .present,
        isComplete: true,
        fetchedAt: Date()
      )
      storage.fetched[table] = tablePredicates
    }
  }

  func seedOwnedRow(table: String, rowKey: String) {
    _ = storage.lock.withLock {
      storage.ownedRowKeys[table, default: []].insert(rowKey)
    }
  }

  func seedOwnershipTags(table: String, rowKey: String, tags: [String]) {
    storage.lock.withLock {
      storage.ownershipTags[table, default: [:]][rowKey] = tags
    }
  }

  func ownershipTags(table: String) -> [String: [String]] {
    storage.lock.withLock { storage.ownershipTags[table] ?? [:] }
  }

  func hasFetched(table: String, predicate: PredicateHash, transaction: Any?) throws -> Bool {
    let storage = storage(for: transaction)
    return storage.lock.withLock { storage.fetched[table]?[predicate]?.isComplete == true }
  }

  func getFetchedPredicates(table: String, transaction: Any?) throws -> [FetchedPredicate] {
    let storage = storage(for: transaction)
    return storage.lock.withLock { storage.fetched[table].map { Array($0.values) } ?? [] }
  }

  func recordFetch(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome,
    isComplete: Bool,
    transaction: Any?
  ) throws {
    let storage = storage(for: transaction)
    storage.lock.withLock {
      var tablePredicates = storage.fetched[table] ?? [:]
      tablePredicates[predicate] = FetchedPredicate(
        predicateHash: predicate,
        predicateJSON: predicateJSON,
        snapshotBoundary: snapshotBoundary,
        outcome: outcome,
        isComplete: isComplete,
        fetchedAt: Date()
      )
      storage.fetched[table] = tablePredicates
    }
  }

  func getFetchedRanges(table: String, orderField: String, transaction: Any?) throws
    -> [FetchedRange]
  {
    let storage = storage(for: transaction)
    return storage.lock.withLock { storage.ranges[table]?[orderField] ?? [] }
  }

  func recordRange(
    table: String,
    orderField: String,
    range: FetchedRange,
    transaction: Any?
  ) throws {
    let storage = storage(for: transaction)
    storage.lock.withLock {
      var tableRanges = storage.ranges[table] ?? [:]
      tableRanges[orderField, default: []].append(range)
      storage.ranges[table] = tableRanges
    }
  }

  func clearMetadata(table: String, transaction: Any?) throws {
    let storage = storage(for: transaction)
    storage.lock.withLock {
      storage.fetched[table] = nil
      storage.ranges[table] = nil
    }
  }

  func getSyncState(collectionId: String, transaction: Any?) throws -> SyncState? {
    let storage = storage(for: transaction)
    return storage.lock.withLock { storage.syncStates[collectionId] }
  }

  func updateSyncState(collectionId: String, state: SyncState, transaction: Any?) throws {
    let storage = storage(for: transaction)
    storage.lock.withLock { storage.syncStates[collectionId] = state }
  }

  func releaseAllRowOwnership(
    table: String,
    shapeIdentity _: String,
    transaction: Any?
  ) throws -> [String] {
    let storage = storage(for: transaction)
    return storage.lock.withLock {
      defer { storage.ownedRowKeys[table] = nil }
      return Array(storage.ownedRowKeys[table] ?? []).sorted()
    }
  }

  func clearExclusiveWorkingSetOwnership(table: String, transaction: Any?) throws {
    let storage = storage(for: transaction)
    storage.lock.withLock {
      storage.ownedRowKeys[table] = nil
      storage.ownershipTags[table] = nil
    }
  }

  func getRowOwnershipTags(
    table: String,
    shapeIdentity _: String,
    rowKeys: Set<String>?,
    transaction: Any?
  ) throws -> [String: [String]] {
    let storage = storage(for: transaction)
    return storage.lock.withLock {
      let tags = storage.ownershipTags[table] ?? [:]
      guard let rowKeys else { return tags }
      return tags.filter { rowKeys.contains($0.key) }
    }
  }

  func updateRowOwnership(
    table: String,
    shapeIdentity _: String,
    tagsByRowKey: [String: [String]],
    removedRowKeys: Set<String>,
    transaction: Any?
  ) throws {
    let storage = storage(for: transaction)
    storage.lock.withLock {
      var tags = storage.ownershipTags[table] ?? [:]
      for rowKey in removedRowKeys { tags[rowKey] = nil }
      for (rowKey, values) in tagsByRowKey { tags[rowKey] = values }
      storage.ownershipTags[table] = tags
    }
  }

  func trackerRebuildOwnership(
    table _: String,
    shapeIdentity _: String,
    transaction _: Any?
  ) throws -> [String: [String]]? {
    if trackerRebuildAdmissionError {
      throw ElectricSyncError.fetchFailed("simulated ownership admission SQL error")
    }
    return trackerRebuildOwnershipTags
  }

  func admitsFreshOnDemandPristineOwner(
    table _: String,
    localTableOwnership _: ElectricLocalTableOwnership,
    transaction _: Any?
  ) throws -> Bool {
    if let scriptedResult = lock.withLock({
      pristineOwnerAdmissionResults.isEmpty
        ? nil
        : pristineOwnerAdmissionResults.removeFirst()
    }) {
      switch scriptedResult {
      case .admit:
        let action = lock.withLock { () -> (@Sendable () -> Void)? in
          defer { afterNextPristineOwnerAdmission = nil }
          return afterNextPristineOwnerAdmission
        }
        action?()
        return true
      case .refuse:
        return false
      case .error:
        throw ElectricSyncError.fetchFailed("simulated pristine-owner admission SQL error")
      }
    }
    if pristineOwnerAdmissionError {
      throw ElectricSyncError.fetchFailed("simulated pristine-owner admission SQL error")
    }
    return admitsFreshOnDemandPristineOwner
  }
}

extension ElectricMessage {
  fileprivate static func replicaRecord<Model: Codable>(
    _ record: Model,
    offset: String,
    tags: [String]? = nil,
    key: String? = nil,
    txids: [Int64]? = nil,
    isSubsetSnapshot: Bool = false
  ) -> ElectricMessage {
    ElectricMessage(
      payload: try! JSONEncoder().encode(record),
      key: key,
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: false,
      kind: .snapshot,
      txids: txids,
      tags: tags,
      isSubsetSnapshot: isSubsetSnapshot
    )
  }

  fileprivate static func replicaUpToDate(offset: String) -> ElectricMessage {
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

  fileprivate static func replicaSubsetEnd(offset: String) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: false,
      kind: .snapshot,
      control: .subsetEnd,
      isSubsetSnapshot: true
    )
  }

  fileprivate static func replicaSubsetSnapshotEnd(
    offset: String,
    snapshot: PostgresSnapshot
  ) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: false,
      kind: .snapshot,
      control: .snapshotEnd,
      postgresSnapshot: snapshot,
      isSubsetSnapshot: true
    )
  }

  fileprivate static func replicaSnapshotEnd(offset: String) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: false,
      kind: .snapshot,
      control: .snapshotEnd
    )
  }

  fileprivate static func replicaTruncate() -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: "-1",
      handle: nil,
      cursor: nil,
      isUpToDate: false,
      kind: .truncate
    )
  }
}

private func waitUntilTrue(
  timeout: TimeInterval = 30,
  condition: @escaping @Sendable () -> Bool
) async throws {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    if condition() { return }
    try await Task.sleep(nanoseconds: 10_000_000)
  }
  Issue.record("Timed out waiting for condition")
}

private func waitUntilTrueAsync(
  timeout: TimeInterval = 30,
  condition: @escaping @Sendable () async -> Bool
) async throws {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    if await condition() { return }
    try await Task.sleep(nanoseconds: 10_000_000)
  }
  Issue.record("Timed out waiting for async condition")
}
