import Foundation
import GRDB
import Testing

@testable import ElectricSync

private typealias SQLExpression = ElectricSync.SQLExpression

struct ElectricSyncClientTests {
  @Test(.timeLimit(.minutes(1)))
  func subscriptionCancellationRelayHandlesTerminalAndPreInstallCancellationRaces() async {
    let finishedRelay = ElectricSubscriptionCancellationRelay()
    finishedRelay.finish()
    let completedProducer = Task<Void, Error> {}
    _ = await completedProducer.result
    finishedRelay.install(producerTask: completedProducer)
    #expect(!finishedRelay.hasInstalledProducerTask)

    let cancellationRelay = ElectricSubscriptionCancellationRelay()
    cancellationRelay.cancel()
    let cancellationCounter = ThreadSafeCounter()
    let cancelledBeforeInstallProducer = Task<Void, Error> {
      try await withTaskCancellationHandler {
        try await Task.sleep(for: .seconds(60))
      } onCancel: {
        cancellationCounter.increment()
      }
    }
    cancellationRelay.install(producerTask: cancelledBeforeInstallProducer)
    _ = await cancelledBeforeInstallProducer.result

    #expect(cancellationCounter.value == 1)
    cancellationRelay.finish()
    #expect(!cancellationRelay.hasInstalledProducerTask)
  }

  @Test
  func forcedFullBootstrapWaitsForUpToDateAfterSnapshotEnd() async throws {
    let stalePrefix = TestRecord(id: "user", name: "Not onboarded")
    let authoritativeRecord = TestRecord(id: "user", name: "Onboarded")
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          .make(record: stalePrefix, offset: "snapshot-offset", key: stalePrefix.id),
          .snapshotEnd(offset: "snapshot-offset"),
        ],
        [
          .make(
            record: authoritativeRecord,
            offset: "up-to-date-offset",
            key: authoritativeRecord.id
          ),
          .upToDate(offset: "up-to-date-offset"),
        ],
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: InMemoryMetadataProvider(),
        httpClient: http
      )
    )

    let batch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .progressive,
        live: false,
        forceFullBootstrap: true
      )
    )

    #expect(await http.requestCount() == 2)
    #expect(batch.messages.contains { $0.control == .upToDate })
    #expect(batch.messages.count == 4)
    let requests = await http.capturedRequests()
    try #require(requests.count == 2)
    #expect(requests[0].offset == "-1")
    #expect(requests[1].offset == "snapshot-offset")
  }

  @Test
  func optimisticPublicationEvidenceStaysRowAddressedAcrossSplitChunks() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let sharedFrontier = Data([7, 7])
    let messages = try [
      ElectricMessage(
        payload: JSONEncoder().encode(TestRecord(id: "a", name: "Loro")),
        key: "a",
        offset: "1",
        handle: "handle",
        txids: [42]
      ),
      ElectricMessage(
        payload: JSONEncoder().encode(TestRecord(id: "b", name: "Loro")),
        key: "b",
        offset: "2",
        handle: "handle",
        txids: [42]
      ),
    ]
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: InMemoryHTTPClientProvider(
          responses: messages.enumerated().map {
            [$0.element, .upToDate(offset: String($0.offset + 1))]
          }
        )
      )
    )

    for _ in messages {
      let batch = try #require(
        try await client.pollStream(
          TestRecord.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try batch.apply(in: store)
    }

    let retirements = metadata.optimisticRetirementEvidence()
    #expect(retirements.count == 2)
    #expect(retirements[0].publications.map(\.rowEffect.rowId) == ["a"])
    #expect(retirements[1].publications.map(\.rowEffect.rowId) == ["b"])
    for retirement in retirements {
      #expect(retirement.publications.first?.transactionIds == [42])
      #expect(retirement.publications.first?.loroFrontiers == [sharedFrontier])
    }
  }

  @Test
  func splitDeleteRecreatePublishesEffectsInCausalOrderEvenWithSharedTransaction() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    store.upsert(TestRecord(id: "row", name: "Before"))
    let deleted = try ElectricMessage(
      payload: JSONEncoder().encode(TestRecord(id: "row", name: "__delete__")),
      key: "row",
      offset: "1",
      handle: "handle",
      txids: [42]
    )
    let recreated = try ElectricMessage(
      payload: JSONEncoder().encode(TestRecord(id: "row", name: "After")),
      key: "row",
      offset: "2",
      handle: "handle",
      txids: [42]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: InMemoryHTTPClientProvider(
          responses: [
            [deleted, .upToDate(offset: "1")],
            [recreated, .upToDate(offset: "2")],
          ]
        )
      )
    )

    for _ in 0..<2 {
      let batch = try #require(
        try await client.pollStream(
          TestRecord.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try batch.apply(in: store)
    }

    let effects = metadata.optimisticRetirementEvidence().compactMap {
      $0.publications.first?.rowEffect
    }
    #expect(effects.map(\.rowId) == ["row", "row"])
    #expect(effects.map(\.operation) == [.delete, .insert])
  }

  @Test
  func truncateAndUnaddressedSnapshotDoNotPublishRetirementEvidence() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let missingSnapshot = ElectricMessage(
      payload: Data(#"{"id":"missing"}"#.utf8),
      key: "missing",
      offset: "2",
      handle: "replacement",
      kind: .snapshot,
      txids: [42]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: InMemoryHTTPClientProvider(
          responses: [
            [.truncate(handle: "replacement")],
            [missingSnapshot, .snapshotEnd(offset: "2"), .upToDate(offset: "2")],
          ]
        )
      )
    )

    let truncateBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    let truncateOutput = try truncateBatch.apply(in: store)
    #expect(truncateOutput.encounteredTruncate)
    #expect(metadata.optimisticRetirementEvidence().isEmpty)

    let snapshotBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try snapshotBatch.apply(in: store)
    #expect(metadata.optimisticRetirementEvidence().last?.publications.isEmpty == true)
  }

  @Test
  func committedTrackerLossPurgeRemainsNonResumableAfterClientReopen() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    store.upsert(TestRecord(id: "stale", name: "Stale"))
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestRecord.self,
      basePredicate: nil
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "5",
        handle: "handle-5",
        cursor: "cursor-5",
        isUpToDate: true,
        lastSyncedAt: nil,
        protocolSemanticEpoch: .taggedShape1_7_7
      ),
      transaction: nil
    )
    metadata.seedRowOwnership(
      table: TestRecord.tableName,
      rowKey: "stale",
      shapeIdentity: streamStateKey
    )

    let replacement = TestRecord(id: "replacement", name: "Replacement")
    let taggedSnapshot = [
      ElectricMessage.make(
        record: replacement,
        offset: "6",
        key: replacement.id,
        tags: ["scope/replacement"]
      ),
      ElectricMessage.upToDate(offset: "6"),
    ]
    let http = InMemoryHTTPClientProvider(responses: [taggedSnapshot, taggedSnapshot])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { true }
        )
      )
    )

    let resetBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    let resetOutput = try resetBatch.apply(in: store)
    #expect(resetOutput.requiresReplacementSwap)
    resetOutput.transactionDidCommit()

    let replacementBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    #expect(try #require(await http.capturedRequests().last).offset == "-1")

    // Simulate a crash after the replacement transaction purges stale rows but
    // before the fetched replacement batch is applied.
    try replacementBatch.prepareTruncateSwap(in: store)
    #expect(store.allRecords().isEmpty)
    let committedState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(committedState.canResumeWithoutFullBootstrap == false)
    #expect(committedState.offset == "-1")

    let reopenedHTTP = InMemoryHTTPClientProvider(responses: [taggedSnapshot])
    let reopenedClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: reopenedHTTP,
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { true }
        )
      )
    )
    _ = try await reopenedClient.pollStream(
      TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand,
      live: false
    )
    #expect(try #require(await reopenedHTTP.capturedRequests().first).offset == "-1")
  }

  @Test
  func optimisticRetirementRequiresPublishedTypedBaseEvidence() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    store.upsert(TestRecord(id: "row-1", name: "Same"))
    let unchangedPresence = try ElectricMessage(
      payload: JSONEncoder().encode(TestRecord(id: "row-1", name: "Same")),
      key: "row-1",
      offset: "1",
      handle: "handle",
      kind: .mutation,
      tags: ["pending"]
    )
    let moveOut = ElectricMessage(
      payload: Data(),
      offset: "2",
      handle: "handle",
      kind: .mutation,
      txids: [77],
      event: .moveOut(patterns: [MovePattern(pos: 0, value: "pending")])
    )
    let exactPublish = try ElectricMessage(
      payload: JSONEncoder().encode(TestRecord(id: "row-2", name: "Published")),
      key: "row-2",
      offset: "3",
      handle: "handle",
      kind: .mutation,
      txids: [42]
    )
    let http = InMemoryHTTPClientProvider(
      responses: [
        [unchangedPresence, .upToDate(offset: "1")],
        [moveOut, .upToDate(offset: "2")],
        [exactPublish, .upToDate(offset: "3")],
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(metadataProvider: metadata, httpClient: http)
    )

    for _ in 0..<3 {
      let batch = try #require(
        try await client.pollStream(
          TestRecord.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: false
        )
      )
      _ = try batch.apply(in: store)
    }

    let retirements = metadata.optimisticRetirementEvidence()
    #expect(retirements.count == 3)
    #expect(retirements[0].publications.count == 1)
    #expect(retirements[0].publications.first?.rowEffect.rowId == "row-1")
    #expect(retirements[0].publications.first?.transactionIds.isEmpty == true)
    #expect(retirements[1].publications.isEmpty)
    #expect(retirements[2].publications.count == 1)
    #expect(retirements[2].publications.first?.rowEffect.rowId == "row-2")
    #expect(retirements[2].publications.first?.transactionIds == [42])
  }

  @Test
  func productionKeepSyncedRegistrationEmitsAfterCommitAndCoalescesRepeatedWrites() async throws {
    let diagnostics = ElectricCursorOwnershipDiagnostics()
    let telemetry = RecordingElectricCursorTelemetry()
    let basePredicate = SQLExpression("account_id = 'owner'")
    let predicate = SQLExpression("id = 'non-owner'")
    let orderBy = [OrderBy(field: "name", direction: .descending)]
    let configuration = ElectricCollectionConfiguration(
      modelType: TestRecord.self,
      syncMode: .eager,
      basePredicate: basePredicate,
      predicate: predicate,
      orderBy: orderBy,
      limit: 1,
      shapeTopology: .staticallySimple
    )
    let ownerHTTP = InMemoryHTTPClientProvider(responses: [])
    let ownerClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: InMemoryMetadataProvider(),
        httpClient: ownerHTTP,
        tracer: telemetry
      ),
      cursorOwnershipDiagnostics: diagnostics
    )
    let store = TestRecordStore()
    let ownerCollection = ElectricCollection(
      configuration: configuration,
      client: ownerClient,
      cacheProvider: StoreBackedCacheProvider(store: store),
      transactionRunner: { operation in try operation(store) },
      logger: telemetry,
      tracer: telemetry
    )

    let sessionController = TestSessionController()
    do {
      let session = try #require(
        sessionController.captureAuthenticatedSession()
      )
      let ownerToken = ownerCollection.keepSynced(session: session)
      defer { ownerToken.cancel() }
      try await waitUntil(timeout: 30) {
        ownerCollection.replica.liveOwnerCount == 1
      }
      try await waitUntilAsync(timeout: 30) {
        await ownerHTTP.requestCount() > 0
      }
      let ownerRequests = await ownerHTTP.capturedRequests()
      let ownerRequest = try #require(ownerRequests.first)
      #expect(ownerRequest.offset == nil)
      #expect(ownerRequest.log == nil)

      let metadata = InMemoryMetadataProvider()
      let http = InMemoryHTTPClientProvider(
        responses: [
          [
            ElectricMessage.upToDate(offset: "offset-non-owner-1"),
            ElectricMessage.subsetEnd(offset: "offset-non-owner-1"),
          ],
          [ElectricMessage.upToDate(offset: "offset-non-owner-2")],
          [ElectricMessage.upToDate(offset: "offset-non-owner-3")],
        ]
      )
      let writerClient = ElectricSyncClientImpl(
        configuration: .init(
          metadataProvider: metadata,
          httpClient: http,
          tracer: telemetry
        ),
        cursorOwnershipDiagnostics: diagnostics
      )
      let writerCollection = ElectricCollection(
        configuration: configuration,
        client: writerClient,
        cacheProvider: StoreBackedCacheProvider(store: store),
        transactionRunner: { operation in
          try operation(store)
          let collisionLogs = telemetry.recordedLogs().filter {
            $0.metadata[ElectricCursorOwnershipDiagnostics.collisionTypeAttribute]
              == "non_owning_cursor_writer"
          }
          #expect(collisionLogs.isEmpty)
        },
        tracer: telemetry
      )
      _ = try await writerCollection.query(
        where: predicate,
        orderBy: orderBy,
        limit: 1,
        session: session
      )

      let streamStateKey = legacyStreamStateKey(
        for: TestRecord.self,
        basePredicate: basePredicate,
        syncMode: .eager
      )
      #expect(ownerCollection.replica.identity.persistedCursorKey != streamStateKey)

      let subsetState = try #require(
        try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
      )
      #expect(subsetState.offset == "offset-non-owner-1")
      #expect(subsetState.handle == "handle-offset-non-owner-1")
      #expect(subsetState.cursor == nil)

      let firstStreamBatch = try #require(
        try await writerClient.pollStream(
          TestRecord.self,
          basePredicate: basePredicate,
          syncMode: .eager,
          live: false
        )
      )
      let firstStreamOutput = try firstStreamBatch.apply(in: store)
      firstStreamOutput.emitCursorOwnershipCollisionReports()

      var collisionLogs = telemetry.recordedLogs().filter {
        $0.metadata[ElectricCursorOwnershipDiagnostics.collisionTypeAttribute]
          == "non_owning_cursor_writer"
      }
      #expect(collisionLogs.count == 1)
      let collisionLog = try #require(collisionLogs.first)
      #expect(collisionLog.level == LogLevel.warning.rawValue)
      #expect(collisionLog.metadata[ElectricCursorOwnershipDiagnostics.ownerCountAttribute] == "1")
      #expect(
        collisionLog.metadata[ElectricCursorOwnershipDiagnostics.writerIsOwnerAttribute] == "false"
      )
      #expect(
        collisionLog.metadata[ElectricCursorOwnershipDiagnostics.persistedCursorKeyAttribute]
          == streamStateKey
      )
      let collisionSpans = telemetry.recordedSpans().filter {
        $0.name == "electric.cursor_writer.collision"
      }
      #expect(collisionSpans.count == 1)
      let collisionSpan = try #require(collisionSpans.first)
      #expect(
        collisionSpan.attributes[ElectricCursorOwnershipDiagnostics.ownerCountAttribute] == "1")
      #expect(collisionSpan.status == .failure)
      #expect(try metadata.getSyncState(collectionId: streamStateKey, transaction: nil) != nil)

      let repeatedStreamBatch = try #require(
        try await writerClient.pollStream(
          TestRecord.self,
          basePredicate: basePredicate,
          syncMode: .eager,
          live: false
        )
      )
      let repeatedStreamOutput = try repeatedStreamBatch.apply(in: store)
      repeatedStreamOutput.emitCursorOwnershipCollisionReports()
      collisionLogs = telemetry.recordedLogs().filter {
        $0.metadata[ElectricCursorOwnershipDiagnostics.collisionTypeAttribute]
          == "non_owning_cursor_writer"
      }
      #expect(collisionLogs.count == 1)
    }
  }

  @Test
  func logsWhenCursorWriterWritesLiveStreamStateWithoutOwning() async throws {
    let diagnostics = ElectricCursorOwnershipDiagnostics()
    let telemetry = RecordingElectricCursorTelemetry()
    let manager = ElectricCollectionStreamManager(
      gcTime: 0,
      cursorOwnershipDiagnostics: diagnostics
    )
    let ownerClient = NSObject()
    // Register ownership only under the live stream-state key; subset evidence
    // is intentionally outside cursor ownership.
    let streamStateKey = ElectricSyncClientImpl.streamStateKey(
      for: TestRecord.self,
      basePredicate: nil
    )
    let ownerToken = manager.acquire(
      key: .init(
        tableName: TestRecord.tableName,
        clientId: ObjectIdentifier(ownerClient)
      ),
      persistedCursorKeys: [streamStateKey],
      collectionIdentifier: TestRecord.collectionIdentifier,
      logger: telemetry,
      tracer: telemetry
    ) {
      Task {}
    }

    let metadata = InMemoryMetadataProvider()
    let http = InMemoryHTTPClientProvider(
      responses: [[ElectricMessage.upToDate(offset: "offset-live-non-owner")]]
    )
    let writerClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        tracer: telemetry,
        isExactCursorCutoverEnabled: true
      ),
      cursorOwnershipDiagnostics: diagnostics
    )

    let batch = try #require(
      try await writerClient.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    let output = try batch.apply(in: TestRecordStore())

    #expect(telemetry.recordedLogs().isEmpty)
    output.emitCursorOwnershipCollisionReports()

    let collisionLogs = telemetry.recordedLogs().filter {
      $0.metadata[ElectricCursorOwnershipDiagnostics.collisionTypeAttribute]
        == "non_owning_cursor_writer"
    }
    #expect(collisionLogs.count == 1)
    let log = try #require(collisionLogs.first)
    #expect(
      log.metadata[ElectricCursorOwnershipDiagnostics.persistedCursorKeyAttribute]
        == streamStateKey
    )

    let collisionSpans = telemetry.recordedSpans().filter {
      $0.name == "electric.cursor_writer.collision"
    }
    #expect(collisionSpans.count == 1)
    let span = try #require(collisionSpans.first)
    #expect(span.attributes[ElectricCursorOwnershipDiagnostics.ownerCountAttribute] == "1")
    #expect(span.status == .failure)
    #expect(try metadata.getSyncState(collectionId: streamStateKey, transaction: nil) != nil)

    ownerToken.cancel()
  }

  @Test
  func queryFetchesAndPersistsMissingData() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "1", name: "Alpha"),
            offset: "offset-1",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store
    )

    let predicate = SQLExpression("id = '1'")
    let results = try await collection.query(where: predicate, orderBy: [], limit: 1)

    #expect(results == [TestRecord(id: "1", name: "Alpha")])
    #expect(await http.requestCount() == 1)

    let recorded = try #require(
      metadata.recordedPredicate(
        table: TestRecord.tableName,
        predicate: fetchMetadataHash(predicate: predicate, orderBy: [], limit: 1)
      )
    )
    #expect(
      recorded.snapshotBoundary == PostgresSnapshot(xmin: "10", xmax: "20", xipList: ["11", "12"]))
    #expect(recorded.outcome == .present)
    #expect(recorded.isComplete)
  }

  @Test
  func requestSnapshotAndStreamApplyAdvanceOwnerCursorState() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let predicate = SQLExpression("id = 'target'")
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "target", name: "Subset"),
            offset: "offset-subset",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-subset"),
          ElectricMessage.subsetEnd(offset: "offset-subset"),
        ],
        [
          ElectricMessage.make(
            record: TestRecord(id: "stream", name: "Stream"),
            offset: "offset-stream",
            cursor: "cursor-stream"
          ),
          ElectricMessage.upToDate(offset: "offset-stream", cursor: "cursor-stream"),
        ],
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(metadataProvider: metadata, httpClient: http)
    )
    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )

    let subsetBatch = try #require(
      await client.requestSnapshot(
        TestRecord.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(predicate: predicate, orderBy: [], limit: nil),
        syncMode: .onDemand
      )
    )
    _ = try subsetBatch.apply(in: store)

    let subsetState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(subsetState.offset == "offset-subset")
    #expect(subsetState.handle == "handle-offset-subset")
    #expect(subsetState.isUpToDate == false)

    let streamBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    _ = try streamBatch.apply(in: store)

    let syncState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(syncState.offset == "offset-stream")
    #expect(syncState.handle == "handle-offset-stream")
    #expect(syncState.cursor == "cursor-stream")
    #expect(syncState.isUpToDate)
  }

  @Test
  func exactModeFreshSubsetLoadsWithoutPersistedBootstrapFence() async throws {
    let metadata = InMemoryMetadataProvider()
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "target", name: "Fresh subset"),
            offset: "subset-offset",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "subset-offset"),
          ElectricMessage.subsetEnd(offset: "subset-offset"),
        ]
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { true }
        )
      )
    )

    let batch = try await client.requestSnapshot(
      TestRecord.self,
      basePredicate: nil,
      descriptor: QueryDescriptor(
        predicate: SQLExpression("id = 'target'"),
        orderBy: [],
        limit: nil
      ),
      syncMode: .onDemand
    )

    #expect(batch != nil)
    #expect(await http.requestCount() == 1)
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil) == nil
    )
  }

  @Test
  func exactModeFreshTransientSubsetLoadsWithoutPersistedBootstrapFence() async throws {
    let metadata = InMemoryMetadataProvider()
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "target", name: "Fresh transient subset"),
            offset: "subset-offset",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "subset-offset"),
          ElectricMessage.subsetEnd(offset: "subset-offset"),
        ]
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { true }
        )
      )
    )

    let batch = try await client.fetchSnapshot(
      TestRecord.self,
      basePredicate: nil,
      descriptor: QueryDescriptor(
        predicate: SQLExpression("id = 'target'"),
        orderBy: [],
        limit: nil
      ),
      syncMode: .onDemand
    )

    #expect(batch != nil)
    #expect(await http.requestCount() == 1)
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil) == nil
    )
  }

  @Test
  func querySendsStructuredPredicateAsSubsetSnapshot() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(record: TestRecord(id: "1", name: "Override"), offset: "offset-1"),
          ElectricMessage.snapshotEnd(offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store
    )

    let parentId = UUID(uuidString: "8580d63d-3272-4644-b6cb-53ce8918d9f0")!
    let predicate = SQLExpression(
      predicate: .membership(
        field: "recurringParentId",
        values: [.string(parentId.uuidString)]
      )
    )

    let results = try await collection.query(where: predicate)

    let request = try #require(await http.capturedRequests().first)
    let subset = try #require(request.subset)
    #expect(results == [TestRecord(id: "1", name: "Override")])
    #expect(request.predicate == nil)
    #expect(subset.whereClause == "recurring_parent_id IN ($1)")
    #expect(subset.paramsJSON == #"{"1":"\#(parentId.uuidString)"}"#)
  }

  @Test
  func queryUsesCacheWhenPredicateAlreadyFetched() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let http = InMemoryHTTPClientProvider(responses: [])
    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store
    )

    let predicate = SQLExpression("name LIKE 'A%'")
    metadata.seedFetchedPredicate(
      table: TestRecord.tableName,
      predicate: PredicateHash(from: predicate)
    )
    store.upsert(TestRecord(id: "cached", name: "Cached"))

    let results = try await collection.query(where: predicate, orderBy: [], limit: 1)

    #expect(results.first?.name == "Cached")
    #expect(await http.requestCount() == 0)
  }

  @Test
  func ensureSubsetFetchesAndAppliesDespiteCompletedCoverage() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "target", name: "Fresh"),
            offset: "offset-transient",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-transient"),
          ElectricMessage.subsetEnd(offset: "offset-transient"),
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
    let collection = makeTestCollection(client: client, store: store)
    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("target"))
    )
    let metadataKey = ElectricFetchTracker.metadataKey(
      predicate: predicate,
      orderBy: [],
      limit: 1
    ).predicateHash
    metadata.seedFetchedPredicate(table: TestRecord.tableName, predicate: metadataKey)

    let result = try await collection.ensureSubset(where: predicate, limit: 1)

    #expect(result.appliedRecords == [TestRecord(id: "target", name: "Fresh")])
    #expect(result.localRecords == [TestRecord(id: "target", name: "Fresh")])
    #expect(await http.requestCount() == 1)
    #expect(
      try metadata.getFetchedPredicates(table: TestRecord.tableName, transaction: nil).count == 1)

    let observation = try #require(
      metadata.recordedObservation(table: TestRecord.tableName, predicate: metadataKey)
    )
    #expect(observation.outcome == .present)
    #expect(
      observation.snapshotBoundary
        == PostgresSnapshot(xmin: "10", xmax: "20", xipList: ["11", "12"])
    )
    #expect(result.observation == observation)

    let request = try #require(await http.capturedRequests().first)
    #expect(request.log == .changesOnly)
    #expect(request.subset?.whereClause == "id = $1")
    #expect(request.subset?.paramsJSON == #"{"1":"target"}"#)
  }

  @Test
  func ensureSubsetReportsEmptyResultWithoutWritingLocally() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let transactionCounter = TransactionCounter()
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.snapshotEnd(offset: "offset-empty"),
          ElectricMessage.subsetEnd(offset: "offset-empty"),
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
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter
    )
    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("absent"))
    )

    let result = try await collection.ensureSubset(where: predicate, limit: 1)

    #expect(result.appliedRecords.isEmpty)
    #expect(result.localRecords.isEmpty)
    #expect(await http.requestCount() == 1)
    #expect(store.allRecords().isEmpty)
    #expect(await transactionCounter.count() == 1)
    #expect(
      try metadata.getFetchedPredicates(table: TestRecord.tableName, transaction: nil).isEmpty)

    let observation = try #require(
      metadata.recordedObservation(
        table: TestRecord.tableName,
        predicate: fetchMetadataHash(predicate: predicate, orderBy: [], limit: 1)
      )
    )
    #expect(observation.outcome == .absent)
    #expect(
      observation.snapshotBoundary
        == PostgresSnapshot(xmin: "10", xmax: "20", xipList: ["11", "12"])
    )
    #expect(result.observation == observation)
  }

  @Test
  func replicaSeparatesAuthoritativeEmptySnapshotFromStaleLocalRow() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let staleLocalRecord = TestRecord(id: "absent", name: "Stale")
    store.upsert(staleLocalRecord)
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.snapshotEnd(offset: "offset-empty"),
          ElectricMessage.subsetEnd(offset: "offset-empty"),
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
    let collection = makeTestCollection(client: client, store: store)
    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("absent"))
    )

    let result = try await collection.ensureSubset(where: predicate, limit: 1)

    #expect(result.appliedRecords.isEmpty)
    #expect(result.localRecords == [staleLocalRecord])
    #expect(store.allRecords() == [staleLocalRecord])

    let observation = try #require(
      metadata.recordedObservation(
        table: TestRecord.tableName,
        predicate: fetchMetadataHash(predicate: predicate, orderBy: [], limit: 1)
      )
    )
    #expect(observation.outcome == .absent)
    #expect(result.observation == observation)
  }

  @Test
  func ensureSubsetTreatsCatchUpTargetAsAbsentWhenSubsetIsEmpty() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let target = TestRecord(id: "target", name: "Transient")
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: target,
            offset: "offset-catch-up",
            key: target.id,
            tags: ["pending"]
          )
        ],
        [
          ElectricMessage(
            payload: Data(),
            offset: "offset-move-out",
            handle: "handle-offset-move-out",
            kind: .mutation,
            event: .moveOut(patterns: [MoveOutPattern(pos: 0, value: "pending")])
          ),
          ElectricMessage.upToDate(offset: "offset-move-out"),
          ElectricMessage.snapshotEnd(offset: "offset-empty"),
          ElectricMessage.subsetEnd(offset: "offset-empty"),
        ],
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let collection = makeTestCollection(client: client, store: store)
    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string(target.id))
    )

    let result = try await collection.ensureSubset(where: predicate, limit: 1)

    #expect(result.appliedRecords.isEmpty)
    #expect(result.localRecords.isEmpty)
    #expect(store.allRecords().isEmpty)
    let requestCount = await http.requestCount()
    #expect(requestCount == 2)
  }

  @Test
  func ensureSubsetAppliesUnrelatedCatchUpThroughOwnerWhenSubsetIsEmpty() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let transactionCounter = TransactionCounter()
    let unrelated = TestRecord(id: "unrelated", name: "Buffered")
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: unrelated,
            offset: "offset-catch-up"
          ),
          ElectricMessage.upToDate(offset: "offset-catch-up"),
          ElectricMessage.snapshotEnd(offset: "offset-empty"),
          ElectricMessage.subsetEnd(offset: "offset-empty"),
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
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter
    )
    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "offset-stale",
        handle: "handle-stale",
        cursor: "cursor-stale",
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 1_234)
      ),
      transaction: nil
    )
    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("absent"))
    )

    let result = try await collection.ensureSubset(where: predicate, limit: 1)

    #expect(result.appliedRecords.isEmpty)
    #expect(result.localRecords == [unrelated])
    #expect(store.allRecords() == [unrelated])
    #expect(store.writeCallCount() == 1)
    #expect(await transactionCounter.count() == 1)
    #expect(await http.requestCount() == 1)
  }

  @Test
  func repeatedOwnerSnapshotAppliesCatchUpAndAdvancesResumeIdentity() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let transactionCounter = TransactionCounter()
    let unrelated = TestRecord(id: "unrelated", name: "Buffered")
    let catchUpPage = [
      ElectricMessage.make(record: unrelated, offset: "offset-catch-up"),
      ElectricMessage.upToDate(offset: "offset-catch-up"),
    ]
    let emptySubsetPage = [
      ElectricMessage.snapshotEnd(offset: "offset-empty"),
      ElectricMessage.subsetEnd(offset: "offset-empty"),
    ]
    let http = InMemoryHTTPClientProvider(
      responses: [
        catchUpPage + emptySubsetPage,
        catchUpPage + emptySubsetPage,
        [
          ElectricMessage.make(
            record: unrelated,
            offset: "offset-catch-up",
            cursor: "cursor-owner"
          ),
          ElectricMessage.upToDate(offset: "offset-owner", cursor: "cursor-owner"),
        ],
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter
    )
    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "offset-stale",
        handle: "handle-stale",
        cursor: "cursor-stale",
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 1_234)
      ),
      transaction: nil
    )
    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("absent"))
    )

    let firstResult = try await collection.ensureSubset(where: predicate, limit: 1)
    let secondResult = try await collection.ensureSubset(where: predicate, limit: 1)

    #expect(firstResult.appliedRecords.isEmpty)
    #expect(secondResult.appliedRecords.isEmpty)
    #expect(store.allRecords() == [unrelated])
    #expect(store.writeCallCount() == 2)
    #expect(await transactionCounter.count() == 2)

    let requestsBeforeOwner = await http.capturedRequests()
    #expect(requestsBeforeOwner.count == 2)
    #expect(requestsBeforeOwner[0].offset == "offset-stale")
    #expect(requestsBeforeOwner[1].offset == "offset-empty")

    let ownerBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )
    let ownerOutput = try ownerBatch.apply(in: store)

    #expect(ownerOutput.records == [unrelated])
    #expect(store.allRecords() == [unrelated])
    #expect(store.writeCallCount() == 3)
    #expect(await http.requestCount() == 3)

    let ownerRequest = try #require(await http.capturedRequests().last)
    #expect(ownerRequest.offset == "offset-empty")
    #expect(ownerRequest.handle == "handle-offset-empty")
    #expect(ownerRequest.cursor == "cursor-stale")

    let liveState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(liveState.offset == "offset-owner")
    #expect(liveState.handle == "handle-offset-owner")
    #expect(liveState.cursor == "cursor-owner")
  }

  @Test
  func ensureSubsetPreservesOwningStreamIdentity() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let lastSyncedAt = Date(timeIntervalSince1970: 2_345)
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "target", name: "InStream"),
            offset: "offset-transient",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-transient"),
          ElectricMessage.subsetEnd(offset: "offset-transient"),
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
    let collection = makeTestCollection(client: client, store: store)
    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "offset-live",
        handle: "handle-live",
        cursor: "cursor-live",
        isUpToDate: true,
        lastSyncedAt: lastSyncedAt
      ),
      transaction: nil
    )
    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("target"))
    )

    let result = try await collection.ensureSubset(where: predicate, limit: 1)

    #expect(result.appliedRecords == [TestRecord(id: "target", name: "InStream")])
    #expect(result.localRecords == [TestRecord(id: "target", name: "InStream")])

    // The request rides the live stream's identity so Electric injects the
    // subset rows into the same shape stream under the same cursor.
    let request = try #require(await http.capturedRequests().first)
    #expect(request.offset == "offset-live")
    #expect(request.handle == "handle-live")
    #expect(request.cursor == "cursor-live")
    #expect(request.log == .changesOnly)

    // The terminal subset boundary advances the same owner's resume identity.
    let liveState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(liveState.offset == "offset-transient")
    #expect(liveState.handle == "handle-offset-transient")
    #expect(liveState.cursor == "cursor-live")
    #expect(liveState.isUpToDate)
    #expect(liveState.lastSyncedAt != lastSyncedAt)
    #expect(
      try metadata.getFetchedPredicates(table: TestRecord.tableName, transaction: nil).isEmpty)
  }

  @Test
  func subscribeHydratesMissingRowWithoutRecordingHydrationCoverage() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let partialUpdatePayload = try JSONSerialization.data(withJSONObject: [
      "id": "missing"
    ])

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: partialUpdatePayload,
            key: #""public"."test_records"/"missing""#,
            offset: "offset-1",
            handle: "handle-offset-1",
            cursor: nil,
            isUpToDate: false,
            kind: .snapshot
          ),
          ElectricMessage.upToDate(offset: "offset-1"),
        ],
        [
          ElectricMessage.make(
            record: TestRecord(id: "missing", name: "Hydrated"),
            offset: "offset-1",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ],
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(client: client, store: store)
    let stream = collection.subscribe()
    let consumer = Task {
      for await _ in stream {}
    }

    defer {
      consumer.cancel()
    }

    try await waitUntil(timeout: 30) {
      store.record(id: "missing")?.name == "Hydrated"
    }

    let requests = await http.capturedRequests()
    try #require(requests.count >= 2)
    #expect(requests[1].subset?.whereClause == "id IN ($1)")
    #expect(requests[1].subset?.paramsJSON == #"{"1":"missing"}"#)

    let hydrationPredicate = SQLExpression(
      predicate: .membership(field: "id", values: [.string("missing")])
    )
    let hydrationMetadata = metadata.recordedPredicate(
      table: TestRecord.tableName,
      predicate: fetchMetadataHash(predicate: hydrationPredicate, orderBy: [], limit: nil)
    )
    #expect(hydrationMetadata == nil)
  }

  @Test
  func subscribeRetriesNestedMissingRowHydrationWithoutRecordingHydrationCoverage() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let partialUpdatePayload = try JSONSerialization.data(withJSONObject: [
      "id": "missing"
    ])

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage(
            payload: partialUpdatePayload,
            key: #""public"."test_records"/"missing""#,
            offset: "offset-1",
            handle: "handle-offset-1",
            cursor: nil,
            isUpToDate: false,
            kind: .snapshot
          ),
          ElectricMessage.upToDate(offset: "offset-1"),
        ],
        [
          ElectricMessage(
            payload: partialUpdatePayload,
            key: #""public"."test_records"/"missing""#,
            offset: "offset-1",
            handle: "handle-offset-1",
            cursor: nil,
            isUpToDate: false,
            kind: .snapshot,
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ],
        [
          ElectricMessage.make(
            record: TestRecord(id: "missing", name: "Hydrated"),
            offset: "offset-1",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ],
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(client: client, store: store)
    let stream = collection.subscribe()
    let consumer = Task {
      for await _ in stream {}
    }

    defer {
      consumer.cancel()
    }

    try await waitUntil(timeout: 30) {
      store.record(id: "missing")?.name == "Hydrated"
    }

    let requests = await http.capturedRequests()
    try #require(requests.count >= 3)
    #expect(requests[1].subset?.whereClause == "id IN ($1)")
    #expect(requests[1].subset?.paramsJSON == #"{"1":"missing"}"#)
    #expect(requests[2].subset?.whereClause == "id IN ($1)")
    #expect(requests[2].subset?.paramsJSON == #"{"1":"missing"}"#)

    let hydrationPredicate = SQLExpression(
      predicate: .membership(field: "id", values: [.string("missing")])
    )
    let hydrationMetadata = metadata.recordedPredicate(
      table: TestRecord.tableName,
      predicate: fetchMetadataHash(predicate: hydrationPredicate, orderBy: [], limit: nil)
    )
    #expect(hydrationMetadata == nil)
  }

  @Test
  func ownerRequestSnapshot409HandleRotationRetriesThroughSameWriter() async throws {
    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let store = TestRecordStore()
    let eventHandler = CountingEventHandler()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [.truncate(handle: "next-handle")],
        [
          ElectricMessage.make(record: TestRecord(id: "1", name: "Alpha"), offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ],
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store,
      eventHandler: eventHandler
    )

    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("1"))
    )

    let results: [TestRecord]
    do {
      results = try await collection.query(where: predicate, orderBy: [], limit: 1)
    } catch {
      let trace = await http.capturedRequests().map {
        "offset=\($0.offset ?? "nil") handle=\($0.handle ?? "nil") live=\($0.live) subset=\($0.subset != nil)"
      }
      Issue.record("query threw \(error); requests: \(trace)")
      throw error
    }

    #expect(results.count == 1)
    #expect(await http.requestCount() == 2)
    #expect(store.clearCallCount() == 1)
    #expect(await eventHandler.willTruncateCount() == 1)
    #expect(await eventHandler.didTruncateCount() == 1)
    // The control batch invalidates stale coverage immediately, then the
    // replacement transaction clears it again at the authoritative boundary.
    #expect(
      metadata.clearedMetadataTables() == [
        TestRecord.tableName,
        TestRecord.tableName,
      ])

    let requests = await http.capturedRequests()
    let retryRequest = try #require(requests.last)
    #expect(retryRequest.offset == "-1")
    #expect(retryRequest.handle?.hasPrefix("cachebust-") == true)
    #expect(retryRequest.cursor == nil)
    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    let resumed = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(resumed.offset == "offset-1")
    #expect(resumed.handle == "handle-offset-1")
  }

  @Test
  func queryTruncateRetryIgnoresStaleLiveStreamIdentity() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let eventHandler = CountingEventHandler()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [.truncate(handle: "next-handle")],
        [
          ElectricMessage.make(record: TestRecord(id: "1", name: "Alpha"), offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ],
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    let staleLiveState = SyncState(
      offset: "offset-live",
      handle: "handle-live",
      cursor: "cursor-live",
      isUpToDate: true,
      lastSyncedAt: Date(timeIntervalSince1970: 1_234)
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: staleLiveState,
      transaction: nil
    )

    let collection = makeTestCollection(
      client: client,
      store: store,
      eventHandler: eventHandler
    )

    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .equal, value: .string("1"))
    )

    let results = try await collection.query(where: predicate, orderBy: [], limit: 1)

    #expect(results.count == 1)
    #expect(await http.requestCount() == 2)

    let requests = await http.capturedRequests()
    let firstRequest = try #require(requests.first)
    #expect(firstRequest.offset == "offset-live")
    let retryRequest = try #require(requests.last)
    // The retry must not resume from the invalidated persisted identity.
    #expect(retryRequest.offset == "-1")
    #expect(retryRequest.handle?.hasPrefix("cachebust-") == true)
    #expect(retryRequest.cursor == nil)

    // The reset and terminal subset retry commit through the same owner writer.
    let persisted = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(persisted.offset == "offset-1")
    #expect(persisted.handle == "handle-offset-1")
    #expect(persisted.cursor == nil)
  }

  @Test
  func truncateBatchClearsPersistedResumeIdentity() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [.truncate(handle: "next-handle")]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    store.upsert(TestRecord(id: "stale", name: "Stale"))

    let batch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false
      )
    )

    let output = try batch.apply(in: store)

    #expect(output.encounteredTruncate)
    #expect(metadata.clearedMetadataTables() == [TestRecord.tableName])

    let syncStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    let syncState = try #require(
      try metadata.getSyncState(collectionId: syncStateKey, transaction: nil)
    )
    #expect(syncState.offset == "-1")
    #expect(syncState.handle == nil)
    #expect(syncState.cursor == nil)
    #expect(!syncState.isUpToDate)
  }

  @Test
  func fetchSnapshotDoesNotBorrowOrAdvanceLiveStreamSyncState() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let lastSyncedAt = Date(timeIntervalSince1970: 1_234)

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "missing", name: "Hydrated"),
            offset: "offset-hydration",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-hydration"),
          ElectricMessage.subsetEnd(offset: "offset-hydration"),
        ]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "offset-live",
        handle: "handle-live",
        cursor: "cursor-live",
        isUpToDate: true,
        lastSyncedAt: lastSyncedAt
      ),
      transaction: nil
    )

    let predicate = SQLExpression(
      predicate: .membership(field: "id", values: [.string("missing")])
    )
    let batch = try #require(
      try await client.fetchSnapshot(
        TestRecord.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(predicate: predicate, orderBy: [], limit: nil),
        syncMode: .onDemand
      )
    )

    let requests = await http.capturedRequests()
    let request = try #require(requests.first)
    #expect(request.offset == "now")
    #expect(request.handle == nil)
    #expect(request.cursor == nil)

    let output = try batch.apply(in: store)
    #expect(output.records == [TestRecord(id: "missing", name: "Hydrated")])
    #expect(output.encounteredTruncate == false)

    let syncState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(syncState.offset == "offset-live")
    #expect(syncState.handle == "handle-live")
    #expect(syncState.cursor == "cursor-live")
    #expect(syncState.isUpToDate)
    #expect(syncState.lastSyncedAt == lastSyncedAt)
  }

  @Test
  func fetchSnapshotTruncateDoesNotStageOwnerResumeState() async throws {
    let metadata = InMemoryMetadataProvider()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [.truncate(handle: "next-handle")]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "offset-live",
        handle: "handle-live",
        cursor: "cursor-live",
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 1_234)
      ),
      transaction: nil
    )

    let predicate = SQLExpression(
      predicate: .membership(field: "id", values: [.string("missing")])
    )
    let batch = try #require(
      try await client.fetchSnapshot(
        TestRecord.self,
        basePredicate: nil,
        descriptor: QueryDescriptor(predicate: predicate, orderBy: [], limit: nil),
        syncMode: .onDemand
      )
    )
    let output = try batch.apply(in: TestRecordStore())
    #expect(output.encounteredTruncate)

    let syncState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(syncState.offset == "offset-live")
    #expect(syncState.handle == "handle-live")
    #expect(syncState.cursor == "cursor-live")
    #expect(syncState.isUpToDate)
  }

  @Test
  func snapshotEndRecordsCoverageAndSubsetEndAdvancesOwnerCursorState() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "1", name: "Alpha"),
            offset: "offset-1",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store
    )

    let predicate = SQLExpression("id = '1'")
    let results1 = try await collection.query(where: predicate, orderBy: [], limit: 1)

    #expect(results1 == [TestRecord(id: "1", name: "Alpha")])
    #expect(await http.requestCount() == 1)

    let recorded = try #require(
      metadata.recordedPredicate(
        table: TestRecord.tableName,
        predicate: fetchMetadataHash(predicate: predicate, orderBy: [], limit: 1)
      )
    )
    #expect(recorded.outcome == .present)
    #expect(
      recorded.snapshotBoundary
        == PostgresSnapshot(xmin: "10", xmax: "20", xipList: ["11", "12"])
    )
    #expect(recorded.isComplete)

    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    let streamState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(streamState.offset == "offset-1")
    #expect(streamState.handle == "handle-offset-1")
    #expect(streamState.cursor == nil)

    let results2 = try await collection.query(where: predicate, orderBy: [], limit: 1)
    #expect(results2 == results1)
    #expect(await http.requestCount() == 1)
  }

  @Test
  func initialOnDemandUnscopedLoadDoesNotBlockLaterOwnerBackfill() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(record: TestRecord(id: "1", name: "Alpha"), offset: "offset-1"),
          ElectricMessage.snapshotEnd(offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ],
        [
          ElectricMessage.make(record: TestRecord(id: "2", name: "Beta"), offset: "offset-2"),
          ElectricMessage.upToDate(offset: "offset-2"),
          ElectricMessage.subsetEnd(offset: "offset-2"),
        ],
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let onDemandCollection = makeTestCollection(
      client: client,
      store: store,
      syncMode: .onDemand
    )

    let bootstrapResults = try await onDemandCollection.query()
    #expect(bootstrapResults == [TestRecord(id: "1", name: "Alpha")])

    let onDemandRequests = await http.capturedRequests()
    let firstRequest = try #require(onDemandRequests.first)
    #expect(firstRequest.offset == "now")
    #expect(firstRequest.log == .changesOnly)

    let recorded = metadata.recordedPredicate(
      table: TestRecord.tableName,
      predicate: PredicateHash(from: nil as SQLExpression?)
    )
    #expect(recorded == nil || recorded?.isComplete == false)

    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    let streamState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(streamState.offset == "offset-1")
    #expect(streamState.handle == "handle-offset-1")
    #expect(streamState.cursor == nil)

    let backfillResults = try await onDemandCollection.query()
    #expect(await http.requestCount() == 2)
    #expect(Set(backfillResults.map(\.id)) == Set(["1", "2"]))
  }

  @Test
  func baseScopedProgressiveLoadDoesNotMarkUnscopedCoverage() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let recurringPredicate = SQLExpression(
      predicate: .comparison(field: "isRecurring", op: .equal, value: .bool(true))
    )

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(record: TestRecord(id: "1", name: "Recurring"), offset: "offset-1"),
          ElectricMessage.snapshotEnd(offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ],
        [
          ElectricMessage.make(record: TestRecord(id: "2", name: "General"), offset: "offset-2"),
          ElectricMessage.upToDate(offset: "offset-2"),
          ElectricMessage.subsetEnd(offset: "offset-2"),
        ],
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let recurringCollection = makeTestCollection(
      client: client,
      store: store,
      syncMode: .progressive,
      basePredicate: recurringPredicate
    )

    let recurringResults = try await recurringCollection.query()
    #expect(recurringResults == [TestRecord(id: "1", name: "Recurring")])

    let scopedPredicate = fetchMetadataHash(
      basePredicate: recurringPredicate,
      predicate: nil,
      orderBy: [],
      limit: nil
    )
    #expect(
      metadata.recordedPredicate(
        table: TestRecord.tableName,
        predicate: scopedPredicate
      ) == nil
    )
    #expect(
      metadata.recordedObservation(
        table: TestRecord.tableName,
        predicate: scopedPredicate
      ) == nil
    )

    let unscopedRecorded = metadata.recordedPredicate(
      table: TestRecord.tableName,
      predicate: fetchMetadataHash(predicate: nil, orderBy: [], limit: nil)
    )
    #expect(unscopedRecorded == nil || unscopedRecorded?.isComplete == false)

    let unscopedCollection = makeTestCollection(
      client: client,
      store: store,
      syncMode: .progressive
    )

    let backfillResults = try await unscopedCollection.query()
    #expect(await http.requestCount() == 2)
    #expect(Set(backfillResults.map(\.id)) == Set(["1", "2"]))
  }

  @Test
  func markFetchedDelegatesToMetadataProvider() async throws {
    let metadata = InMemoryMetadataProvider()
    let http = InMemoryHTTPClientProvider(responses: [])
    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let predicate = SQLExpression("status = 'open'")
    try await client.markFetched(TestRecord.self, where: predicate)

    let recorded = try #require(
      metadata.recordedPredicate(
        table: TestRecord.tableName,
        predicate: PredicateHash(from: predicate)
      )
    )
    #expect(recorded.isComplete)
    #expect(recorded.snapshotBoundary == nil)
    #expect(recorded.outcome == .present)
  }

  @Test
  func subsetEndRecordsCoverageAndAdvancesOwnerCursorState() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: TestRecord(id: "1", name: "Alpha"),
            offset: "offset-1",
            isSubsetSnapshot: true
          ),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store
    )

    let predicate = SQLExpression("id = '1'")
    let results1 = try await collection.query(where: predicate, orderBy: [], limit: 1)

    #expect(results1 == [TestRecord(id: "1", name: "Alpha")])
    #expect(await http.requestCount() == 1)

    let recorded = try #require(
      metadata.recordedPredicate(
        table: TestRecord.tableName,
        predicate: fetchMetadataHash(predicate: predicate, orderBy: [], limit: 1)
      )
    )
    #expect(recorded.snapshotBoundary == nil)
    #expect(recorded.outcome == .present)
    #expect(recorded.isComplete)

    let streamStateKey = legacyStreamStateKey(
      for: TestRecord.self,
      basePredicate: nil,
      syncMode: .onDemand
    )
    let streamState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(streamState.offset == "offset-1")
    #expect(streamState.handle == "handle-offset-1")
    #expect(streamState.cursor == nil)

    let results2 = try await collection.query(where: predicate, orderBy: [], limit: 1)
    #expect(results2 == results1)
    #expect(await http.requestCount() == 1)
  }

  @Test
  func queryDropsFetchedBatchWhenAuthSessionTearsDownBeforeApply() async throws {
    let sessionController = TestSessionController()
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let http = BlockingHTTPClientProvider(
      response: [
        ElectricMessage.make(record: TestRecord(id: "1", name: "Alpha"), offset: "offset-1"),
        ElectricMessage.upToDate(offset: "offset-1"),
        ElectricMessage.subsetEnd(offset: "offset-1"),
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker,
        sessionProvider: sessionController.provider()
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store
    )

    do {
      let predicate = SQLExpression("id = '1'")
      let queryTask = Task {
        try await collection.query(where: predicate, orderBy: [], limit: 1)
      }

      await http.waitForFetchStart()
      await sessionController.beginTeardown()
      await http.releaseFetch()

      await #expect(throws: CancellationError.self) {
        try await queryTask.value
      }
    }

    #expect(store.allRecords().isEmpty)
    let syncStateKey = ElectricSyncClientImpl.syncStateKey(
      collectionIdentifier: TestRecord.collectionIdentifier,
      predicateHash: fetchMetadataHash(predicate: SQLExpression("id = '1'"), orderBy: [], limit: 1)
    )
    #expect(try metadata.getSyncState(collectionId: syncStateKey, transaction: nil) == nil)
  }

  @Test
  func queriesFromDifferentSessionGenerationsDoNotShareInflightWork() async throws {
    let sessionController = TestSessionController()
    let metadata = InMemoryMetadataProvider()
    let record = TestRecord(id: "1", name: "Current generation")
    let http = BlockingHTTPClientProvider(
      response: [
        ElectricMessage.make(record: record, offset: "offset-1"),
        ElectricMessage.upToDate(offset: "offset-1"),
        ElectricMessage.subsetEnd(offset: "offset-1"),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        sessionProvider: sessionController.provider()
      )
    )
    let coordinator = ElectricCollectionBackgroundCoordinator<TestRecord>(
      syncMode: .onDemand,
      collectionIdentifier: TestRecord.collectionIdentifier,
      backgroundTaskProvider: NoopBackgroundTaskProvider(),
      runtimeProvider: .live,
      logger: NoopLogProvider(),
      tracer: NoopElectricSyncTracer()
    )
    let descriptor = QueryDescriptor(predicate: SQLExpression("id = '1'"), limit: 1)
    let firstSession = try #require(sessionController.captureAuthenticatedSession())

    let firstQuery = Task {
      try await coordinator.performQuery(
        client: client,
        basePredicate: nil,
        shapeTopology: .dnf,
        descriptor: descriptor,
        session: firstSession,
        transactionRunner: { operation in try operation(nil) },
        eventHandler: NoopElectricSyncEventHandler()
      )
    }
    await http.waitForFetchStart()

    let secondSession = sessionController.activate()
    let secondQuery = Task {
      try await coordinator.performQuery(
        client: client,
        basePredicate: nil,
        shapeTopology: .dnf,
        descriptor: descriptor,
        session: secondSession,
        transactionRunner: { operation in try operation(nil) },
        eventHandler: NoopElectricSyncEventHandler()
      )
    }
    try await waitUntilAsync(timeout: 30) {
      await http.fetchCount() == 2
    }

    await http.releaseFetch()

    await #expect(throws: CancellationError.self) {
      try await firstQuery.value
    }
    _ = try await secondQuery.value
    #expect(await http.fetchCount() == 2)
  }

  @Test
  func replicaCancellationJoinsBlockedQueryBeforeDatabaseSuspension() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let transactionCounter = TransactionCounter()
    let http = BlockingHTTPClientProvider(
      response: [
        ElectricMessage.make(
          record: TestRecord(id: "late", name: "Must not apply"),
          offset: "offset-late",
          isSubsetSnapshot: true
        ),
        ElectricMessage.snapshotEnd(offset: "offset-late"),
        ElectricMessage.subsetEnd(offset: "offset-late"),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter
    )

    do {
      let queryTask = Task {
        try await collection.query(where: SQLExpression("id = 'late'"), limit: 1)
      }
      await http.waitForFetchStart()

      let suspensionFence = AsyncCompletionFlag()
      let cancellationTask = Task {
        await collection.replica.cancelAndWait()
        await suspensionFence.markComplete()
      }
      try await waitUntilAsync(timeout: 30) {
        await http.didObserveCancellation()
      }

      #expect(await suspensionFence.isComplete() == false)
      #expect(await transactionCounter.count() == 0)
      #expect(store.allRecords().isEmpty)

      await http.releaseFetch()
      await cancellationTask.value

      #expect(await suspensionFence.isComplete())
      await #expect(throws: CancellationError.self) {
        try await queryTask.value
      }
      #expect(await transactionCounter.count() == 0)
      #expect(store.allRecords().isEmpty)

      await #expect(throws: CancellationError.self) {
        _ = try await collection.query(where: SQLExpression("id = 'new-command'"), limit: 1)
      }
      #expect(await http.fetchCount() == 1)
    }
  }

  @Test
  func replicaCancellationJoinsBlockedTransientQueryBeforeDatabaseSuspension() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let transactionCounter = TransactionCounter()
    let http = BlockingHTTPClientProvider(
      response: [
        ElectricMessage.make(
          record: TestRecord(id: "late", name: "Must not apply"),
          offset: "offset-late",
          isSubsetSnapshot: true
        ),
        ElectricMessage.snapshotEnd(offset: "offset-late"),
        ElectricMessage.subsetEnd(offset: "offset-late"),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter
    )

    do {
      let queryTask = Task {
        try await collection.ensureSubset(where: SQLExpression("id = 'late'"), limit: 1)
      }
      await http.waitForFetchStart()

      let suspensionFence = AsyncCompletionFlag()
      let cancellationTask = Task {
        await collection.replica.cancelAndWait()
        await suspensionFence.markComplete()
      }
      try await waitUntilAsync(timeout: 30) {
        await http.didObserveCancellation()
      }

      #expect(await suspensionFence.isComplete() == false)
      #expect(await transactionCounter.count() == 0)
      #expect(store.allRecords().isEmpty)

      await http.releaseFetch()
      await cancellationTask.value

      #expect(await suspensionFence.isComplete())
      await #expect(throws: CancellationError.self) {
        try await queryTask.value
      }
      #expect(await transactionCounter.count() == 0)
      #expect(store.allRecords().isEmpty)

      await #expect(throws: CancellationError.self) {
        _ = try await collection.query(where: SQLExpression("id = 'new-command'"), limit: 1)
      }
      #expect(await http.fetchCount() == 1)
    }
  }

  @Test
  func replicaCancellationJoinsFinalCacheReadBeforeDatabaseSuspension() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let cache = BlockingDataCacheProvider(store: store)
    let http = InMemoryHTTPClientProvider(
      responses: [[.upToDate(offset: "offset-ready"), .subsetEnd(offset: "offset-ready")]]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      cacheProvider: cache
    )

    do {
      let queryTask = Task {
        try await collection.query(where: SQLExpression("id = 'late-cache'"), limit: 1)
      }
      await cache.waitForLoadStart()

      let suspensionFence = AsyncCompletionFlag()
      let cancellationTask = Task {
        await collection.replica.cancelAndWait()
        await suspensionFence.markComplete()
      }
      try await waitUntilAsync(timeout: 30) {
        await cache.didObserveCancellation()
      }

      #expect(await suspensionFence.isComplete() == false)

      await cache.releaseLoad()
      await cancellationTask.value

      #expect(await suspensionFence.isComplete())
      await #expect(throws: CancellationError.self) {
        try await queryTask.value
      }
    }
  }

  @Test
  func queryCancelsBeforeFetchWhenNoAuthenticatedSnapshotIsAvailable() async throws {
    let sessionController = TestSessionController(initiallyAuthenticated: false)
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(record: TestRecord(id: "1", name: "Alpha"), offset: "offset-1"),
          ElectricMessage.upToDate(offset: "offset-1"),
        ]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker,
        sessionProvider: sessionController.provider()
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store
    )

    do {
      await #expect(throws: CancellationError.self) {
        _ = try await collection.query(where: SQLExpression("id = '1'"), orderBy: [], limit: 1)
      }
    }

    #expect(await http.requestCount() == 0)
    #expect(store.allRecords().isEmpty)
  }

  @Test
  func cursorTiePaginationRequestsTwoSubsetSnapshots() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(record: TestRecord(id: "tie-1", name: "Tie"), offset: "offset-1"),
          ElectricMessage.subsetEnd(offset: "offset-1"),
        ],
        [
          ElectricMessage.make(record: TestRecord(id: "next-1", name: "Next"), offset: "offset-2"),
          ElectricMessage.subsetEnd(offset: "offset-2"),
        ],
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store
    )

    let predicate = SQLExpression(
      predicate: .comparison(field: "status", op: .equal, value: .string("open"))
    )
    let orderBy = [OrderBy(field: "createdAt", direction: .descending)]
    let cursor = ElectricCursorExpressions(
      whereFrom: .comparison(field: "createdAt", op: .lessThan, value: .int(100)),
      whereCurrent: .comparison(field: "createdAt", op: .equal, value: .int(100))
    )

    _ = try await collection.query(
      where: predicate,
      orderBy: orderBy,
      limit: 10,
      cursor: cursor
    )

    #expect(await http.requestCount() == 2)

    let requests = await http.capturedRequests()
    #expect(requests.count == 2)

    let tiesRequest = requests[0]
    let pageRequest = requests[1]

    #expect(tiesRequest.subset?.limit == nil)
    #expect(pageRequest.subset?.limit == 10)

    #expect(tiesRequest.subset?.whereClause.contains("status") == true)
    #expect(pageRequest.subset?.whereClause.contains("status") == true)

    #expect(tiesRequest.subset?.whereClause.contains("created_at =") == true)
    #expect(pageRequest.subset?.whereClause.contains("created_at <") == true)

    let recorded = try #require(
      metadata.recordedPredicate(
        table: TestRecord.tableName,
        predicate: fetchMetadataHash(
          predicate: predicate,
          orderBy: orderBy,
          limit: 10,
          cursor: cursor
        )
      )
    )
    #expect(recorded.isComplete)
  }

  @Test
  func queryAppliesLargeBatchInSingleTransaction() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let transactionCounter = TransactionCounter()

    let recordsCount = 450
    let snapshotMessages = (0..<recordsCount).map { index in
      ElectricMessage.make(
        record: TestRecord(id: "record-\(index)", name: "Record \(index)"),
        offset: "offset-\(index)"
      )
    }
    let http = InMemoryHTTPClientProvider(
      responses: [
        snapshotMessages + [
          .upToDate(offset: "offset-\(recordsCount)"),
          .subsetEnd(offset: "offset-\(recordsCount)"),
        ]
      ]
    )

    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: tracker
      )
    )

    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter
    )

    _ = try await collection.query(where: nil, orderBy: [], limit: nil)

    #expect(await transactionCounter.count() == 1)
    #expect(store.allRecords().count == recordsCount)
    #expect(await http.requestCount() == 1)
  }

  @Test
  func nonFirstChunkTruncateDefersReplacementSwapUntilCleanBootstrap() async throws {
    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let store = TestRecordStore()
    store.upsert(TestRecord(id: "existing", name: "Existing generation"))

    let decodeChunkSize = 200
    let interruptedReplacement =
      (0..<decodeChunkSize).map { index in
        ElectricMessage.make(
          record: TestRecord(id: "interrupted-\(index)", name: "Interrupted \(index)"),
          offset: "interrupted-\(index)"
        )
      } + [
        .truncate(handle: "replacement-again"),
        .subsetEnd(offset: "interrupted-terminal"),
      ]
    let finalRecord = TestRecord(id: "replacement", name: "Replacement generation")
    let http = InMemoryHTTPClientProvider(
      responses: [
        [.truncate(handle: "replacement")],
        interruptedReplacement,
        [
          .make(record: finalRecord, offset: "replacement-final"),
          .subsetEnd(offset: "replacement-final"),
        ],
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let collection = makeTestCollection(client: client, store: store)

    let records: [TestRecord] = try await collection.query(where: nil, orderBy: [], limit: nil)

    #expect(records == [finalRecord])
    #expect(store.allRecords() == [finalRecord])
    #expect(store.clearCallCount() == 1)
    let requests = await http.capturedRequests()
    #expect(requests.count == 3)
    #expect(requests[1].offset == "-1")
    #expect(requests[2].offset == "-1")
  }

  @Test
  func grdbReplacementBoundaryNeverPublishesOrPersistsAPartialChunk() async throws {
    let databaseURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("electric-boundary-\(UUID().uuidString).sqlite")
    defer { try? FileManager.default.removeItem(at: databaseURL) }

    let database = try DatabasePool(path: databaseURL.path)
    try await database.write { db in
      try db.create(table: GRDBAtomicTestRecord.databaseTableName) { table in
        table.column("id", .text).primaryKey()
        table.column("name", .text).notNull()
      }
      try GRDBAtomicTestRecord(id: "existing", name: "Existing generation").insert(db)
    }

    let observation = ValueObservation.tracking { db in
      try String.fetchAll(
        db,
        sql: "SELECT id FROM \(GRDBAtomicTestRecord.databaseTableName) ORDER BY id"
      )
    }
    let observedIDs = GRDBObservationRecorder<[String]>()
    let observationTask = Task {
      do {
        for try await ids in observation.values(in: database) {
          await observedIDs.append(ids)
        }
      } catch is CancellationError {
        return
      } catch {
        Issue.record("GRDB observation failed: \(error)")
      }
    }
    defer { observationTask.cancel() }
    try await waitUntilAsync(timeout: 5) { await observedIDs.count() == 1 }

    let replacementMessages =
      (0..<201).map { index in
        ElectricMessage.grdbAtomicRecord(
          GRDBAtomicTestRecord(id: "replacement-\(index)", name: "Replacement \(index)"),
          offset: "replacement-\(index)"
        )
      } + [.subsetEnd(offset: "replacement-201")]
    let http = InMemoryHTTPClientProvider(
      responses: [
        [.truncate(handle: nil)],
        replacementMessages,
      ]
    )
    let metadata = InMemoryMetadataProvider()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let lateWriteGate = GRDBLateWriteGate(blockedWrite: 201, failsAfterRelease: true)
    let transactionRunner: ElectricTransactionRunner = { operation in
      try await database.write { db in
        try operation(GRDBAtomicTransactionContext(database: db, gate: lateWriteGate))
      }
    }
    let collection = ElectricCollection(
      configuration: ElectricCollectionConfiguration(
        modelType: GRDBAtomicTestRecord.self,
        syncMode: .onDemand,
        shapeTopology: .staticallySimple
      ),
      client: client,
      cacheProvider: GRDBAtomicCacheProvider(database: database),
      transactionRunner: transactionRunner
    )

    let queryTask = Task {
      try await collection.query(where: SQLExpression("id IS NOT NULL"))
    }
    try await lateWriteGate.waitUntilBlocked()

    let visibleIDs = try await database.read { db in
      try String.fetchAll(
        db,
        sql: "SELECT id FROM \(GRDBAtomicTestRecord.databaseTableName) ORDER BY id"
      )
    }
    #expect(visibleIDs == ["existing"])
    #expect(await observedIDs.values() == [["existing"]])

    lateWriteGate.release()
    await #expect(throws: ElectricSyncError.self) {
      _ = try await queryTask.value
    }

    let durableIDs = try await database.read { db in
      try String.fetchAll(
        db,
        sql: "SELECT id FROM \(GRDBAtomicTestRecord.databaseTableName) ORDER BY id"
      )
    }
    #expect(durableIDs == ["existing"])
    try await Task.sleep(for: .milliseconds(50))
    #expect(await observedIDs.values() == [["existing"]])
  }

  @Test
  func sharedReplicaRegistersOneOwnerAcrossDistinctDemands() async throws {
    let metadata = InMemoryMetadataProvider()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: InMemoryHTTPClientProvider(responses: []),
        isExactCursorCutoverEnabled: true
      )
    )
    let basePredicate = SQLExpression("tenant = '\(UUID().uuidString)'")
    let collection = makeTestCollection(
      client: client,
      store: TestRecordStore(),
      basePredicate: basePredicate
    )
    let replica = collection.replica
    let starts = ThreadSafeCounter()

    let firstToken = replica.acquireStream(syncMode: .onDemand) {
      starts.increment()
      return Task {
        while !Task.isCancelled {
          try? await Task.sleep(nanoseconds: 5_000_000)
        }
      }
    }
    let secondToken = replica.acquireStream(syncMode: .onDemand) {
      Issue.record("A second demand must not start another shape owner")
      return Task {}
    }

    #expect(starts.value == 1)
    #expect(replica.liveOwnerCount == 1)
    #expect(
      ElectricCursorOwnershipDiagnostics.shared.ownerCount(
        persistedCursorKey: replica.identity.persistedCursorKey
      ) == 1
    )

    firstToken.cancel()
    secondToken.cancel()
    replica.cancel()
    #expect(
      ElectricCursorOwnershipDiagnostics.shared.ownerCount(
        persistedCursorKey: replica.identity.persistedCursorKey
      ) == 0
    )
  }

  @Test(.timeLimit(.minutes(1)))
  func livePublicationWaitsForSnapshotPublication() async throws {
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: InMemoryMetadataProvider(),
        httpClient: InMemoryHTTPClientProvider(responses: [])
      )
    )
    let replica = makeTestCollection(client: client, store: TestRecordStore()).replica
    let snapshotStarted = AsyncTestLatch()
    let releaseSnapshot = AsyncTestLatch()
    let recorder = ReplicaPublicationRecorder()

    let snapshotTask = Task {
      try await replica.ensureSubset {
        await recorder.append("snapshot.start")
        await snapshotStarted.open()
        await releaseSnapshot.wait()
        await recorder.append("snapshot.end")
      }
    }

    await snapshotStarted.wait()
    let liveTask = Task {
      try await replica.withStreamPublication {
        await recorder.append("live")
      }
    }

    do {
      try await waitForPublicationWaiter(replica)
      #expect(await recorder.events() == ["snapshot.start"])

      await releaseSnapshot.open()
      try await snapshotTask.value
      try await liveTask.value
    } catch {
      snapshotTask.cancel()
      liveTask.cancel()
      await releaseSnapshot.open()
      _ = await snapshotTask.result
      _ = await liveTask.result
      throw error
    }

    #expect(await recorder.events() == ["snapshot.start", "snapshot.end", "live"])
  }

  @Test(.timeLimit(.minutes(1)))
  func twoConcurrentSubsetsPublishSerially() async throws {
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: InMemoryMetadataProvider(),
        httpClient: InMemoryHTTPClientProvider(responses: [])
      )
    )
    let replica = makeTestCollection(client: client, store: TestRecordStore()).replica
    let firstStarted = AsyncTestLatch()
    let releaseFirst = AsyncTestLatch()
    let recorder = ReplicaPublicationRecorder()

    let firstTask = Task {
      try await replica.ensureSubset {
        await recorder.begin("first")
        await firstStarted.open()
        await releaseFirst.wait()
        await recorder.end("first")
      }
    }

    await firstStarted.wait()
    let secondTask = Task {
      try await replica.ensureSubset {
        await recorder.begin("second")
        await recorder.end("second")
      }
    }

    do {
      try await waitForPublicationWaiter(replica)
      let blockedState = await recorder.state()
      #expect(blockedState.events == ["first.start"])
      #expect(blockedState.activeCount == 1)
      #expect(blockedState.maximumActiveCount == 1)

      await releaseFirst.open()
      try await firstTask.value
      try await secondTask.value
    } catch {
      firstTask.cancel()
      secondTask.cancel()
      await releaseFirst.open()
      _ = await firstTask.result
      _ = await secondTask.result
      throw error
    }

    let completedState = await recorder.state()
    #expect(
      completedState.events
        == ["first.start", "first.end", "second.start", "second.end"]
    )
    #expect(completedState.activeCount == 0)
    #expect(completedState.maximumActiveCount == 1)
  }

  @Test
  func snapshotMustRefetchDropsPartialRowsAndRestartsWithoutStaleCursor() async throws {
    let metadata = InMemoryMetadataProvider(supportsDurableRowOwnership: false)
    let store = TestRecordStore()
    let stale = TestRecord(id: "stale", name: "Stale")
    let partial = TestRecord(id: "partial", name: "Partial")
    let fresh = TestRecord(id: "fresh", name: "Fresh")
    store.upsert(stale)

    let http = InMemoryHTTPClientProvider(
      responses: [
        [
          ElectricMessage.make(
            record: partial,
            offset: "offset-partial",
            cursor: "cursor-partial",
            isSubsetSnapshot: true
          ),
          ElectricMessage.mustRefetch(offset: "offset-reset"),
        ],
        [
          ElectricMessage.make(
            record: fresh,
            offset: "offset-fresh",
            cursor: "cursor-fresh",
            isSubsetSnapshot: true
          ),
          ElectricMessage.snapshotEnd(offset: "offset-fresh"),
          ElectricMessage.subsetEnd(offset: "offset-fresh"),
        ],
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata)
      )
    )
    let collection = makeTestCollection(client: client, store: store)
    let streamStateKey = collection.replica.identity.legacyPersistedCursorKey(syncMode: .onDemand)
    try metadata.updateSyncState(
      collectionId: streamStateKey,
      state: SyncState(
        offset: "offset-old",
        handle: "handle-old",
        cursor: "cursor-old",
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 1_000)
      ),
      transaction: nil
    )
    let sessionController = TestSessionController()
    let session = try #require(sessionController.captureAuthenticatedSession())

    let records = try await collection.query(
      where: nil,
      orderBy: [],
      limit: nil,
      session: session
    )

    #expect(records == [fresh])
    #expect(store.allRecords() == [fresh])
    #expect(store.clearCallCount() == 1)

    let requests = await http.capturedRequests()
    #expect(requests.count == 2)
    #expect(requests[0].offset == "offset-old")
    #expect(requests[0].handle == "handle-old")
    #expect(requests[0].cursor == "cursor-old")
    #expect(requests[1].offset == "-1")
    #expect(requests[1].handle?.hasPrefix("cachebust-") == true)
    #expect(requests[1].cursor == nil)

    let resumedState = try #require(
      try metadata.getSyncState(collectionId: streamStateKey, transaction: nil)
    )
    #expect(resumedState.offset == "offset-fresh")
    #expect(resumedState.handle == "handle-offset-fresh")
    #expect(resumedState.cursor == "cursor-fresh")
  }

  @Test
  func replicaIdentityUsesStableCompleteBaseShapeSerialization() {
    let predicate = SQLExpression("tenant_id = 'tenant-1'")
    let endpointV1 = ElectricShapeWireIdentity(
      endpoint: "/shapes/test-records",
      selectedColumns: ["name", "id"]
    )
    let first = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: predicate,
      wireIdentity: endpointV1
    )
    let equivalent = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: SQLExpression("  tenant_id = 'tenant-1'  "),
      wireIdentity: ElectricShapeWireIdentity(
        endpoint: "shapes/test-records",
        selectedColumns: ["id", "name"]
      )
    )
    let differentEndpoint = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: predicate,
      wireIdentity: ElectricShapeWireIdentity(
        endpoint: "/shapes/test-records-v2",
        selectedColumns: ["id", "name"]
      )
    )
    let differentSelectedColumns = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: predicate,
      wireIdentity: ElectricShapeWireIdentity(
        endpoint: "/shapes/test-records",
        selectedColumns: ["id", "name", "version"]
      )
    )
    let differentAuthorizationScope = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: SQLExpression("tenant_id = 'tenant-2'"),
      wireIdentity: endpointV1
    )

    #expect(first == equivalent)
    #expect(first.persistedCursorKey == equivalent.persistedCursorKey)
    #expect(first != differentEndpoint)
    #expect(first.persistedCursorKey != differentEndpoint.persistedCursorKey)
    #expect(first != differentSelectedColumns)
    #expect(first.persistedCursorKey != differentSelectedColumns.persistedCursorKey)
    #expect(first != differentAuthorizationScope)
    #expect(first.persistedCursorKey != differentAuthorizationScope.persistedCursorKey)
  }

  @Test
  func customReplicaIdentifiersPersistIndependentCursorState() async throws {
    let metadata = InMemoryMetadataProvider()
    let http = InMemoryHTTPClientProvider(responses: [
      [.upToDate(offset: "offset-a")],
      [.upToDate(offset: "offset-b")],
    ])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true
      )
    )
    let firstIdentity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: "custom-test-records-a",
      basePredicate: nil
    )
    let secondIdentity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: "custom-test-records-b",
      basePredicate: nil
    )

    let firstBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false,
        replicaIdentity: firstIdentity
      )
    )
    let secondBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: false,
        replicaIdentity: secondIdentity
      )
    )

    _ = try firstBatch.apply(in: TestRecordStore())
    _ = try secondBatch.apply(in: TestRecordStore())

    #expect(firstBatch.collectionIdentifier == "custom-test-records-a")
    #expect(secondBatch.collectionIdentifier == "custom-test-records-b")
    #expect(firstBatch.streamStateKey == firstIdentity.persistedCursorKey)
    #expect(secondBatch.streamStateKey == secondIdentity.persistedCursorKey)
    #expect(firstBatch.streamStateKey != secondBatch.streamStateKey)
    #expect(
      try metadata.getSyncState(
        collectionId: firstIdentity.persistedCursorKey,
        transaction: nil
      )?.offset == "offset-a"
    )
    #expect(
      try metadata.getSyncState(
        collectionId: secondIdentity.persistedCursorKey,
        transaction: nil
      )?.offset == "offset-b"
    )
    let requests = await http.capturedRequests()
    #expect(
      requests.map(\.wireIdentity) == [
        TestRecord.electricShapeWireIdentity,
        TestRecord.electricShapeWireIdentity,
      ])
  }

  @Test
  func disabledExactCutoverResumesAndAdvancesLegacyModeCursor() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let legacyKey = identity.legacyPersistedCursorKey(syncMode: .progressive)
    try metadata.updateSyncState(
      collectionId: legacyKey,
      state: SyncState(
        offset: "legacy-offset",
        handle: "legacy-handle",
        cursor: "legacy-cursor",
        isUpToDate: true,
        lastSyncedAt: Date(timeIntervalSince1970: 123)
      ),
      transaction: nil
    )
    let http = InMemoryHTTPClientProvider(
      responses: [[.upToDate(offset: "next-offset", cursor: "next-cursor")]]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(metadataProvider: metadata, httpClient: http)
    )

    let batch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .progressive,
        live: true
      )
    )

    let request = try #require(await http.capturedRequests().first)
    #expect(request.offset == "legacy-offset")
    #expect(request.handle == "legacy-handle")
    #expect(request.cursor == "legacy-cursor")
    #expect(request.live)
    #expect(batch.streamStateKey == legacyKey)

    _ = try batch.apply(in: TestRecordStore())

    let advancedLegacyState = try #require(
      try metadata.getSyncState(collectionId: legacyKey, transaction: nil)
    )
    #expect(advancedLegacyState.offset == "next-offset")
    #expect(advancedLegacyState.handle == "handle-next-offset")
    #expect(advancedLegacyState.cursor == "next-cursor")
    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil) == nil
    )
  }

  @Test
  func legacyOnlyResetRequiresAdmissionAfterUpgrade() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let legacyKey = "\(TestRecord.tableName)|stream|mode:progressive|base:none"
    let legacyState = SyncState(
      offset: "-1",
      handle: "legacy-handle",
      cursor: nil,
      isUpToDate: false,
      lastSyncedAt: Date(timeIntervalSince1970: 123)
    )
    try metadata.updateSyncState(
      collectionId: legacyKey,
      state: legacyState,
      transaction: nil
    )
    let http = InMemoryHTTPClientProvider(responses: [[.upToDate(offset: "next-offset")]])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true
      )
    )

    do {
      _ = try await client.withLegacyBootstrapAdmission(identity: identity, stage: "test") {
        try await client.pollStream(
          TestRecord.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: true
        )
      }
      Issue.record("Expected the legacy-only reset to fail closed")
    } catch ElectricSyncError.legacyExactMissBootstrapDisabled {
      // Expected: the default configuration does not allow a cold bootstrap.
    } catch {
      Issue.record("Unexpected error: \(error)")
    }

    #expect(await http.requestCount() == 0)
    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil) == nil)
    let untouchedLegacyState = try #require(
      try metadata.getSyncState(collectionId: legacyKey, transaction: nil)
    )
    #expect(untouchedLegacyState.offset == legacyState.offset)
    #expect(untouchedLegacyState.handle == legacyState.handle)
    #expect(untouchedLegacyState.cursor == legacyState.cursor)
    #expect(untouchedLegacyState.isUpToDate == legacyState.isUpToDate)
  }

  @Test
  func legacyModeResetRequiresAdmissionBeforeTransportAfterOwnershipUpgrade() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let legacyKey = identity.legacyPersistedCursorKey(syncMode: .onDemand)
    try metadata.updateSyncState(
      collectionId: legacyKey,
      state: SyncState(
        offset: "-1",
        handle: nil,
        cursor: nil,
        isUpToDate: false,
        lastSyncedAt: nil
      ),
      transaction: nil
    )
    let http = InMemoryHTTPClientProvider(responses: [[.upToDate(offset: "next-offset")]])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: false
      )
    )

    await #expect(throws: ElectricSyncError.self) {
      _ = try await client.withLegacyBootstrapAdmission(
        identity: identity,
        stage: "legacy_mode_upgrade",
        syncMode: .onDemand
      ) {
        try await client.pollStream(
          TestRecord.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: true,
          replicaIdentity: identity
        )
      }
    }

    #expect(await http.requestCount() == 0)
    #expect(
      try metadata.getSyncState(collectionId: legacyKey, transaction: nil)?.offset == "-1"
    )
  }

  @Test
  func legacyCursorExactMissBootstrapsOnlyWhenExplicitlyEnabled() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let firstKey = "\(TestRecord.tableName)|stream|mode:eager|base:none"
    let secondKey = "\(TestRecord.tableName)|stream|mode:onDemand|base:none"
    try metadata.updateSyncState(
      collectionId: firstKey,
      state: SyncState(
        offset: "offset-a",
        handle: "handle-a",
        cursor: "cursor-a",
        isUpToDate: true,
        lastSyncedAt: nil
      ),
      transaction: nil
    )
    try metadata.updateSyncState(
      collectionId: secondKey,
      state: SyncState(
        offset: "offset-b",
        handle: "handle-b",
        cursor: "cursor-b",
        isUpToDate: true,
        lastSyncedAt: nil
      ),
      transaction: nil
    )
    let http = InMemoryHTTPClientProvider(responses: [
      [.upToDate(offset: "bootstrap-offset")],
      [.truncate(handle: "replacement-handle")],
    ])
    let controller = ElectricLegacyBootstrapAdmissionController(enabled: true)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true,
        legacyBootstrapAdmissionController: controller
      )
    )

    _ = try await client.withLegacyBootstrapAdmission(
      identity: identity,
      stage: "test"
    ) {
      let batch = try #require(
        try await client.pollStream(
          TestRecord.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: true,
          replicaIdentity: identity
        )
      )
      return try batch.apply(in: TestRecordStore())
    }

    let request = try #require(await http.capturedRequests().first)
    #expect(request.offset == "-1")
    #expect(request.live == false)
    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil)?.offset
        == "bootstrap-offset")
    for legacyKey in [firstKey, secondKey] {
      let rollbackState = try #require(
        try metadata.getSyncState(collectionId: legacyKey, transaction: nil)
      )
      #expect(rollbackState.offset != "-1")
    }
    let metrics = await controller.metricsSnapshot()
    #expect(metrics.admitted == 1)
    #expect(metrics.completed == 1)
    #expect(metrics.exactCursorAdvanced == 1)

    let truncateBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: true,
        replicaIdentity: identity
      )
    )
    _ = try truncateBatch.apply(in: TestRecordStore())
    await controller.setEnabled(false)
    let restartedClient = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true,
        legacyBootstrapAdmissionController: controller
      )
    )
    await #expect(throws: ElectricSyncError.self) {
      _ = try await restartedClient.withLegacyBootstrapAdmission(
        identity: identity,
        stage: "restart_after_truncate"
      ) {
        try await restartedClient.pollStream(
          TestRecord.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: true,
          replicaIdentity: identity
        )
      }
    }
    #expect(await http.requestCount() == 2)
  }

  @Test
  func provenV1LegacyCursorAdoptsAndDualWritesAdvancement() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let legacyKey = "\(TestRecord.tableName)|stream|mode:progressive|base:all"
    let legacyState = SyncState(
      offset: "legacy-offset",
      handle: "legacy-handle",
      cursor: "legacy-cursor",
      isUpToDate: true,
      lastSyncedAt: Date(timeIntervalSince1970: 123)
    )
    try metadata.updateSyncState(
      collectionId: legacyKey,
      state: legacyState,
      transaction: nil
    )
    let http = InMemoryHTTPClientProvider(responses: [
      [.upToDate(offset: "next-offset")],
      [.truncate(handle: "replacement-handle")],
    ])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true
      )
    )

    let batch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: true,
        replicaIdentity: identity
      )
    )
    _ = try batch.apply(in: TestRecordStore())

    let request = try #require(await http.capturedRequests().first)
    #expect(request.offset == "legacy-offset")
    #expect(request.handle == "legacy-handle")
    #expect(request.cursor == "legacy-cursor")
    #expect(request.live)
    #expect(
      try metadata.getSyncState(
        collectionId: identity.persistedCursorKey,
        transaction: nil
      )?.offset == "next-offset"
    )
    #expect(
      try metadata.getSyncState(collectionId: legacyKey, transaction: nil)?.offset
        == "next-offset"
    )

    let truncateBatch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: true,
        replicaIdentity: identity
      )
    )
    let truncateOutput = try truncateBatch.apply(in: TestRecordStore())
    #expect(truncateOutput.encounteredTruncate)
    #expect(
      try metadata.getSyncState(
        collectionId: identity.persistedCursorKey,
        transaction: nil
      )?.offset == "-1"
    )
    #expect(try metadata.getSyncState(collectionId: legacyKey, transaction: nil)?.offset == "-1")
    for absentLegacyKey in identity.legacyPersistedCursorKeys where absentLegacyKey != legacyKey {
      #expect(try metadata.getSyncState(collectionId: absentLegacyKey, transaction: nil) == nil)
    }
  }

  @Test
  func freshExactCursorDoesNotManufactureLegacyRollbackState() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let http = InMemoryHTTPClientProvider(responses: [
      [.upToDate(offset: "first-offset")],
      [.truncate(handle: "replacement-handle")],
      [.upToDate(offset: "second-offset")],
    ])
    let disabledController = ElectricLegacyBootstrapAdmissionController()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true,
        legacyBootstrapAdmissionController: disabledController
      )
    )

    for expectedOffset in ["first-offset", "-1", "second-offset"] {
      let batch = try #require(
        try await client.pollStream(
          TestRecord.self,
          basePredicate: nil,
          syncMode: .onDemand,
          live: true,
          replicaIdentity: identity
        )
      )
      _ = try batch.apply(in: TestRecordStore())
      #expect(
        try metadata.getSyncState(
          collectionId: identity.persistedCursorKey,
          transaction: nil
        )?.offset == expectedOffset
      )
    }

    #expect(
      try metadata.getSyncState(
        collectionId: identity.persistedCursorKey,
        transaction: nil
      )?.offset == "second-offset"
    )
    for legacyKey in identity.legacyPersistedCursorKeys {
      #expect(try metadata.getSyncState(collectionId: legacyKey, transaction: nil) == nil)
    }
  }

  @Test
  func disablingRollbackDualWriteAtApplyKeepsExactWriteAndLeavesLegacyUntouched() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let legacyKey = "\(TestRecord.tableName)|stream|mode:progressive|base:all"
    let legacyState = SyncState(
      offset: "legacy-offset",
      handle: "legacy-handle",
      cursor: "legacy-cursor",
      isUpToDate: true,
      lastSyncedAt: nil
    )
    try metadata.updateSyncState(
      collectionId: legacyKey,
      state: legacyState,
      transaction: nil
    )
    let rollbackSwitch = RuntimeBoolean(true)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: InMemoryHTTPClientProvider(responses: [[.upToDate(offset: "exact-next")]]),
        isExactCursorCutoverEnabled: true,
        isRollbackDualWriteEnabled: { rollbackSwitch.value }
      )
    )

    let batch = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: true,
        replicaIdentity: identity
      )
    )
    rollbackSwitch.set(false)
    _ = try batch.apply(in: TestRecordStore())

    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil)?.offset
        == "exact-next")
    #expect(
      try metadata.getSyncState(collectionId: legacyKey, transaction: nil)?.offset
        == legacyState.offset)
  }

  @Test
  func truncateInvalidationReachesRollbackKeysWhileDualWriteIsDisabled() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let legacyKey = "\(TestRecord.tableName)|stream|mode:progressive|base:all"
    try metadata.updateSyncState(
      collectionId: legacyKey,
      state: SyncState(
        offset: "legacy-offset",
        handle: "legacy-handle",
        cursor: "legacy-cursor",
        isUpToDate: true,
        lastSyncedAt: nil
      ),
      transaction: nil
    )
    let rollbackSwitch = RuntimeBoolean(true)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: InMemoryHTTPClientProvider(responses: [
          [.upToDate(offset: "exact-next")],
          [.truncate(handle: "replacement-handle")],
        ]),
        isExactCursorCutoverEnabled: true,
        isRollbackDualWriteEnabled: { rollbackSwitch.value }
      )
    )

    let advance = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: true,
        replicaIdentity: identity
      )
    )
    _ = try advance.apply(in: TestRecordStore())

    rollbackSwitch.set(false)
    let truncate = try #require(
      try await client.pollStream(
        TestRecord.self,
        basePredicate: nil,
        syncMode: .onDemand,
        live: true,
        replicaIdentity: identity
      )
    )
    _ = try truncate.apply(in: TestRecordStore())

    rollbackSwitch.set(true)
    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil)?
        .canResumeWithoutFullBootstrap == false)
    #expect(
      try metadata.getSyncState(collectionId: legacyKey, transaction: nil)?
        .canResumeWithoutFullBootstrap == false)
  }

  @Test(.timeLimit(.minutes(1)))
  func requestSnapshotCancelsBlockedLivePollBeforeSubsetAndPreservesOwnerOrdering() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let snapshotRecord = TestRecord(id: "row-1", name: "Snapshot")
    let staleRecord = TestRecord(id: "row-1", name: "Stale live overlap")
    let snapshotMessages = [
      ElectricMessage.make(
        record: snapshotRecord,
        offset: "snapshot-offset",
        key: "row-1",
        isSubsetSnapshot: true
      ),
      ElectricMessage.snapshotEnd(offset: "snapshot-offset"),
      ElectricMessage.subsetEnd(offset: "snapshot-offset"),
    ]
    let stalePayload = try JSONEncoder().encode(staleRecord)
    let overlappingLiveMessages = [
      ElectricMessage(
        payload: stalePayload,
        key: "row-1",
        offset: "live-overlap-offset",
        handle: "handle-live-overlap-offset",
        kind: .mutation,
        txids: [9]
      ),
      ElectricMessage.upToDate(offset: "live-overlap-offset"),
    ]
    let http = SnapshotParityHTTPClientProvider(
      snapshotMessages: snapshotMessages,
      overlappingLiveMessages: overlappingLiveMessages
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true
      )
    )
    let collection = makeTestCollection(client: client, store: store)
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      let token = collection.keepSynced(
        session: session
      )
      defer {
        token.cancel()
        collection.replica.cancel()
      }

      try await waitUntilAsync(timeout: 30) {
        await http.requestCount() >= 2
      }

      let subsetTask = Task {
        try await collection.ensureSubset(
          where: SQLExpression("id = 'row-1'"),
          session: session
        )
      }
      let result: ElectricSubsetResult<TestRecord>
      do {
        result = try await awaitTaskResultBeforeTimeout(subsetTask, timeout: .seconds(4))
      } catch {
        subsetTask.cancel()
        token.cancel()
        collection.replica.cancel()
        await http.abortBlockedLiveRequest()
        _ = await subsetTask.result
        throw error
      }
      #expect(result.appliedRecords == [snapshotRecord])

      try await waitUntilAsync(timeout: 30) {
        await http.requestCount() >= 5
      }

      let requests = await http.capturedRequests()
      try #require(requests.count >= 5)
      #expect(!requests[0].live)
      #expect(requests[0].offset == "-1")
      #expect(requests[0].subset == nil)
      #expect(requests[1].live)
      #expect(requests[1].offset == "owner-offset")
      #expect(!requests[2].live)
      #expect(requests[2].offset == "owner-offset")
      #expect(requests[2].subset != nil)
      #expect(requests[3].live)
      #expect(requests[3].offset == "snapshot-offset")
      #expect(requests[3].subset == nil)
      #expect(requests[4].live)
      #expect(requests[4].offset == "live-overlap-offset")
      #expect(await http.cancelledLiveRequestCount() == 1)
      #expect(await http.subsetStartedAfterLiveCancellation())
      #expect(store.record(id: "row-1") == snapshotRecord)

      let state = try #require(
        try metadata.getSyncState(
          collectionId: collection.replica.identity.persistedCursorKey,
          transaction: nil
        )
      )
      #expect(state.offset == "live-overlap-offset")
      #expect(state.isUpToDate)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func cancellationInsensitiveLiveBatchCannotPublishOrAdvanceCursorBeforeSubset() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let snapshotRecord = TestRecord(id: "row-1", name: "Snapshot")
    let staleLiveRecord = TestRecord(id: "row-1", name: "Stale live")
    let snapshotMessages = [
      ElectricMessage.make(
        record: snapshotRecord,
        offset: "snapshot-offset",
        key: snapshotRecord.id,
        isSubsetSnapshot: true
      ),
      ElectricMessage.snapshotEnd(offset: "snapshot-offset"),
      ElectricMessage.subsetEnd(offset: "snapshot-offset"),
    ]
    let staleLiveMessages = [
      try ElectricMessage(
        payload: JSONEncoder().encode(staleLiveRecord),
        key: staleLiveRecord.id,
        offset: "stale-live-offset",
        handle: "stale-live-handle",
        kind: .mutation
      ),
      ElectricMessage.upToDate(offset: "stale-live-offset"),
    ]
    let http = CancellationInsensitiveLiveHTTPClientProvider(
      snapshotMessages: snapshotMessages,
      staleLiveMessages: staleLiveMessages
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true
      )
    )
    let collection = makeTestCollection(client: client, store: store)
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      let token = collection.keepSynced(session: session)
      defer {
        token.cancel()
        collection.replica.cancel()
      }

      try await waitUntilAsync(timeout: 5) {
        await http.firstLiveRequestStarted()
      }

      let subsetTask = Task {
        try await collection.ensureSubset(
          where: SQLExpression("id = 'row-1'"),
          session: session
        )
      }
      let result: ElectricSubsetResult<TestRecord>
      do {
        result = try await awaitTaskResultBeforeTimeout(subsetTask, timeout: .seconds(4))
      } catch {
        subsetTask.cancel()
        token.cancel()
        collection.replica.cancel()
        await http.abortBlockedLiveRequest()
        _ = await subsetTask.result
        throw error
      }

      #expect(result.appliedRecords == [snapshotRecord])
      #expect(await http.firstLiveReturnedAfterCancellation())
      #expect(await http.subsetStartedAfterStaleLiveReturn())
      #expect(store.record(id: snapshotRecord.id) == snapshotRecord)
      #expect(store.writeCallCount() == 1)

      let state = try #require(
        try metadata.getSyncState(
          collectionId: collection.replica.identity.persistedCursorKey,
          transaction: nil
        )
      )
      #expect(state.offset == "snapshot-offset")
      #expect(state.handle != "stale-live-handle")
      #expect(
        !metadata.syncStateHistory(
          collectionId: collection.replica.identity.persistedCursorKey
        ).contains { $0.offset == "stale-live-offset" }
      )
    }
  }

  @Test
  func unsupportedMoveInQuarantinesOwnerWithoutPublicationOrRefetchLoop() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let telemetry = RecordingElectricCursorTelemetry()
    let snapshotRecord = TestRecord(id: "row-1", name: "Snapshot")
    let rejectedRecord = TestRecord(id: "row-1", name: "Rejected live change")
    let snapshotMessages = [
      ElectricMessage.make(
        record: snapshotRecord,
        offset: "snapshot-offset",
        key: "row-1",
        isSubsetSnapshot: true
      ),
      ElectricMessage.snapshotEnd(offset: "snapshot-offset"),
      ElectricMessage.subsetEnd(offset: "snapshot-offset"),
    ]
    let rejectedMessages = [
      ElectricMessage(
        payload: try JSONEncoder().encode(rejectedRecord),
        key: "row-1",
        offset: "rejected-offset",
        handle: "handle-rejected-offset",
        kind: .mutation,
        txids: [20]
      ),
      ElectricMessage(
        payload: Data(),
        offset: "rejected-offset",
        handle: "handle-rejected-offset",
        kind: .mutation,
        event: .moveIn(patterns: [MovePattern(pos: 0, value: "pending")])
      ),
      ElectricMessage.upToDate(offset: "rejected-offset"),
    ]
    let http = SnapshotParityHTTPClientProvider(
      snapshotMessages: snapshotMessages,
      liveMessageResponses: [rejectedMessages]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        tracer: telemetry,
        isExactCursorCutoverEnabled: true
      )
    )
    let collection = makeTestCollection(client: client, store: store, logger: telemetry)
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      let token = collection.keepSynced(
        session: session,
        circuitBreaker: ZeroDelayCircuitBreaker()
      )
      defer {
        token.cancel()
        collection.replica.cancel()
      }

      try await waitUntilAsync(timeout: 30) {
        await http.requestCount() >= 2
      }

      let result = try await collection.ensureSubset(
        where: SQLExpression("id = 'row-1'"),
        session: session
      )
      #expect(result.appliedRecords == [snapshotRecord])

      try await waitUntilAsync(timeout: 30) {
        await http.deliveredLiveMessageResponseCount() == 1
      }
      try await waitUntil(timeout: 30) {
        telemetry.recordedLogs().contains { log in
          log.message.contains("Electric protocol quarantined")
        }
      }

      let requestCountAfterQuarantine = await http.requestCount()
      try await Task.sleep(nanoseconds: 100_000_000)

      #expect(await http.requestCount() == requestCountAfterQuarantine)
      #expect(store.record(id: "row-1") == snapshotRecord)
      #expect(store.writeCallCount() == 1)
      let quarantineLogs = telemetry.recordedLogs().filter {
        $0.message.contains("Electric protocol quarantined")
      }
      #expect(quarantineLogs.count == 1)
      #expect(quarantineLogs.first?.metadata["reason"] == "move_in")
      #expect(
        quarantineLogs.first?.metadata["capability.gate"]
          == ElectricProtocolCapabilityPolicy.gateName
      )
      let state = try #require(
        try metadata.getSyncState(
          collectionId: collection.replica.identity.persistedCursorKey,
          transaction: nil
        )
      )
      #expect(state.offset == "snapshot-offset")
      #expect(state.isUpToDate)
    }
  }

  @Test
  func protocolBoundaryErrorQuarantinesWithoutCircuitBreakerRetry() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let telemetry = RecordingElectricCursorTelemetry()
    let transactionCounter = TransactionCounter()
    let http = ProtocolRetryHTTPClientProvider(steps: [
      .protocolError(false),
      .messages([
        .upToDate(offset: "must-not-fetch")
      ]),
    ])
    let client = ElectricSyncClientImpl(
      configuration: .init(metadataProvider: metadata, httpClient: http)
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter,
      logger: telemetry
    )
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      for await _ in collection.subscribe(
        session: session,
        circuitBreaker: ZeroDelayCircuitBreaker()
      ) {
        Issue.record("A quarantined protocol error must not publish")
      }
    }

    #expect(await http.requestCount() == 1)
    #expect(await transactionCounter.count() == 0)
    let quarantineLogs = telemetry.recordedLogs().filter {
      $0.message.contains("Electric protocol quarantined")
    }
    #expect(quarantineLogs.count == 1)
    #expect(quarantineLogs.first?.metadata["reason"] == "control")
    #expect(
      !telemetry.recordedLogs().contains {
        $0.message.contains("scheduled bounded full-bootstrap recovery")
      }
    )
  }

  @Test
  func schemaIncompatibilityQuarantinesNormalProductionBatchBeforePublication() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let telemetry = RecordingElectricCursorTelemetry()
    let transactionCounter = TransactionCounter()
    let rejectedRecord = TestRecord(id: "row-1", name: "Rejected")
    let http = ProtocolRetryHTTPClientProvider(steps: [
      .messages([
        .make(record: rejectedRecord, offset: "rejected-offset", key: rejectedRecord.id),
        .upToDate(offset: "rejected-offset"),
      ]),
      .messages([.upToDate(offset: "must-not-fetch")]),
    ])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        tracer: telemetry,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { false },
          schemaCompatibility: {
            .incompatible(
              detail: "schema 8 is unsupported",
              compatibilityMayChangeAfterFullBootstrap: false
            )
          }
        )
      )
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter,
      logger: telemetry
    )
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      for await _ in collection.subscribe(
        session: session,
        circuitBreaker: ZeroDelayCircuitBreaker()
      ) {
        Issue.record("A schema-incompatible batch must not publish")
      }
    }

    #expect(await http.requestCount() == 1)
    #expect(await transactionCounter.count() == 0)
    #expect(store.record(id: rejectedRecord.id) == nil)
    #expect(
      try metadata.getSyncState(
        collectionId: collection.replica.identity.persistedCursorKey,
        transaction: nil
      ) == nil
    )
    let quarantineLogs = telemetry.recordedLogs().filter {
      $0.message.contains("Electric protocol quarantined for collection")
    }
    #expect(quarantineLogs.count == 1)
    #expect(quarantineLogs.first?.metadata["reason"] == "schema")
    #expect(
      !telemetry.recordedLogs().contains {
        $0.message.contains("scheduled bounded full-bootstrap recovery")
      }
    )
  }

  @Test
  func versionIncompatibilityGetsOneBootstrapThenQuarantinesNormalProductionBatch() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let telemetry = RecordingElectricCursorTelemetry()
    let transactionCounter = TransactionCounter()
    let rejectedRecord = TestRecord(id: "row-1", name: "Rejected")
    let rejectedMessages = [
      ElectricMessage.make(
        record: rejectedRecord,
        offset: "rejected-offset",
        key: rejectedRecord.id
      ),
      ElectricMessage.upToDate(offset: "rejected-offset"),
    ]
    let http = ProtocolRetryHTTPClientProvider(steps: [
      .messages(rejectedMessages),
      .messages(rejectedMessages),
      .messages([.upToDate(offset: "must-not-fetch")]),
    ])
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        tracer: telemetry,
        protocolCapabilityPolicy: ElectricProtocolCapabilityPolicy(
          isTaggedShapeProtocolEnabled: { false },
          versionCompatibility: {
            .incompatible(
              detail: "server protocol 9 is unsupported",
              compatibilityMayChangeAfterFullBootstrap: true
            )
          }
        )
      )
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter,
      logger: telemetry
    )
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      for await _ in collection.subscribe(
        session: session,
        circuitBreaker: ZeroDelayCircuitBreaker()
      ) {
        Issue.record("A version-incompatible batch must not publish")
      }
    }

    let requests = await http.capturedRequests()
    #expect(requests.count == 2)
    #expect(requests[1].offset == "-1")
    #expect(requests[1].cursor == nil)
    #expect(!requests[1].live)
    #expect(await transactionCounter.count() == 0)
    #expect(store.record(id: rejectedRecord.id) == nil)
    #expect(
      try metadata.getSyncState(
        collectionId: collection.replica.identity.persistedCursorKey,
        transaction: nil
      ) == nil
    )
    #expect(
      telemetry.recordedLogs().filter {
        $0.message.contains("scheduled bounded full-bootstrap recovery")
      }.count == 1
    )
    let quarantineLogs = telemetry.recordedLogs().filter {
      $0.message.contains("Electric protocol quarantined for collection")
    }
    #expect(quarantineLogs.count == 1)
    #expect(quarantineLogs.first?.metadata["reason"] == "version")
  }

  @Test
  func compatibilityChangingErrorGetsOneFullBootstrapRecovery() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let telemetry = RecordingElectricCursorTelemetry()
    let record = TestRecord(id: "row-1", name: "Recovered")
    let http = ProtocolRetryHTTPClientProvider(steps: [
      .protocolError(true),
      .messages([
        .make(record: record, offset: "recovered-offset", key: record.id),
        .upToDate(offset: "recovered-offset"),
      ]),
    ])
    let client = ElectricSyncClientImpl(
      configuration: .init(metadataProvider: metadata, httpClient: http)
    )
    let collection = makeTestCollection(client: client, store: store, logger: telemetry)
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      let stream = collection.subscribe(
        session: session,
        circuitBreaker: ZeroDelayCircuitBreaker()
      )
      let consumer = Task {
        for await _ in stream {}
      }
      try await waitUntilAsync(timeout: 30) {
        await http.requestCount() == 3 && store.record(id: record.id) == record
      }
      consumer.cancel()
      await consumer.value
    }

    let requests = await http.capturedRequests()
    #expect(requests.count == 3)
    #expect(requests[1].offset == "-1")
    #expect(requests[1].cursor == nil)
    #expect(!requests[1].live)
    #expect(requests[2].offset == "recovered-offset")
    #expect(requests[2].live)
    #expect(requests.filter { $0.offset == "-1" }.count == 1)
    #expect(store.record(id: record.id) == record)
    #expect(
      telemetry.recordedLogs().filter {
        $0.message.contains("scheduled bounded full-bootstrap recovery")
      }.count == 1
    )
    #expect(
      !telemetry.recordedLogs().contains {
        $0.message.contains("Electric protocol quarantined for collection")
      }
    )
  }

  @Test
  func repeatedCompatibilityChangingErrorExhaustsOneRecoveryThenQuarantines() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let telemetry = RecordingElectricCursorTelemetry()
    let transactionCounter = TransactionCounter()
    let http = ProtocolRetryHTTPClientProvider(steps: [
      .protocolError(true), .protocolError(true),
      .messages([.upToDate(offset: "must-not-fetch")]),
    ])
    let client = ElectricSyncClientImpl(
      configuration: .init(metadataProvider: metadata, httpClient: http)
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter,
      logger: telemetry
    )
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      for await _ in collection.subscribe(
        session: session,
        circuitBreaker: ZeroDelayCircuitBreaker()
      ) {
        Issue.record("An incompatible recovery response must not publish")
      }
    }

    let requests = await http.capturedRequests()
    #expect(requests.count == 2)
    #expect(requests[1].offset == "-1")
    #expect(!requests[1].live)
    #expect(await transactionCounter.count() == 0)
    #expect(
      telemetry.recordedLogs().filter {
        $0.message.contains("scheduled bounded full-bootstrap recovery")
      }.count == 1
    )
    #expect(
      telemetry.recordedLogs().filter {
        $0.message.contains("Electric protocol quarantined for collection")
      }.count == 1
    )
  }

  @Test
  func newAuthGenerationCanRestartAfterProtocolQuarantine() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let record = TestRecord(id: "row-1", name: "New generation")
    let http = ProtocolRetryHTTPClientProvider(steps: [
      .protocolError(false),
      .messages([
        .make(record: record, offset: "new-generation-offset", key: record.id),
        .upToDate(offset: "new-generation-offset"),
      ]),
    ])
    let sessionController = TestSessionController()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        sessionProvider: sessionController.provider()
      )
    )
    let collection = makeTestCollection(client: client, store: store)
    let firstSnapshot = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      for await _ in collection.subscribe(
        session: firstSnapshot,
        circuitBreaker: ZeroDelayCircuitBreaker()
      ) {
        Issue.record("The quarantined generation must not publish")
      }

      await sessionController.beginTeardown()
      sessionController.finishUnauthenticated()
      let nextSnapshot = sessionController.activate()
      #expect(nextSnapshot.generation != firstSnapshot.generation)
      #expect(nextSnapshot.identifier != firstSnapshot.identifier)

      let stream = collection.subscribe(
        session: nextSnapshot,
        circuitBreaker: ZeroDelayCircuitBreaker()
      )
      let consumer = Task {
        for await _ in stream {}
      }
      try await waitUntilAsync(timeout: 30) {
        await http.requestCount() == 3 && store.record(id: record.id) == record
      }
      consumer.cancel()
      await consumer.value
    }

    let requests = await http.capturedRequests()
    #expect(requests.count == 3)
    #expect(requests[2].offset == "new-generation-offset")
    #expect(requests[2].live)
    #expect(store.record(id: record.id) == record)
  }

  @Test
  func ordinaryTransportErrorRetainsCircuitBreakerRetryBehavior() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let transactionCounter = TransactionCounter()
    let record = TestRecord(id: "row-1", name: "Retried")
    let http = ProtocolRetryHTTPClientProvider(steps: [
      .transientError,
      .messages([
        .make(record: record, offset: "retry-offset", key: record.id),
        .upToDate(offset: "retry-offset"),
      ]),
    ])
    let client = ElectricSyncClientImpl(
      configuration: .init(metadataProvider: metadata, httpClient: http)
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      transactionCounter: transactionCounter
    )
    let sessionController = TestSessionController()
    let session = try #require(
      sessionController.captureAuthenticatedSession()
    )

    do {
      let stream = collection.subscribe(
        session: session,
        circuitBreaker: ZeroDelayCircuitBreaker()
      )
      let consumer = Task {
        for await _ in stream {}
      }
      try await waitUntilAsync(timeout: 30) {
        await http.requestCount() == 3 && store.record(id: record.id) == record
      }
      consumer.cancel()
      await consumer.value
    }

    let requests = await http.capturedRequests()
    #expect(requests.count == 3)
    #expect(requests[2].offset == "retry-offset")
    #expect(requests[2].live)
    #expect(await transactionCounter.count() == 1)
    #expect(store.record(id: record.id) == record)
  }

  @Test
  func legacyBootstrapAdmissionIsDefaultOffAndPlumbedIntoClientConfiguration() async throws {
    let controller = ElectricLegacyBootstrapAdmissionController()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let configuration = ElectricSyncClientConfiguration(
      metadataProvider: InMemoryMetadataProvider(),
      httpClient: NoopHTTPClientProvider(),
      legacyBootstrapAdmissionController: controller
    )
    let client = ElectricSyncClientImpl(configuration: configuration)
    let clientController = await client.legacyBootstrapAdmissionController

    #expect(configuration.legacyBootstrapAdmissionController === controller)
    #expect(clientController === controller)
    await #expect(throws: ElectricSyncError.self) {
      _ = try await controller.acquire(identity: identity, legacyCursorCount: 1)
    }
    #expect(
      await controller.state()
        == ElectricLegacyBootstrapAdmissionState(isEnabled: false, queued: 0, inFlight: 0)
    )
    #expect(ElectricLegacyBootstrapRolloutPolicy.maxConcurrentPerProcess == 1)
    #expect(ElectricLegacyBootstrapRolloutPolicy.minimumCompletedForExpansion == 100)
    #expect(ElectricLegacyBootstrapRolloutPolicy.maximumFailureRateForExpansion == 0.01)
    #expect(ElectricLegacyBootstrapRolloutPolicy.minimumCompletedForRollback == 20)
    #expect(ElectricLegacyBootstrapRolloutPolicy.failureRateForRollback == 0.05)
    #expect(ElectricLegacyBootstrapRolloutPolicy.stableExitWindowDays == 14)
    #expect(ElectricLegacyBootstrapRolloutPolicy.rollbackExitRequiresZeroLegacyResumeDays == 14)
  }

  @Test
  func providerBoundariesAreStableAndPropagateThroughClientConfiguration() async throws {
    let session = ElectricSyncSession(generation: 42, identifier: "session-42")
    let teardownRecorder = SessionProviderTeardownRecorder()
    let sessionProvider = ElectricSyncSessionProvider(
      captureAuthenticatedSession: { session },
      isCurrent: { $0 == session },
      registerTeardownHandler: { handler in teardownRecorder.register(handler) },
      unregisterTeardownHandler: { id in teardownRecorder.unregister(id) }
    )
    let now = Date(timeIntervalSince1970: 1_700_000_000)
    let identifier = UUID(uuidString: "00000000-0000-0000-0000-000000000042")!
    let runtimeRecorder = RuntimeProviderRecorder()
    let runtimeProvider = ElectricSyncRuntimeProvider(
      now: { now },
      makeUUID: { identifier },
      sleep: { duration in await runtimeRecorder.recordSleep(duration) }
    )
    let logger = RecordingProviderBoundaryLogger()
    let configuration = ElectricSyncClientConfiguration(
      metadataProvider: InMemoryMetadataProvider(),
      httpClient: InMemoryHTTPClientProvider(responses: []),
      sessionProvider: sessionProvider,
      runtimeProvider: runtimeProvider,
      logger: logger
    )
    let client = ElectricSyncClientImpl(configuration: configuration)

    #expect(client.sessionProvider.captureAuthenticatedSession() == session)
    #expect(client.sessionProvider.isCurrent(session))
    #expect(client.runtimeProvider.now() == now)
    #expect(client.runtimeProvider.makeUUID() == identifier)
    try await client.runtimeProvider.sleep(for: .seconds(2))
    #expect(await runtimeRecorder.requestedDurations() == [.seconds(2)])

    let registrationID = client.sessionProvider.registerTeardownHandler {
      await runtimeRecorder.recordTeardown()
    }
    await teardownRecorder.invoke(id: registrationID)
    client.sessionProvider.unregisterTeardownHandler(registrationID)
    #expect(await runtimeRecorder.teardownCount() == 1)
    #expect(teardownRecorder.registeredIDs() == [])

    client.logger.log(.info, message: "provider boundary", metadata: ["source": "test"])
    #expect(logger.entries() == [.init(level: .info, message: "provider boundary")])
  }

  @Test
  func runtimeProviderProducesDistinctDeterministicIdentifiers() {
    let identifiers = IncrementingUUIDSource()
    let provider = ElectricSyncRuntimeProvider(
      now: Date.init,
      makeUUID: identifiers.next,
      sleep: { _ in }
    )

    #expect(provider.makeUUID() == UUID(uuidString: "00000000-0000-0000-0000-000000000001"))
    #expect(provider.makeUUID() == UUID(uuidString: "00000000-0000-0000-0000-000000000002"))
  }

  @Test
  func unmanagedSessionProviderUsesOneStableCurrentSession() {
    let provider = ElectricSyncSessionProvider.unmanaged
    let first = provider.captureAuthenticatedSession()
    let second = provider.captureAuthenticatedSession()

    #expect(first == second)
    #expect(first.map(provider.isCurrent) == true)
    #expect(!provider.isCurrent(.init(generation: 1, identifier: "managed")))
  }

  @Test
  func liveRuntimeProviderPreservesCancellation() async {
    let sleeper = Task {
      try await ElectricSyncRuntimeProvider.live.sleep(for: .seconds(60))
    }

    await Task.yield()
    sleeper.cancel()

    await #expect(throws: CancellationError.self) {
      try await sleeper.value
    }
  }

  @Test
  func legacyBootstrapAdmissionSerializesWorkIntoOneProcessSlot() async throws {
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let controller = ElectricLegacyBootstrapAdmissionController(enabled: true)
    let first = try await controller.acquire(identity: identity, legacyCursorCount: 1)
    let secondTask = Task {
      try await controller.acquire(identity: identity, legacyCursorCount: 2)
    }

    _ = await waitForLegacyBootstrapAdmissionState(controller) {
      $0.inFlight == 1 && $0.queued == 1
    }
    await controller.release(first)

    let second = try await secondTask.value
    #expect(second.wasQueued)
    #expect(second.legacyCursorCount == 2)
    #expect(await controller.state().inFlight == 1)
    await controller.release(second)
    #expect(await controller.state().inFlight == 0)
  }

  @Test(.timeLimit(.minutes(1)))
  func legacyExactMissBootstrapsThroughAdmittedLongPollFirst() async throws {
    let metadata = InMemoryMetadataProvider()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil,
      shapeDefinitionVersion: "2"
    )
    try metadata.updateSyncState(
      collectionId: try #require(identity.legacyPersistedCursorKeys.first),
      state: SyncState(
        offset: "legacy-offset",
        handle: "legacy-handle",
        cursor: "legacy-cursor",
        isUpToDate: true,
        lastSyncedAt: nil
      ),
      transaction: nil
    )
    let http = InMemoryHTTPClientProvider(responses: [[.upToDate(offset: "exact-offset")]])
    let controller = ElectricLegacyBootstrapAdmissionController(enabled: true)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        isExactCursorCutoverEnabled: true,
        legacyBootstrapAdmissionController: controller
      )
    )
    let batch = try #require(
      await client.withLegacyBootstrapAdmission(
        identity: identity,
        stage: "legacy_exact_miss_test",
        syncMode: .onDemand
      ) {
        try await client.pollStream(
          TestRecord.self,
          basePredicate: nil,
          shapeTopology: .dnf,
          syncMode: .onDemand,
          live: false,
          forceFullBootstrap: true,
          replicaIdentity: identity
        )
      }
    )
    _ = try batch.apply(in: nil)

    #expect(await http.requestCount() == 1)
    let request = try #require(await http.capturedRequests().first)
    #expect(request.live == false)
    #expect(request.offset == "-1")
    #expect(request.log == nil)
    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil)?.offset
        == "exact-offset"
    )
    #expect(await controller.metricsSnapshot().admitted == 1)
  }

  @Test
  func sseResetRequiresAdmissionBeforeApplyingReplacementBatch() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let resumableState = SyncState(
      offset: "exact-offset",
      handle: "exact-handle",
      cursor: nil,
      isUpToDate: true,
      lastSyncedAt: nil
    )
    for key in [
      identity.persistedCursorKey, try #require(identity.legacyPersistedCursorKeys.first),
    ] {
      try metadata.updateSyncState(collectionId: key, state: resumableState, transaction: nil)
    }
    let controller = ElectricLegacyBootstrapAdmissionController()
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: InMemoryHTTPClientProvider(responses: []),
        httpStreamClient: ScriptedHTTPStreamClientProvider(streams: [
          [
            .truncate(handle: "replacement-handle"),
            .make(
              record: TestRecord(id: "replacement", name: "Replacement"), offset: "replacement"),
            .upToDate(offset: "replacement"),
          ]
        ]),
        isExactCursorCutoverEnabled: true,
        legacyBootstrapAdmissionController: controller
      )
    )
    let collection = makeTestCollection(client: client, store: store, liveTransport: .sse)
    let consumer = Task {
      for await _ in collection.subscribe(circuitBreaker: ZeroDelayCircuitBreaker()) {}
    }
    try await waitUntilAsync(timeout: 30) {
      await controller.metricsSnapshot().rejected > 0
    }
    consumer.cancel()
    await consumer.value
    #expect(
      try metadata.getSyncState(collectionId: identity.persistedCursorKey, transaction: nil)?.offset
        == "-1")
  }

  @Test(.timeLimit(.minutes(1)))
  func sseHydrationReconnectsFromCommittedSubsetCursorBeforePublishingOverlap() async throws {
    let metadata = InMemoryMetadataProvider()
    let store = TestRecordStore()
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    try metadata.updateSyncState(
      collectionId: identity.persistedCursorKey,
      state: SyncState(
        offset: "A",
        handle: "handle-A",
        cursor: nil,
        isUpToDate: true,
        lastSyncedAt: nil
      ),
      transaction: nil
    )

    let partialPayload = Data(#"{"id":"row-1"}"#.utf8)
    let initialLiveBatch = [
      ElectricMessage(
        payload: partialPayload,
        key: "row-1",
        offset: "B",
        handle: "handle-B",
        kind: .mutation
      ),
      ElectricMessage.upToDate(offset: "B"),
    ]
    let staleQueuedBatch = [
      ElectricMessage.make(
        record: TestRecord(id: "row-1", name: "Stale B"),
        offset: "B",
        key: "row-1"
      ),
      ElectricMessage.upToDate(offset: "B"),
    ]
    let replacementLiveBatch = [
      ElectricMessage.make(
        record: TestRecord(id: "row-1", name: "Already visible in hydration"),
        offset: "C-overlap",
        key: "row-1",
        txids: [13]
      ),
      ElectricMessage.make(
        record: TestRecord(id: "row-1", name: "Fresh D"),
        offset: "D",
        key: "row-1",
        txids: [21]
      ),
      ElectricMessage.upToDate(offset: "D"),
    ]
    let streamHTTP = HydrationReconnectHTTPStreamClientProvider(
      initialBatch: initialLiveBatch,
      staleQueuedBatch: staleQueuedBatch,
      replacementBatch: replacementLiveBatch
    )
    let hydrationHTTP = HydrationReconnectHTTPClientProvider(
      streamHTTP: streamHTTP,
      response: [
        ElectricMessage.make(
          record: TestRecord(id: "row-1", name: "Hydrated C"),
          offset: "C",
          key: "row-1",
          isSubsetSnapshot: true
        ),
        ElectricMessage.snapshotEnd(offset: "C"),
        ElectricMessage.subsetEnd(offset: "C"),
      ]
    )
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: hydrationHTTP,
        httpStreamClient: streamHTTP,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true
      )
    )
    let collection = makeTestCollection(
      client: client,
      store: store,
      liveTransport: .sse
    )
    let sessionController = TestSessionController()
    let snapshot = try #require(sessionController.captureAuthenticatedSession())

    do {
      let token = collection.keepSynced(session: snapshot)
      defer {
        token.cancel()
        collection.replica.cancel()
      }

      try await waitUntilAsync(timeout: 30) {
        await streamHTTP.requestCount() >= 2
          && store.record(id: "row-1") == TestRecord(id: "row-1", name: "Fresh D")
          && (try? metadata.getSyncState(
            collectionId: identity.persistedCursorKey,
            transaction: nil
          ))?.offset == "D"
      }
      try await waitUntilAsync(timeout: 30) {
        await streamHTTP.firstStreamCancellationCount() == 1
      }

      let streamRequests = await streamHTTP.capturedRequests()
      #expect(streamRequests.count == 2)
      #expect(streamRequests[0].offset == "A")
      #expect(streamRequests[1].offset == "C")
      #expect(await streamHTTP.firstStreamCancellationCount() == 1)
      #expect(await streamHTTP.replacementOpenedAfterFirstCancellation())
      #expect(await streamHTTP.activeStreamCount() == 1)
      #expect(collection.replica.liveOwnerCount == 1)
      #expect(store.writeCallCount() == 2)
      #expect(store.record(id: "row-1") == TestRecord(id: "row-1", name: "Fresh D"))
      #expect(
        try metadata.getSyncState(
          collectionId: identity.persistedCursorKey,
          transaction: nil
        )?.offset == "D"
      )
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func queryAndTransientQueryShareAdmissionBeforePublicationThroughApply() async throws {
    let metadata = InMemoryMetadataProvider()
    let legacyKey = "\(TestRecord.tableName)|stream|mode:progressive|base:none"
    try metadata.updateSyncState(
      collectionId: legacyKey,
      state: SyncState(
        offset: "legacy-offset",
        handle: "legacy-handle",
        cursor: "legacy-cursor",
        isUpToDate: true,
        lastSyncedAt: nil
      ),
      transaction: nil
    )
    let response = [
      ElectricMessage.make(
        record: TestRecord(id: "overlap", name: "Overlap"),
        offset: "subset-offset",
        isSubsetSnapshot: true
      ),
      ElectricMessage.snapshotEnd(offset: "subset-offset"),
      ElectricMessage.subsetEnd(offset: "subset-offset"),
    ]
    let http = InMemoryHTTPClientProvider(responses: [response, response])
    let controller = ElectricLegacyBootstrapAdmissionController(enabled: true)
    let client = ElectricSyncClientImpl(
      configuration: .init(
        metadataProvider: metadata,
        httpClient: http,
        fetchTracker: ElectricFetchTracker(metadataProvider: metadata),
        isExactCursorCutoverEnabled: true,
        legacyBootstrapAdmissionController: controller
      )
    )
    let transactionGate = BlockingFirstTransactionGate()
    let collection = makeTestCollection(
      client: client,
      store: TestRecordStore(),
      transactionGate: transactionGate
    )
    let predicate = SQLExpression("id = 'overlap'")

    let queryTask = Task {
      try await collection.query(where: predicate, limit: 1)
    }
    await transactionGate.waitForFirstTransaction()
    let transientTask = Task {
      try await collection.ensureSubset(where: predicate, limit: 1)
    }

    _ = await waitForLegacyBootstrapAdmissionState(controller) { $0.queued == 1 }
    #expect(await controller.state().inFlight == 1)
    #expect(await http.requestCount() == 1)
    await transactionGate.releaseFirstTransaction()

    #expect(try await queryTask.value == [TestRecord(id: "overlap", name: "Overlap")])
    #expect(
      try await transientTask.value.appliedRecords
        == [TestRecord(id: "overlap", name: "Overlap")]
    )
    #expect(await http.requestCount() == 2)
    let admissionMetrics = await controller.metricsSnapshot()
    #expect(admissionMetrics.admitted == 2)
    #expect(admissionMetrics.nonAdvancingCompleted + admissionMetrics.superseded == 2)
  }

  @Test
  func legacyBootstrapKillSwitchRejectsQueueWhileActiveWorkDrains() async throws {
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let controller = ElectricLegacyBootstrapAdmissionController(enabled: true)
    let active = try await controller.acquire(identity: identity, legacyCursorCount: 1)
    let queuedTask = Task {
      try await controller.acquire(identity: identity, legacyCursorCount: 1)
    }
    _ = await waitForLegacyBootstrapAdmissionState(controller) { $0.queued == 1 }

    await controller.setEnabled(false)
    await #expect(throws: ElectricSyncError.self) {
      _ = try await queuedTask.value
    }
    #expect(
      await controller.state()
        == ElectricLegacyBootstrapAdmissionState(isEnabled: false, queued: 0, inFlight: 1)
    )

    await controller.release(active)
    #expect(await controller.state().inFlight == 0)
  }

  @Test
  func cancellingQueuedLegacyBootstrapRemovesItsWaiter() async throws {
    let identity = ElectricReplicaIdentity(
      modelType: TestRecord.self,
      modelIdentifier: TestRecord.collectionIdentifier,
      basePredicate: nil
    )
    let controller = ElectricLegacyBootstrapAdmissionController(enabled: true)
    let active = try await controller.acquire(identity: identity, legacyCursorCount: 1)
    let queuedTask = Task {
      try await controller.acquire(identity: identity, legacyCursorCount: 1)
    }
    _ = await waitForLegacyBootstrapAdmissionState(controller) { $0.queued == 1 }

    queuedTask.cancel()
    await #expect(throws: CancellationError.self) {
      _ = try await queuedTask.value
    }
    _ = await waitForLegacyBootstrapAdmissionState(controller) { $0.queued == 0 }
    #expect(await controller.state().inFlight == 1)

    await controller.release(active)
  }
}

// MARK: - Test Fixtures

private struct GRDBAtomicTestRecord:
  ElectricCollectionModel, Codable, FetchableRecord, PersistableRecord, Equatable
{
  let id: String
  let name: String

  static let tableName = "grdb_atomic_test_records"
  static let databaseTableName = tableName
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(
      endpoint: "/shapes/grdb-atomic-test-records",
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
  ) throws -> ProcessedMessage<GRDBAtomicTestRecord> {
    let context = try #require(transaction as? GRDBAtomicTransactionContext)
    let record = try JSONDecoder().decode(Self.self, from: message.payload)
    try context.gate.beforeWrite()
    try record.save(context.database)
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
    let context = try #require(transaction as? GRDBAtomicTransactionContext)
    _ = try deleteAll(context.database)
  }

  static func deleteByKey(_ key: String, transaction: Any?) throws {
    let context = try #require(transaction as? GRDBAtomicTransactionContext)
    _ = try deleteOne(context.database, key: key)
  }
}

private final class GRDBAtomicTransactionContext {
  let database: Database
  let gate: GRDBLateWriteGate

  init(database: Database, gate: GRDBLateWriteGate) {
    self.database = database
    self.gate = gate
  }
}

private final class GRDBLateWriteGate: @unchecked Sendable {
  private let condition = NSCondition()
  private let blockedWrite: Int
  private let failsAfterRelease: Bool
  private var writeCount = 0
  private var isBlocked = false
  private var isReleased = false

  init(blockedWrite: Int, failsAfterRelease: Bool) {
    self.blockedWrite = blockedWrite
    self.failsAfterRelease = failsAfterRelease
  }

  func beforeWrite() throws {
    condition.lock()
    writeCount += 1
    guard writeCount == blockedWrite else {
      condition.unlock()
      return
    }

    isBlocked = true
    condition.broadcast()
    while !isReleased {
      condition.wait()
    }
    let shouldFail = failsAfterRelease
    condition.unlock()

    if shouldFail {
      throw ElectricSyncError.fetchFailed("Injected late boundary write failure")
    }
  }

  func waitUntilBlocked() async throws {
    try await waitUntilAsync(timeout: 5) { [self] in blockedState() }
  }

  func release() {
    condition.lock()
    isReleased = true
    condition.broadcast()
    condition.unlock()
  }

  private func blockedState() -> Bool {
    condition.lock()
    defer { condition.unlock() }
    return isBlocked
  }
}

private actor GRDBObservationRecorder<Element: Sendable> {
  private var recordedValues: [Element] = []

  func append(_ value: Element) {
    recordedValues.append(value)
  }

  func count() -> Int {
    recordedValues.count
  }

  func values() -> [Element] {
    recordedValues
  }
}

private struct GRDBAtomicCacheProvider: DataCacheProvider {
  let database: DatabasePool

  func load<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> [T]
  where T: ElectricCollectionModel {
    guard T.tableName == GRDBAtomicTestRecord.tableName else { return [] }
    let records = try await database.read { db in
      try GRDBAtomicTestRecord.fetchAll(db)
    }
    return records as? [T] ?? []
  }

  func hasData<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> Bool
  where T: ElectricCollectionModel {
    guard T.tableName == GRDBAtomicTestRecord.tableName else { return false }
    return try await database.read { db in
      try GRDBAtomicTestRecord.fetchCount(db) > 0
    }
  }

  func clear<T>(_ type: T.Type) async throws where T: ElectricCollectionModel {
    guard T.tableName == GRDBAtomicTestRecord.tableName else { return }
    try await database.write { db in
      _ = try GRDBAtomicTestRecord.deleteAll(db)
    }
  }
}

private func waitForLegacyBootstrapAdmissionState(
  _ controller: ElectricLegacyBootstrapAdmissionController,
  condition: @escaping @Sendable (ElectricLegacyBootstrapAdmissionState) -> Bool
) async -> ElectricLegacyBootstrapAdmissionState {
  for _ in 0..<10_000 {
    let state = await controller.state()
    if condition(state) {
      return state
    }
    await Task.yield()
  }

  Issue.record("Timed out waiting for legacy bootstrap admission state")
  return await controller.state()
}

private struct TestRecord: ElectricCollectionModel, Codable, Equatable {
  let id: String
  let name: String

  static var tableName: String { "test_records" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(
      endpoint: "/shapes/test-records",
      selectedColumns: ["id", "name"],
      legacyCursorVersion: "1"
    )
  }

  static func hydrationQueryDescriptor(forMissingRowKeys keys: [String]) -> QueryDescriptor? {
    ElectricMissingRowHydration.primaryKeyComponentDescriptor(
      rowKeys: keys,
      field: "id"
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
    let decoder = JSONDecoder()
    if let record = try? decoder.decode(TestRecord.self, from: message.payload) {
      if record.name == "__delete__" {
        if let store = transaction as? TestRecordStore {
          store.delete(id: record.id)
        }
        return ProcessedMessage(
          records: [],
          metadata: StoreMetadata(
            offset: message.offset,
            handle: message.handle,
            cursor: message.cursor,
            operation: .delete
          ),
          optimisticPublishedRowEffects: [
            OptimisticPublishedRowEffect(rowId: record.id, operation: .delete)
          ]
        )
      }
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
        ),
        optimisticPublishedRowEffects: [
          OptimisticPublishedRowEffect(rowId: record.id, operation: .insert)
        ],
        confirmedLoroFrontiers: record.name == "Loro" ? [Data([7, 7])] : []
      )
    }

    struct PartialRecord: Decodable {
      let id: String
      let name: String?
    }

    let partial = try decoder.decode(PartialRecord.self, from: message.payload)
    if let store = transaction as? TestRecordStore,
      var existing = store.record(id: partial.id)
    {
      if let name = partial.name {
        existing = TestRecord(id: existing.id, name: name)
        store.upsert(existing)
      }
      return ProcessedMessage(
        records: [existing],
        metadata: StoreMetadata(
          offset: message.offset,
          handle: message.handle,
          cursor: message.cursor,
          operation: .update
        ),
        optimisticPublishedRowEffects: [
          OptimisticPublishedRowEffect(rowId: existing.id, operation: .update)
        ],
        confirmedLoroFrontiers: existing.name == "Loro" ? [Data([7, 7])] : []
      )
    }

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

  static func truncate(transaction: Any?) throws {
    if let store = transaction as? TestRecordStore {
      store.clear()
    }
  }

  static func deleteByKey(_ key: String, transaction: Any?) throws {
    (transaction as? TestRecordStore)?.delete(id: key)
  }
}

private func fetchMetadataHash(
  basePredicate: SQLExpression? = nil,
  predicate: SQLExpression?,
  orderBy: [OrderBy],
  limit: Int?,
  cursor: ElectricCursorExpressions? = nil
) -> PredicateHash {
  ElectricFetchTracker.metadataKey(
    predicate: ElectricFetchTracker.combinedCoveragePredicate(
      scope: basePredicate,
      requested: predicate
    ),
    orderBy: orderBy,
    limit: limit,
    cursor: cursor
  ).predicateHash
}

private final class TestRecordStore: @unchecked Sendable {
  private var records: [TestRecord] = []
  private var clears: Int = 0
  private var writes: Int = 0
  private let lock = NSLock()

  func upsert(_ record: TestRecord) {
    lock.lock()
    defer { lock.unlock() }
    writes += 1
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

  func record(id: String) -> TestRecord? {
    lock.lock()
    defer { lock.unlock() }
    return records.first(where: { $0.id == id })
  }

  func delete(id: String) {
    lock.lock()
    defer { lock.unlock() }
    writes += 1
    records.removeAll(where: { $0.id == id })
  }

  func clear() {
    lock.lock()
    defer { lock.unlock() }
    records = []
    clears += 1
  }

  func clearCallCount() -> Int {
    lock.lock()
    defer { lock.unlock() }
    return clears
  }

  func writeCallCount() -> Int {
    lock.lock()
    defer { lock.unlock() }
    return writes
  }
}

// Timeouts are wall-clock failure bounds, not expected outcomes: every call
// site treats expiry as a test failure. Successful waits resolve in well under
// one second; the generous budget only adds headroom for constrained CI runners,
// where parallel rebuilds have expired 2-second waits.
private func waitUntil(
  timeout: TimeInterval,
  pollInterval: TimeInterval = 0.01,
  condition: @escaping @Sendable () -> Bool
) async throws {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    if condition() {
      return
    }
    try await Task.sleep(nanoseconds: UInt64(pollInterval * 1_000_000_000))
  }

  Issue.record("Timed out waiting for condition")
}

private func waitUntilAsync(
  timeout: TimeInterval,
  pollInterval: TimeInterval = 0.01,
  condition: @escaping @Sendable () async -> Bool
) async throws {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    if await condition() {
      return
    }
    try await Task.sleep(nanoseconds: UInt64(pollInterval * 1_000_000_000))
  }

  throw AsyncConditionTimeoutError(timeout: timeout)
}

private struct AsyncConditionTimeoutError: Error, CustomStringConvertible {
  let timeout: TimeInterval

  var description: String {
    "Timed out waiting for async condition after \(timeout) seconds"
  }
}

private struct TaskTimeoutError: Error, CustomStringConvertible {
  let duration: Duration

  var description: String {
    "Timed out after \(duration)"
  }
}

private func awaitTaskResultBeforeTimeout<Output: Sendable>(
  _ task: Task<Output, Error>,
  timeout: Duration
) async throws -> Output {
  let stream = AsyncThrowingStream<Output, Error>.makeStream()
  let completionTask = Task {
    do {
      stream.continuation.yield(try await task.value)
      stream.continuation.finish()
    } catch {
      stream.continuation.finish(throwing: error)
    }
  }
  let timeoutTask = Task {
    do {
      try await Task.sleep(for: timeout)
      stream.continuation.finish(throwing: TaskTimeoutError(duration: timeout))
    } catch is CancellationError {
      return
    } catch {
      stream.continuation.finish(throwing: error)
    }
  }
  stream.continuation.onTermination = { _ in
    completionTask.cancel()
    timeoutTask.cancel()
  }

  var iterator = stream.stream.makeAsyncIterator()
  guard let result = try await iterator.next() else {
    throw TaskTimeoutError(duration: timeout)
  }
  return result
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

  func clear<T>(_ type: T.Type) async throws where T: ElectricCollectionModel {
    guard T.tableName == TestRecord.tableName else { return }
    store.clear()
  }
}

private actor BlockingDataCacheProvider: DataCacheProvider {
  private let store: TestRecordStore
  private var loadStarted = false
  private var cancellationObserved = false
  private var loadReleased = false
  private var loadWaiters: [CheckedContinuation<Void, Never>] = []

  init(store: TestRecordStore) {
    self.store = store
  }

  func load<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> [T]
  where T: ElectricCollectionModel {
    loadStarted = true
    for waiter in loadWaiters {
      waiter.resume()
    }
    loadWaiters.removeAll()

    while !loadReleased {
      do {
        try await Task.sleep(nanoseconds: 10_000_000)
      } catch {
        cancellationObserved = true
        await Task.yield()
      }
    }

    guard T.tableName == TestRecord.tableName else { return [] }
    return store.allRecords() as? [T] ?? []
  }

  func hasData<T>(_ type: T.Type, request _: QueryDescriptor) async throws -> Bool
  where T: ElectricCollectionModel {
    guard T.tableName == TestRecord.tableName else { return false }
    return !store.allRecords().isEmpty
  }

  func waitForLoadStart() async {
    guard !loadStarted else { return }
    await withCheckedContinuation { continuation in
      loadWaiters.append(continuation)
    }
  }

  func didObserveCancellation() -> Bool {
    cancellationObserved
  }

  func releaseLoad() {
    loadReleased = true
  }
}

private actor BlockingFirstTransactionGate {
  private var firstTransactionStarted = false
  private var firstTransactionReleased = false
  private var startWaiters: [CheckedContinuation<Void, Never>] = []
  private var releaseWaiter: CheckedContinuation<Void, Never>?

  func waitBeforeTransaction() async {
    guard !firstTransactionStarted else { return }
    firstTransactionStarted = true
    for waiter in startWaiters {
      waiter.resume()
    }
    startWaiters.removeAll()
    guard !firstTransactionReleased else { return }
    await withCheckedContinuation { continuation in
      releaseWaiter = continuation
    }
  }

  func waitForFirstTransaction() async {
    guard !firstTransactionStarted else { return }
    await withCheckedContinuation { continuation in
      startWaiters.append(continuation)
    }
  }

  func releaseFirstTransaction() {
    firstTransactionReleased = true
    releaseWaiter?.resume()
    releaseWaiter = nil
  }
}

private final class SessionProviderTeardownRecorder: @unchecked Sendable {
  private let lock = NSLock()
  private var handlers: [UUID: @Sendable () async -> Void] = [:]

  func register(_ handler: @escaping @Sendable () async -> Void) -> UUID {
    lock.withLock {
      let id = UUID()
      handlers[id] = handler
      return id
    }
  }

  func unregister(_ id: UUID) {
    let _: Void = lock.withLock {
      handlers.removeValue(forKey: id)
    }
  }

  func invoke(id: UUID) async {
    let handler = lock.withLock { handlers[id] }
    await handler?()
  }

  func registeredIDs() -> [UUID] {
    lock.withLock { Array(handlers.keys) }
  }
}

private actor RuntimeProviderRecorder {
  private var durations: [Duration] = []
  private var teardowns = 0

  func recordSleep(_ duration: Duration) {
    durations.append(duration)
  }

  func requestedDurations() -> [Duration] {
    durations
  }

  func recordTeardown() {
    teardowns += 1
  }

  func teardownCount() -> Int {
    teardowns
  }
}

private final class RecordingProviderBoundaryLogger: LogProvider, @unchecked Sendable {
  struct Entry: Equatable {
    let level: LogLevel
    let message: String
  }

  private let lock = NSLock()
  private var recorded: [Entry] = []

  func log(_ level: LogLevel, message: String, metadata _: [String: String]?) {
    let _: Void = lock.withLock {
      recorded.append(.init(level: level, message: message))
    }
  }

  func entries() -> [Entry] {
    lock.withLock { recorded }
  }
}

private final class InMemoryMetadataProvider: MetadataProvider, @unchecked Sendable {
  // Defaults to durable row ownership; see ReplicaInMemoryMetadataProvider for
  // why the protocol default (false) makes these tests scheduling-sensitive.
  // Tests pinning the non-durable clear/refetch contract pass false.
  let supportsDurableRowOwnership: Bool

  init(supportsDurableRowOwnership: Bool = true) {
    self.supportsDurableRowOwnership = supportsDurableRowOwnership
  }

  private var fetched: [String: [PredicateHash: FetchedPredicate]] = [:]
  private var observations: [String: [PredicateHash: SubsetObservation]] = [:]
  private var ranges: [String: [String: [FetchedRange]]] = [:]
  private var syncStates: [String: SyncState] = [:]
  private var syncStateHistoryByCollection: [String: [SyncState]] = [:]
  private var ownedRowKeysByTableAndShape: [String: [String: Set<String>]] = [:]
  private var clearedTables: [String] = []
  private var optimisticRetirements: [OptimisticRetirementEvidence] = []
  private let lock = NSLock()

  func hasFetched(table: String, predicate: PredicateHash, transaction _: Any?) throws -> Bool {
    lock.lock()
    defer { lock.unlock() }
    return fetched[table]?[predicate]?.isComplete == true
  }

  func getFetchedPredicates(table: String, transaction _: Any?) throws -> [FetchedPredicate] {
    lock.lock()
    defer { lock.unlock() }
    guard let values = fetched[table]?.values else { return [] }
    return Array(values)
  }

  func recordFetch(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome,
    isComplete: Bool,
    transaction _: Any?
  ) throws {
    lock.lock()
    defer { lock.unlock() }
    let record = FetchedPredicate(
      predicateHash: predicate,
      predicateJSON: predicateJSON,
      snapshotBoundary: snapshotBoundary,
      outcome: outcome,
      isComplete: isComplete,
      fetchedAt: Date()
    )
    var tablePredicates = fetched[table] ?? [:]
    tablePredicates[predicate] = record
    fetched[table] = tablePredicates
  }

  func getLatestObservation(
    table: String,
    predicate: PredicateHash,
    transaction _: Any?
  ) throws -> SubsetObservation? {
    lock.lock()
    defer { lock.unlock() }
    return observations[table]?[predicate]
  }

  func recordObservation(
    table: String,
    predicate: PredicateHash,
    predicateJSON: String?,
    snapshotBoundary: PostgresSnapshot?,
    outcome: SubsetObservationOutcome,
    transaction _: Any?
  ) throws {
    lock.lock()
    defer { lock.unlock() }
    let observation = SubsetObservation(
      predicateHash: predicate,
      predicateJSON: predicateJSON,
      snapshotBoundary: snapshotBoundary,
      outcome: outcome,
      observedAt: Date()
    )
    var tableObservations = observations[table] ?? [:]
    tableObservations[predicate] = observation
    observations[table] = tableObservations
  }

  func getFetchedRanges(table: String, orderField: String, transaction _: Any?) throws
    -> [FetchedRange]
  {
    lock.lock()
    defer { lock.unlock() }
    return ranges[table]?[orderField] ?? []
  }

  func recordRange(
    table: String,
    orderField: String,
    range: FetchedRange,
    transaction _: Any?
  ) throws {
    lock.lock()
    defer { lock.unlock() }
    var tableRanges = ranges[table] ?? [:]
    var fieldRanges = tableRanges[orderField] ?? []
    fieldRanges.append(range)
    tableRanges[orderField] = fieldRanges
    ranges[table] = tableRanges
  }

  func clearMetadata(table: String, transaction _: Any?) throws {
    lock.lock()
    defer { lock.unlock() }
    fetched[table] = nil
    ranges[table] = nil
    clearedTables.append(table)
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
    syncStateHistoryByCollection[collectionId, default: []].append(state)
  }

  func resetSyncState(collectionId: String, transaction _: Any?) throws {
    lock.lock()
    defer { lock.unlock() }
    let state = SyncState(
      offset: nil, handle: nil, cursor: nil, isUpToDate: false, lastSyncedAt: nil)
    syncStates[collectionId] = state
    syncStateHistoryByCollection[collectionId, default: []].append(state)
  }

  func syncStateHistory(collectionId: String) -> [SyncState] {
    lock.lock()
    defer { lock.unlock() }
    return syncStateHistoryByCollection[collectionId] ?? []
  }

  func releaseAllRowOwnership(
    table: String,
    shapeIdentity: String,
    transaction _: Any?
  ) throws -> [String] {
    lock.lock()
    defer { lock.unlock() }
    let rowKeys = ownedRowKeysByTableAndShape[table]?[shapeIdentity] ?? []
    ownedRowKeysByTableAndShape[table]?[shapeIdentity] = nil
    return rowKeys.sorted()
  }

  func seedRowOwnership(table: String, rowKey: String, shapeIdentity: String) {
    lock.lock()
    defer { lock.unlock() }
    ownedRowKeysByTableAndShape[table, default: [:]][shapeIdentity, default: []]
      .insert(rowKey)
  }

  func retireOptimisticMutations(
    table: String,
    publications: Set<OptimisticPublicationEvidence>,
    snapshotBoundary _: PostgresSnapshot?,
    transaction _: Any?
  ) throws {
    lock.lock()
    defer { lock.unlock() }
    optimisticRetirements.append(
      OptimisticRetirementEvidence(
        table: table,
        publications: publications
      )
    )
  }

  func recordedPredicate(table: String, predicate: PredicateHash) -> FetchedPredicate? {
    lock.lock()
    defer { lock.unlock() }
    return fetched[table]?[predicate]
  }

  func recordedObservation(table: String, predicate: PredicateHash) -> SubsetObservation? {
    lock.lock()
    defer { lock.unlock() }
    return observations[table]?[predicate]
  }

  func seedFetchedPredicate(table: String, predicate: PredicateHash, isComplete: Bool = true) {
    lock.lock()
    defer { lock.unlock() }
    let record = FetchedPredicate(
      predicateHash: predicate,
      predicateJSON: predicate.value,
      snapshotBoundary: nil,
      outcome: .present,
      isComplete: isComplete,
      fetchedAt: Date()
    )
    var tablePredicates = fetched[table] ?? [:]
    tablePredicates[predicate] = record
    fetched[table] = tablePredicates
  }

  func clearedMetadataTables() -> [String] {
    lock.lock()
    defer { lock.unlock() }
    return clearedTables
  }

  func optimisticRetirementEvidence() -> [OptimisticRetirementEvidence] {
    lock.lock()
    defer { lock.unlock() }
    return optimisticRetirements
  }
}

private struct OptimisticRetirementEvidence: Equatable, Sendable {
  let table: String
  let publications: Set<OptimisticPublicationEvidence>
}

private actor InMemoryHTTPClientProvider: HTTPClientProvider {
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

  func capturedRequests() -> [ElectricShapeRequest] {
    requests
  }
}

private actor HydrationReconnectHTTPClientProvider: HTTPClientProvider {
  private let streamHTTP: HydrationReconnectHTTPStreamClientProvider
  private let response: [ElectricMessage]
  private var requests: [ElectricShapeRequest] = []

  init(
    streamHTTP: HydrationReconnectHTTPStreamClientProvider,
    response: [ElectricMessage]
  ) {
    self.streamHTTP = streamHTTP
    self.response = response
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    requests.append(request)
    await streamHTTP.queueStaleBatch()
    return response
  }
}

private actor HydrationReconnectHTTPStreamClientProvider: HTTPStreamClientProvider {
  private let initialBatch: [ElectricMessage]
  private let staleQueuedBatch: [ElectricMessage]
  private let replacementBatch: [ElectricMessage]
  private var requests: [ElectricShapeRequest] = []
  private var continuations: [AsyncThrowingStream<ElectricMessage, Error>.Continuation] = []
  private var terminatedStreamIndexes: Set<Int> = []
  private var didOpenReplacementAfterFirstCancellation = false

  init(
    initialBatch: [ElectricMessage],
    staleQueuedBatch: [ElectricMessage],
    replacementBatch: [ElectricMessage]
  ) {
    self.initialBatch = initialBatch
    self.staleQueuedBatch = staleQueuedBatch
    self.replacementBatch = replacementBatch
  }

  func stream(_ request: ElectricShapeRequest) async throws -> AsyncThrowingStream<
    ElectricMessage, Error
  > {
    requests.append(request)
    let streamIndex = requests.count - 1
    if streamIndex == 1 {
      didOpenReplacementAfterFirstCancellation = terminatedStreamIndexes.contains(0)
    }
    let pair = AsyncThrowingStream<ElectricMessage, Error>.makeStream()
    pair.continuation.onTermination = { [weak self] _ in
      Task {
        await self?.recordTermination(streamIndex: streamIndex)
      }
    }
    continuations.append(pair.continuation)

    let initialMessages = streamIndex == 0 ? initialBatch : replacementBatch
    for message in initialMessages {
      pair.continuation.yield(message)
    }
    return pair.stream
  }

  func queueStaleBatch() {
    guard let firstContinuation = continuations.first else { return }
    for message in staleQueuedBatch {
      firstContinuation.yield(message)
    }
  }

  private func recordTermination(streamIndex: Int) {
    terminatedStreamIndexes.insert(streamIndex)
  }

  func requestCount() -> Int {
    requests.count
  }

  func capturedRequests() -> [ElectricShapeRequest] {
    requests
  }

  func firstStreamCancellationCount() -> Int {
    terminatedStreamIndexes.contains(0) ? 1 : 0
  }

  func replacementOpenedAfterFirstCancellation() -> Bool {
    didOpenReplacementAfterFirstCancellation
  }

  func activeStreamCount() -> Int {
    requests.count - terminatedStreamIndexes.count
  }
}

private actor ProtocolRetryHTTPClientProvider: HTTPClientProvider {
  enum Step: Sendable {
    case messages([ElectricMessage])
    case protocolError(Bool)
    case transientError
  }

  private var steps: [Step]
  private var requests: [ElectricShapeRequest] = []

  init(steps: [Step]) {
    self.steps = steps
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    requests.append(request)
    guard !steps.isEmpty else {
      while true {
        try await Task.sleep(nanoseconds: 10_000_000)
      }
    }
    switch steps.removeFirst() {
    case .messages(let messages):
      return messages
    case .protocolError(let compatibilityMayChangeAfterFullBootstrap):
      throw TestProtocolBoundaryError(
        compatibilityMayChangeAfterFullBootstrap: compatibilityMayChangeAfterFullBootstrap
      )
    case .transientError:
      throw TestTransientTransportError()
    }
  }

  func requestCount() -> Int {
    requests.count
  }

  func capturedRequests() -> [ElectricShapeRequest] {
    requests
  }
}

private struct TestProtocolBoundaryError: ElectricProtocolIncompatibilityError {
  let electricProtocolQuarantine: ElectricProtocolQuarantine

  init(compatibilityMayChangeAfterFullBootstrap: Bool) {
    electricProtocolQuarantine = ElectricProtocolQuarantine(
      reason: .control,
      detail: "unsupported test protocol control",
      compatibilityMayChangeAfterFullBootstrap: compatibilityMayChangeAfterFullBootstrap
    )
  }
}

private struct TestTransientTransportError: Error {}

private actor SnapshotParityHTTPClientProvider: HTTPClientProvider {
  private let snapshotMessages: [ElectricMessage]
  private var liveMessageResponses: [[ElectricMessage]]
  private var requests: [ElectricShapeRequest] = []
  private var liveRequests = 0
  private var deliveredLiveMessageResponses = 0
  private var cancelledLiveRequests = 0
  private var subsetStartedAfterCancellation = false
  private var shouldAbortBlockedLiveRequest = false

  init(
    snapshotMessages: [ElectricMessage],
    overlappingLiveMessages: [ElectricMessage]
  ) {
    self.snapshotMessages = snapshotMessages
    self.liveMessageResponses = [overlappingLiveMessages]
  }

  init(
    snapshotMessages: [ElectricMessage],
    liveMessageResponses: [[ElectricMessage]]
  ) {
    self.snapshotMessages = snapshotMessages
    self.liveMessageResponses = liveMessageResponses
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    requests.append(request)
    if request.subset != nil {
      subsetStartedAfterCancellation = cancelledLiveRequests > 0
      return snapshotMessages
    }
    if !request.live {
      let offset =
        request.offset == "now" || request.offset == "-1"
        ? "owner-offset"
        : request.offset ?? "owner-offset"
      return [.upToDate(offset: offset)]
    }

    liveRequests += 1
    if liveRequests > 1, !liveMessageResponses.isEmpty {
      deliveredLiveMessageResponses += 1
      return liveMessageResponses.removeFirst()
    }

    do {
      while !shouldAbortBlockedLiveRequest {
        try await Task.sleep(nanoseconds: 10_000_000)
      }
    } catch {
      cancelledLiveRequests += 1
      throw error
    }
    throw TaskTimeoutError(duration: .zero)
  }

  func requestCount() -> Int {
    requests.count
  }

  func capturedRequests() -> [ElectricShapeRequest] {
    requests
  }

  func cancelledLiveRequestCount() -> Int {
    cancelledLiveRequests
  }

  func deliveredLiveMessageResponseCount() -> Int {
    deliveredLiveMessageResponses
  }

  func subsetStartedAfterLiveCancellation() -> Bool {
    subsetStartedAfterCancellation
  }

  func abortBlockedLiveRequest() {
    shouldAbortBlockedLiveRequest = true
  }
}

private actor CancellationInsensitiveLiveHTTPClientProvider: HTTPClientProvider {
  private let snapshotMessages: [ElectricMessage]
  private let staleLiveMessages: [ElectricMessage]
  private var liveRequestCount = 0
  private var didStartFirstLiveRequest = false
  private var didReturnStaleLiveBatch = false
  private var didStartSubsetAfterStaleLiveReturn = false
  private var shouldAbortBlockedLiveRequest = false

  init(snapshotMessages: [ElectricMessage], staleLiveMessages: [ElectricMessage]) {
    self.snapshotMessages = snapshotMessages
    self.staleLiveMessages = staleLiveMessages
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    if request.subset != nil {
      didStartSubsetAfterStaleLiveReturn = didReturnStaleLiveBatch
      return snapshotMessages
    }
    guard request.live else {
      let offset = request.offset == "-1" ? "owner-offset" : request.offset ?? "owner-offset"
      return [.upToDate(offset: offset)]
    }

    liveRequestCount += 1
    if liveRequestCount == 1 {
      didStartFirstLiveRequest = true
      do {
        while !shouldAbortBlockedLiveRequest {
          try await Task.sleep(nanoseconds: 10_000_000)
        }
      } catch {
        // Deliberately emulate a transport that returns a stale page despite
        // cancellation. The caller's post-fetch fence must reject it.
        didReturnStaleLiveBatch = true
        return staleLiveMessages
      }
      throw TaskTimeoutError(duration: .zero)
    }

    while !shouldAbortBlockedLiveRequest {
      try await Task.sleep(nanoseconds: 10_000_000)
    }
    throw TaskTimeoutError(duration: .zero)
  }

  func firstLiveRequestStarted() -> Bool {
    didStartFirstLiveRequest
  }

  func firstLiveReturnedAfterCancellation() -> Bool {
    didReturnStaleLiveBatch
  }

  func subsetStartedAfterStaleLiveReturn() -> Bool {
    didStartSubsetAfterStaleLiveReturn
  }

  func abortBlockedLiveRequest() {
    shouldAbortBlockedLiveRequest = true
  }
}

private final class ThreadSafeCounter: @unchecked Sendable {
  private let lock = NSLock()
  private var storedValue = 0

  var value: Int {
    lock.withLock { storedValue }
  }

  func increment() {
    lock.withLock {
      storedValue += 1
    }
  }
}

private final class IncrementingUUIDSource: @unchecked Sendable {
  private let lock = NSLock()
  private var nextValue = 1

  func next() -> UUID {
    lock.withLock {
      defer { nextValue += 1 }
      return UUID(
        uuidString: String(format: "00000000-0000-0000-0000-%012d", nextValue)
      )!
    }
  }
}

private final class RuntimeBoolean: @unchecked Sendable {
  private let lock = NSLock()
  private var storedValue: Bool

  init(_ value: Bool) {
    self.storedValue = value
  }

  var value: Bool {
    lock.withLock { storedValue }
  }

  func set(_ value: Bool) {
    lock.withLock {
      storedValue = value
    }
  }
}

private actor AsyncTestLatch {
  private var isOpen = false
  private var waiters: [CheckedContinuation<Void, Never>] = []

  func wait() async {
    guard !isOpen else { return }
    await withCheckedContinuation { continuation in
      waiters.append(continuation)
    }
  }

  func open() {
    guard !isOpen else { return }
    isOpen = true
    let pendingWaiters = waiters
    waiters.removeAll(keepingCapacity: false)
    for waiter in pendingWaiters {
      waiter.resume()
    }
  }
}

private actor ReplicaPublicationRecorder {
  struct State: Sendable {
    let events: [String]
    let activeCount: Int
    let maximumActiveCount: Int
  }

  private var recordedEvents: [String] = []
  private var activeCount = 0
  private var maximumActiveCount = 0

  func append(_ event: String) {
    recordedEvents.append(event)
  }

  func begin(_ operation: String) {
    activeCount += 1
    maximumActiveCount = max(maximumActiveCount, activeCount)
    recordedEvents.append("\(operation).start")
  }

  func end(_ operation: String) {
    recordedEvents.append("\(operation).end")
    activeCount -= 1
  }

  func events() -> [String] {
    recordedEvents
  }

  func state() -> State {
    State(
      events: recordedEvents,
      activeCount: activeCount,
      maximumActiveCount: maximumActiveCount
    )
  }
}

private func waitForPublicationWaiter(_ replica: ElectricShapeReplica<TestRecord>) async throws {
  try await waitUntilAsync(timeout: 30) {
    await replica.publicationWaiterCount > 0
  }
}

private actor BlockingHTTPClientProvider: HTTPClientProvider {
  private let response: [ElectricMessage]
  private var requestStarted = false
  private var cancellationObserved = false
  private var fetchReleased = false
  private var requests = 0
  private var firstRequest: ElectricShapeRequest?
  private var waitContinuation: CheckedContinuation<Void, Never>?

  init(response: [ElectricMessage]) {
    self.response = response
  }

  func fetch(_ request: ElectricShapeRequest) async throws -> [ElectricMessage] {
    requests += 1
    firstRequest = firstRequest ?? request
    requestStarted = true
    waitContinuation?.resume()
    waitContinuation = nil

    while !fetchReleased {
      do {
        try await Task.sleep(nanoseconds: 10_000_000)
      } catch {
        cancellationObserved = true
      }
    }

    return response
  }

  func waitForFetchStart() async {
    if requestStarted {
      return
    }

    await withCheckedContinuation { continuation in
      waitContinuation = continuation
    }
  }

  func releaseFetch() {
    fetchReleased = true
  }

  func didObserveCancellation() -> Bool {
    cancellationObserved
  }

  func fetchCount() -> Int {
    requests
  }

  func capturedRequest() -> ElectricShapeRequest? {
    firstRequest
  }
}

private actor AsyncCompletionFlag {
  private var completed = false

  func markComplete() {
    completed = true
  }

  func isComplete() -> Bool {
    completed
  }
}

private actor CountingEventHandler: ElectricSyncEventHandler {
  private var willCount: Int = 0
  private var didCount: Int = 0

  func willReceiveTruncate(table _: String, predicate _: SQLExpression?) async {
    willCount += 1
  }

  func didReceiveTruncate(table _: String, predicate _: SQLExpression?) async {
    didCount += 1
  }

  func willTruncateCount() -> Int { willCount }
  func didTruncateCount() -> Int { didCount }
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

extension ElectricMessage {
  fileprivate static func grdbAtomicRecord(
    _ record: GRDBAtomicTestRecord,
    offset: String
  ) -> ElectricMessage {
    ElectricMessage(
      payload: try! JSONEncoder().encode(record),
      key: record.id,
      offset: offset,
      handle: "handle-\(offset)",
      isUpToDate: false,
      kind: .snapshot,
      txids: [42],
      isSubsetSnapshot: true
    )
  }

  fileprivate static func make(
    record: TestRecord,
    offset: String,
    cursor: String? = nil,
    key: String? = nil,
    tags: [String]? = nil,
    txids: [Int64]? = nil,
    isSubsetSnapshot: Bool = false
  ) -> ElectricMessage {
    let payload = try! JSONEncoder().encode(record)
    return ElectricMessage(
      payload: payload,
      key: key,
      offset: offset,
      handle: "handle-\(offset)",
      cursor: cursor,
      isUpToDate: false,
      kind: .snapshot,
      txids: txids,
      tags: tags,
      isSubsetSnapshot: isSubsetSnapshot
    )
  }

  fileprivate static func upToDate(offset: String, cursor: String? = nil) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: cursor,
      isUpToDate: true,
      kind: .snapshot,
      control: .upToDate
    )
  }

  fileprivate static func snapshotEnd(offset: String) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: false,
      kind: .snapshot,
      control: .snapshotEnd,
      postgresSnapshot: PostgresSnapshot(
        xmin: "10",
        xmax: "20",
        xipList: ["11", "12"]
      ),
      isSubsetSnapshot: true
    )
  }

  static func subsetEnd(offset: String) -> ElectricMessage {
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

  fileprivate static func truncate(handle: String?) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: "-1",
      handle: handle,
      cursor: nil,
      isUpToDate: false,
      kind: .truncate
    )
  }

  fileprivate static func mustRefetch(offset: String) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      offset: offset,
      handle: "handle-\(offset)",
      cursor: nil,
      isUpToDate: false,
      kind: .snapshot,
      control: .mustRefetch
    )
  }
}

private func makeTestCollection(
  client: ElectricSyncClientImpl,
  store: TestRecordStore,
  cacheProvider: (any DataCacheProvider)? = nil,
  eventHandler: (any ElectricSyncEventHandler)? = nil,
  transactionCounter: TransactionCounter? = nil,
  transactionGate: BlockingFirstTransactionGate? = nil,
  logger: any LogProvider = NoopLogProvider(),
  syncMode: ElectricCollectionSyncMode = .onDemand,
  liveTransport: ElectricLiveTransport = .longPoll,
  basePredicate: SQLExpression? = nil,
  shapeTopology: ElectricShapeTopology = .staticallySimple
) -> ElectricCollection<TestRecord> {
  let resolvedCacheProvider = cacheProvider ?? StoreBackedCacheProvider(store: store)
  let transactionRunner:
    @Sendable (@escaping @Sendable (Any?) throws -> Void) async throws -> Void = { operation in
      if let transactionCounter {
        await transactionCounter.increment()
      }
      await transactionGate?.waitBeforeTransaction()
      try operation(store)
    }

  let configuration = ElectricCollectionConfiguration(
    modelType: TestRecord.self,
    syncMode: syncMode,
    live: false,
    liveTransport: liveTransport,
    basePredicate: basePredicate,
    // TestRecord fixtures model subquery-free streams. DNF-specific tests
    // opt in explicitly so an unannotated fixture cannot accidentally opt
    // into a process-local tracker contract.
    shapeTopology: shapeTopology
  )

  return ElectricCollection(
    configuration: configuration,
    client: client,
    cacheProvider: resolvedCacheProvider,
    transactionRunner: transactionRunner,
    eventHandler: eventHandler ?? NoopElectricSyncEventHandler(),
    backgroundTaskProvider: NoopBackgroundTaskProvider(),
    logger: logger
  )
}

private func legacyStreamStateKey<T: ElectricCollectionModel>(
  for type: T.Type,
  basePredicate: SQLExpression?,
  syncMode: ElectricCollectionSyncMode
) -> String {
  ElectricReplicaIdentity(
    modelType: type,
    modelIdentifier: type.collectionIdentifier,
    basePredicate: basePredicate
  ).legacyPersistedCursorKey(syncMode: syncMode)
}
