import Foundation
import Testing

@testable import ElectricSync

struct ElectricFetchTrackerTests {
  @Test
  func limitedRequestSkipsWhenFullPredicateFetched() async throws {
    let metadata = TestMetadataProvider()
    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let predicate = SQLExpression("status = 'open'")
    let unscopedKey = ElectricFetchTracker.metadataKey(
      predicate: predicate,
      orderBy: [],
      limit: nil
    )

    try metadata.recordFetch(
      table: "records",
      predicate: unscopedKey.predicateHash,
      predicateJSON: unscopedKey.predicateJSON,
      snapshotBoundary: nil,
      outcome: .present,
      isComplete: true,
      transaction: nil
    )

    let plan = try await tracker.computeMissing(
      table: "records",
      requested: predicate,
      scope: nil,
      orderBy: [OrderBy(field: "created_at")],
      limit: 10
    )

    #expect(!plan.needsFetch)
    #expect(plan.reuseExisting)
  }

  @Test
  func limitedRequestSkipsWhenScopedPredicateFetched() async throws {
    let metadata = TestMetadataProvider()
    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let predicate = SQLExpression("status = 'open'")
    let scopedKey = ElectricFetchTracker.metadataKey(
      predicate: predicate,
      orderBy: [OrderBy(field: "created_at")],
      limit: 10
    )

    try metadata.recordFetch(
      table: "records",
      predicate: scopedKey.predicateHash,
      predicateJSON: scopedKey.predicateJSON,
      snapshotBoundary: nil,
      outcome: .present,
      isComplete: true,
      transaction: nil
    )

    let plan = try await tracker.computeMissing(
      table: "records",
      requested: predicate,
      scope: nil,
      orderBy: [OrderBy(field: "created_at")],
      limit: 10
    )

    #expect(!plan.needsFetch)
    #expect(plan.reuseExisting)
  }

  @Test
  func limitedFetchDoesNotSatisfyUnboundedRequest() async throws {
    let metadata = TestMetadataProvider()
    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let predicate = SQLExpression("status = 'open'")
    let scopedKey = ElectricFetchTracker.metadataKey(
      predicate: predicate,
      orderBy: [OrderBy(field: "created_at")],
      limit: 10
    )

    try metadata.recordFetch(
      table: "records",
      predicate: scopedKey.predicateHash,
      predicateJSON: scopedKey.predicateJSON,
      snapshotBoundary: nil,
      outcome: .present,
      isComplete: true,
      transaction: nil
    )

    let plan = try await tracker.computeMissing(
      table: "records",
      requested: predicate,
      scope: nil,
      orderBy: [],
      limit: nil
    )

    #expect(plan.needsFetch)
    #expect(plan.predicate?.rawValue == predicate.rawValue)
  }

  @Test
  func scopedRequestSkipsWhenEffectivePredicateFetched() async throws {
    let metadata = TestMetadataProvider()
    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let scope = SQLExpression(
      predicate: .comparison(field: "isRecurring", op: .equal, value: .bool(true))
    )
    let effectivePredicate = ElectricFetchTracker.combinedCoveragePredicate(
      scope: scope,
      requested: nil
    )
    let metadataKey = ElectricFetchTracker.metadataKey(
      predicate: effectivePredicate,
      orderBy: [],
      limit: nil
    )

    try metadata.recordFetch(
      table: "records",
      predicate: metadataKey.predicateHash,
      predicateJSON: metadataKey.predicateJSON,
      snapshotBoundary: nil,
      outcome: .present,
      isComplete: true,
      transaction: nil
    )

    let plan = try await tracker.computeMissing(
      table: "records",
      requested: nil,
      scope: scope,
      orderBy: [],
      limit: nil
    )

    #expect(!plan.needsFetch)
    #expect(plan.reuseExisting)
  }

  @Test
  func completedEmptyWindowCoverageSkipsARepeatFetch() async throws {
    let metadata = TestMetadataProvider()
    let tracker = ElectricFetchTracker(metadataProvider: metadata)
    let window = SQLExpression(
      predicate: .and([
        .comparison(field: "start_date", op: .lessThan, value: .string("2026-05-21")),
        .comparison(field: "end_date", op: .greaterThanOrEqual, value: .string("2026-05-20")),
      ])
    )
    let key = ElectricFetchTracker.metadataKey(predicate: window, orderBy: [], limit: nil)

    try metadata.recordFetch(
      table: "calendar_event",
      predicate: key.predicateHash,
      predicateJSON: key.predicateJSON,
      snapshotBoundary: nil,
      outcome: .absent,
      isComplete: true,
      transaction: nil
    )

    let plan = try await tracker.computeMissing(
      table: "calendar_event",
      requested: window,
      scope: nil,
      orderBy: [],
      limit: nil
    )

    #expect(!plan.needsFetch)
    #expect(plan.reuseExisting)
  }
}

private final class TestMetadataProvider: MetadataProvider, @unchecked Sendable {
  private var fetched: [String: [PredicateHash: FetchedPredicate]] = [:]
  private var ranges: [String: [String: [FetchedRange]]] = [:]
  private var syncStates: [String: SyncState] = [:]
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
}
