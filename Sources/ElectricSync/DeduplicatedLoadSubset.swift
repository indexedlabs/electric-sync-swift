import Foundation

public struct LoadSubsetOptions: Hashable, Sendable {
  public let wherePredicate: SyncPredicateExpression?
  public let orderBy: [OrderBy]
  public let limit: Int?

  public init(
    wherePredicate: SyncPredicateExpression?,
    orderBy: [OrderBy] = [],
    limit: Int? = nil
  ) {
    self.wherePredicate = wherePredicate
    self.orderBy = orderBy
    self.limit = limit
  }
}

public actor DeduplicatedLoadSubset {
  public typealias LoadSubset = @Sendable (LoadSubsetOptions) async throws -> Void
  public typealias OnDeduplicate = @Sendable (LoadSubsetOptions) -> Void

  private struct InflightCall: Sendable {
    let id: UUID
    let options: LoadSubsetOptions
    let task: Task<Void, Error>
    let generation: Int
  }

  private let loadSubset: LoadSubset
  private let onDeduplicate: OnDeduplicate?
  private let runtimeProvider: ElectricSyncRuntimeProvider

  private var hasLoadedAllData = false
  private var unlimitedLoaded: [SyncPredicateExpression] = []
  private var limitedCalls: [LoadSubsetOptions] = []
  private var inflightCalls: [InflightCall] = []
  private var generation: Int = 0

  public init(
    loadSubset: @escaping LoadSubset,
    onDeduplicate: OnDeduplicate? = nil,
    runtimeProvider: ElectricSyncRuntimeProvider = .live
  ) {
    self.loadSubset = loadSubset
    self.onDeduplicate = onDeduplicate
    self.runtimeProvider = runtimeProvider
  }

  public func loadSubset(_ options: LoadSubsetOptions) async throws -> Bool {
    if hasLoadedAllData {
      onDeduplicate?(options)
      return true
    }

    if let requested = options.wherePredicate,
      unlimitedLoaded.contains(where: { PredicateLogic.isSubset(subset: requested, superset: $0) })
    {
      onDeduplicate?(options)
      return true
    }

    if options.limit != nil {
      if limitedCalls.contains(where: { Self.isOptionsSubset(subset: options, superset: $0) }) {
        onDeduplicate?(options)
        return true
      }
    }

    if let inflight = inflightCalls.first(where: {
      Self.isOptionsSubset(subset: options, superset: $0.options)
    }) {
      try await inflight.task.value
      onDeduplicate?(options)
      return true
    }

    var clonedOptions = options

    if options.limit == nil, !unlimitedLoaded.isEmpty {
      if let requested = options.wherePredicate {
        var remaining = requested
        for loaded in unlimitedLoaded {
          if PredicateLogic.isSubset(subset: remaining, superset: loaded) {
            onDeduplicate?(options)
            return true
          }
          guard let diff = PredicateLogic.minusWhere(requested: remaining, subtract: loaded) else {
            continue
          }
          if diff == .constant(false) {
            onDeduplicate?(options)
            return true
          }
          remaining = diff
        }
        clonedOptions = LoadSubsetOptions(
          wherePredicate: remaining,
          orderBy: options.orderBy,
          limit: options.limit
        )
      } else {
        clonedOptions = LoadSubsetOptions(
          wherePredicate: .not(Self.unionWherePredicates(unlimitedLoaded)),
          orderBy: options.orderBy,
          limit: options.limit
        )
      }
    }

    let inflightId = runtimeProvider.makeUUID()
    let capturedGeneration = generation

    let task = Task {
      try await loadSubset(clonedOptions)
    }

    inflightCalls.append(
      InflightCall(
        id: inflightId,
        options: clonedOptions,
        task: task,
        generation: capturedGeneration
      )
    )

    defer {
      inflightCalls.removeAll(where: { $0.id == inflightId })
    }

    do {
      try await task.value

      if capturedGeneration == generation {
        updateTracking(clonedOptions)
      }

      return false
    } catch {
      throw error
    }
  }

  public func reset() {
    hasLoadedAllData = false
    unlimitedLoaded = []
    limitedCalls = []
    inflightCalls = []
    generation += 1
  }

  private func updateTracking(_ options: LoadSubsetOptions) {
    if options.limit == nil {
      if options.wherePredicate == nil {
        hasLoadedAllData = true
        unlimitedLoaded = []
        limitedCalls = []
        inflightCalls = []
        return
      }

      if let wherePredicate = options.wherePredicate {
        unlimitedLoaded.append(wherePredicate)
      }
      return
    }

    limitedCalls.append(options)
  }

  private static func isOptionsSubset(
    subset: LoadSubsetOptions,
    superset: LoadSubsetOptions
  ) -> Bool {
    if superset.limit == nil {
      return PredicateLogic.isSubset(
        subset: subset.wherePredicate, superset: superset.wherePredicate)
    }

    if subset.limit == nil {
      return false
    }

    guard subset.wherePredicate == superset.wherePredicate else { return false }
    guard isOrderBySubset(subset: subset.orderBy, superset: superset.orderBy) else { return false }
    return subset.limit! <= superset.limit!
  }

  private static func isOrderBySubset(subset: [OrderBy], superset: [OrderBy]) -> Bool {
    if subset.isEmpty { return true }
    if subset.count > superset.count { return false }
    for (index, subsetClause) in subset.enumerated() {
      if superset[index] != subsetClause {
        return false
      }
    }
    return true
  }

  private static func unionWherePredicates(_ predicates: [SyncPredicateExpression])
    -> SyncPredicateExpression
  {
    if predicates.isEmpty {
      return .constant(false)
    }
    if predicates.count == 1 {
      return predicates[0]
    }
    return .or(predicates)
  }
}
