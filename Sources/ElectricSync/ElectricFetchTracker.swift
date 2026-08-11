import CryptoKit
import Foundation

internal struct FetchMetadataKey: Sendable {
  let predicateHash: PredicateHash
  let predicateJSON: String?
  let isScoped: Bool
}

public actor ElectricFetchTracker {
  private let metadataProvider: MetadataProvider
  private let predicateAnalyzer: PredicateAnalyzer

  private static let scopedPrefix = "scoped|"

  public init(
    metadataProvider: MetadataProvider,
    predicateAnalyzer: PredicateAnalyzer = PredicateAnalyzer()
  ) {
    self.metadataProvider = metadataProvider
    self.predicateAnalyzer = predicateAnalyzer
  }

  public func computeMissing(
    table: String,
    requested: SQLExpression?,
    scope: SQLExpression?,
    orderBy: [OrderBy],
    limit: Int?
  ) async throws -> FetchPlan {
    let effectiveRequested = Self.combinedCoveragePredicate(
      scope: scope,
      requested: requested
    )
    let unscopedKey = Self.metadataKey(predicate: effectiveRequested, orderBy: [], limit: nil)

    if try metadataProvider.hasFetched(
      table: table,
      predicate: unscopedKey.predicateHash,
      transaction: nil
    ) {
      return FetchPlan(
        needsFetch: false,
        predicate: nil,
        ranges: nil,
        reuseExisting: true
      )
    }

    if let limit {
      let scopedKey = Self.metadataKey(
        predicate: effectiveRequested,
        orderBy: orderBy,
        limit: limit
      )
      if try metadataProvider.hasFetched(
        table: table,
        predicate: scopedKey.predicateHash,
        transaction: nil
      ) {
        return FetchPlan(
          needsFetch: false,
          predicate: nil,
          ranges: nil,
          reuseExisting: true
        )
      }
    }

    let fetched = try metadataProvider.getFetchedPredicates(table: table, transaction: nil)
      .filter { !Self.isScopedPredicate($0.predicateHash) }
    if let effectiveRequested {
      let missingPredicate = predicateAnalyzer.computeMissing(
        requested: effectiveRequested,
        fetched: fetched
      )

      if missingPredicate == nil {
        return FetchPlan(
          needsFetch: false,
          predicate: nil,
          ranges: nil,
          reuseExisting: true
        )
      }

      if scope == nil, let missingPredicate {
        return FetchPlan(
          needsFetch: true,
          predicate: missingPredicate,
          ranges: nil,
          reuseExisting: false
        )
      }
    }

    // Default: fetch with requested predicate (could be nil for full table)
    return FetchPlan(
      needsFetch: true,
      predicate: requested,
      ranges: nil,
      reuseExisting: false
    )
  }

  nonisolated static func metadataKey(
    predicate: SQLExpression?,
    orderBy: [OrderBy],
    limit: Int?,
    cursor: ElectricCursorExpressions? = nil
  ) -> FetchMetadataKey {
    let baseHash = PredicateHash(from: predicate)
    if let cursor {
      let cursorHash = cursorFingerprint(cursor)
      let orderKey = orderByKey(orderBy)
      let scopedHash = PredicateHash(
        value: "\(scopedPrefix)\(baseHash.value)|cursor:\(cursorHash)|\(orderKey)|limit:\(limit ?? -1)"
      )
      return FetchMetadataKey(
        predicateHash: scopedHash,
        predicateJSON: nil,
        isScoped: true
      )
    }

    if let limit {
      let orderKey = orderByKey(orderBy)
      let scopedHash = PredicateHash(
        value: "\(scopedPrefix)\(baseHash.value)|\(orderKey)|limit:\(limit)"
      )
      return FetchMetadataKey(
        predicateHash: scopedHash,
        predicateJSON: nil,
        isScoped: true
      )
    }

    let predicateJSON = predicate?.encodedPredicateJSON() ?? predicate?.normalized()
    return FetchMetadataKey(
      predicateHash: baseHash,
      predicateJSON: predicateJSON,
      isScoped: false
    )
  }

  nonisolated static func combinedCoveragePredicate(
    scope: SQLExpression?,
    requested: SQLExpression?
  ) -> SQLExpression? {
    switch (scope, requested) {
    case let (scope?, requested?):
      if let scopePredicate = scope.predicate, let requestedPredicate = requested.predicate {
        return SQLExpression(predicate: .and([scopePredicate, requestedPredicate]))
      }
      return SQLExpression("(\(scope.rawValue)) AND (\(requested.rawValue))")
    case let (scope?, nil):
      return scope
    case let (nil, requested?):
      return requested
    case (nil, nil):
      return nil
    }
  }

  nonisolated static func isScopedPredicate(_ predicateHash: PredicateHash) -> Bool {
    predicateHash.value.hasPrefix(scopedPrefix)
  }

  nonisolated private static func orderByKey(_ orderBy: [OrderBy]) -> String {
    guard !orderBy.isEmpty else { return "order:none" }
    let parts = orderBy.map { "\($0.field):\($0.direction.rawValue)" }
    return "order:\(parts.joined(separator: ","))"
  }

  nonisolated private static func cursorFingerprint(_ cursor: ElectricCursorExpressions?) -> String {
    guard let cursor else { return "none" }
    let fromJSON = String(decoding: cursor.whereFrom.toJSONData() ?? Data(), as: UTF8.self)
    let currentJSON = String(decoding: cursor.whereCurrent.toJSONData() ?? Data(), as: UTF8.self)
    let data = Data("from:\(fromJSON)|current:\(currentJSON)".utf8)
    let digest = SHA256.hash(data: data)
    return digest.map { String(format: "%02x", $0) }.joined()
  }
}

public struct PredicateAnalyzer: Sendable {
  public init() {}

  public func computeMissing(
    requested: SQLExpression?,
    fetched: [FetchedPredicate]
  ) -> SQLExpression? {
    let requestedHash = PredicateHash(from: requested)
    if fetched.contains(where: { $0.predicateHash == requestedHash && $0.isComplete }) {
      return nil
    }

    guard let requestedPredicate = requested?.predicate else {
      return requested
    }

    var remaining = requestedPredicate
    var modified = false

    for predicate in fetched where predicate.isComplete {
      guard let fetchedExpression = predicate.expression else { continue }

      if PredicateLogic.isSubset(subset: requestedPredicate, superset: fetchedExpression) {
        return nil
      }

      guard
        let difference = PredicateLogic.minusWhere(
          requested: remaining,
          subtract: fetchedExpression
        )
      else {
        continue
      }

      if difference == .constant(false) {
        return nil
      }

      remaining = difference
      modified = true
    }

    return modified ? SQLExpression(predicate: remaining) : requested
  }
}
