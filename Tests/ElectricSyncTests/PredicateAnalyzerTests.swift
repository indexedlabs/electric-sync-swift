import Foundation
import Testing

@testable import ElectricSync

struct PredicateAnalyzerTests {
  private let analyzer = PredicateAnalyzer()

  @Test
  func returnsNilWhenExactPredicateFetched() {
    let requested = SQLExpression(
      predicate: .comparison(field: "age", op: .greaterThan, value: .int(10))
    )
    let fetched = makeFetchedPredicate(expression: requested.predicate!)

    let result = analyzer.computeMissing(requested: requested, fetched: [fetched])

    #expect(result == nil)
  }

  @Test
  func subtractsFetchedRangeFromRequestedRange() {
    let requested = SQLExpression(
      predicate: .comparison(field: "age", op: .greaterThan, value: .int(10))
    )
    let fetchedExpression = SyncPredicateExpression.comparison(
      field: "age",
      op: .greaterThan,
      value: .int(20)
    )
    let fetched = makeFetchedPredicate(expression: fetchedExpression)

    let result = analyzer.computeMissing(requested: requested, fetched: [fetched])

    let description = result?.predicate?.canonicalDescription()
    #expect(description == "age > 10 AND age <= 20")
  }

  @Test
  func handlesInPredicates() {
    let requested = SQLExpression(
      predicate: .in(field: "status", values: [.string("A"), .string("B"), .string("C")])
    )
    let fetched = makeFetchedPredicate(
      expression: .in(field: "status", values: [.string("A"), .string("B")])
    )

    let result = analyzer.computeMissing(requested: requested, fetched: [fetched])

    #expect(result?.predicate?.canonicalDescription() == "status = 'C'")
  }

  @Test
  func treatsDuplicateNestedAndPredicatesAsEquivalentCoverage() {
    let requestedExpression = SyncPredicateExpression.and([
      .comparison(field: "sourceType", op: .equal, value: .string("conversation")),
      .membership(field: "sourceId", values: [.string("conversation-1")]),
    ])
    let duplicatedFetchedExpression = SyncPredicateExpression.and([
      requestedExpression,
      requestedExpression,
    ])
    let requested = SQLExpression(predicate: requestedExpression)
    let fetched = makeFetchedPredicate(expression: duplicatedFetchedExpression)

    let result = analyzer.computeMissing(requested: requested, fetched: [fetched])

    #expect(result == nil)
  }

  private func makeFetchedPredicate(
    expression: SyncPredicateExpression,
    isComplete: Bool = true
  ) -> FetchedPredicate {
    let hash = PredicateHash(from: SQLExpression(predicate: expression))
    let json = expression.toJSONData().flatMap { String(data: $0, encoding: .utf8) }
    return FetchedPredicate(
      predicateHash: hash,
      predicateJSON: json,
      snapshotBoundary: nil,
      outcome: .present,
      isComplete: isComplete,
      fetchedAt: Date()
    )
  }
}
