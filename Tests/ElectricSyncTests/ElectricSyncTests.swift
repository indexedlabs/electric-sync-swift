import Testing

@testable import ElectricSync

struct ElectricSyncTests {
  @Test
  func predicateHashDoesNotLowercaseRawSQL() {
    let predicateA = SQLExpression("SELECT * FROM records WHERE status = 'OPEN'")
    let predicateB = SQLExpression("select * from records where status = 'open'")

    #expect(PredicateHash(from: predicateA) != PredicateHash(from: predicateB))
  }

  @Test
  func predicateHashUsesStructuredPredicateWhenAvailable() {
    let expression = SyncPredicateExpression.comparison(
      field: "status",
      op: .equal,
      value: .string("OPEN")
    )

    let predicateA = SQLExpression("status = 'OPEN'", predicate: expression)
    let predicateB = SQLExpression("status = 'open'", predicate: expression)

    #expect(PredicateHash(from: predicateA) == PredicateHash(from: predicateB))

    let expressionLowercased = SyncPredicateExpression.comparison(
      field: "status",
      op: .equal,
      value: .string("open")
    )
    #expect(
      PredicateHash(from: SQLExpression(predicate: expression))
        != PredicateHash(from: SQLExpression(predicate: expressionLowercased))
    )
  }
}
