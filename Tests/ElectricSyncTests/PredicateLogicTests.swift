import Foundation
import Testing

@testable import ElectricSync

struct PredicateLogicTests {
  @Test
  func minusWherePreservesCommonConditionsBeforeSameFieldRangeSubtraction() {
    let requested = SyncPredicateExpression.and([
      .comparison(field: "status", op: .equal, value: .string("open")),
      .comparison(field: "age", op: .greaterThan, value: .int(10)),
    ])
    let subtract = SyncPredicateExpression.and([
      .comparison(field: "status", op: .equal, value: .string("open")),
      .comparison(field: "age", op: .greaterThan, value: .int(20)),
    ])

    let result = PredicateLogic.minusWhere(requested: requested, subtract: subtract)

    #expect(
      result
        == .and([
          .comparison(field: "status", op: .equal, value: .string("open")),
          .comparison(field: "age", op: .greaterThan, value: .int(10)),
          .comparison(field: "age", op: .lessThanOrEqual, value: .int(20)),
        ])
    )
  }
}
