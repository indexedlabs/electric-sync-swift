import Testing

@testable import ElectricSync

struct PostgresSnapshotTests {
  struct VisibilityCase: Sendable, CustomTestStringConvertible {
    let name: String
    let snapshot: PostgresSnapshot
    let transactionId: Int64
    let isVisible: Bool

    var testDescription: String { name }
  }

  @Test(
    arguments: [
      VisibilityCase(
        name: "transaction before xmin is visible",
        snapshot: PostgresSnapshot(xmin: "100", xmax: "200", xipList: []),
        transactionId: 99,
        isVisible: true
      ),
      VisibilityCase(
        name: "committed transaction inside snapshot is visible",
        snapshot: PostgresSnapshot(xmin: "100", xmax: "200", xipList: []),
        transactionId: 150,
        isVisible: true
      ),
      VisibilityCase(
        name: "transaction still in progress is not visible",
        snapshot: PostgresSnapshot(xmin: "100", xmax: "200", xipList: ["150"]),
        transactionId: 150,
        isVisible: false
      ),
      VisibilityCase(
        name: "transaction at xmax is not visible",
        snapshot: PostgresSnapshot(xmin: "100", xmax: "200", xipList: []),
        transactionId: 200,
        isVisible: false
      ),
      VisibilityCase(
        name: "transaction after xmax is not visible",
        snapshot: PostgresSnapshot(xmin: "100", xmax: "200", xipList: []),
        transactionId: 250,
        isVisible: false
      ),
      VisibilityCase(
        name: "invalid xmin fails closed",
        snapshot: PostgresSnapshot(xmin: "invalid", xmax: "200", xipList: []),
        transactionId: 50,
        isVisible: false
      ),
      VisibilityCase(
        name: "invalid xmax fails closed",
        snapshot: PostgresSnapshot(xmin: "100", xmax: "invalid", xipList: []),
        transactionId: 50,
        isVisible: false
      ),
    ]
  )
  func visibilityMatchesPostgresSnapshotRules(_ testCase: VisibilityCase) {
    #expect(
      testCase.snapshot.isVisible(transactionId: testCase.transactionId)
        == testCase.isVisible
    )
  }
}
