import Testing

@testable import ElectricSync

struct SubsetSQLCompilerTests {
  @Test
  func compilesComparisonWithParams() throws {
    let compiler = SubsetSQLCompiler()
    let compiled = try compiler.compile(
      .comparison(field: "status", op: .equal, value: .string("open"))
    )

    #expect(compiled.whereClause == "status = $1")
    #expect(compiled.params == ["1": .string("open")])
    #expect(try compiled.encodedParamsJSON() == #"{"1":"open"}"#)
  }

  @Test
  func compilesNullEqualityWithoutParam() throws {
    let compiler = SubsetSQLCompiler()
    let compiled = try compiler.compile(
      .comparison(field: "endDateValue", op: .equal, value: .null)
    )

    #expect(compiled.whereClause == "end_date_value IS NULL")
    #expect(compiled.params.isEmpty)
    #expect(try compiled.encodedParamsJSON() == #"{}"#)
  }

  @Test
  func compilesDatetimeColumnAliases() throws {
    let compiler = SubsetSQLCompiler()
    let compiled = try compiler.compile(
      .and([
        .comparison(
          field: "startDateTime",
          op: .lessThan,
          value: .string("2026-06-09T00:00:00Z")
        ),
        .comparison(
          field: "endDateTime",
          op: .greaterThanOrEqual,
          value: .string("2026-06-08T00:00:00Z")
        ),
        .comparison(
          field: "dateTimeValue",
          op: .lessThan,
          value: .string("2026-06-09T00:00:00Z")
        ),
        .comparison(field: "endDateTimeValue", op: .equal, value: .null),
      ])
    )

    #expect(
      compiled.whereClause
        == "(start_datetime < $1 AND end_datetime >= $2 AND datetime_value < $3 AND end_datetime_value IS NULL)"
    )
    #expect(
      compiled.params
        == [
          "1": .string("2026-06-09T00:00:00Z"),
          "2": .string("2026-06-08T00:00:00Z"),
          "3": .string("2026-06-09T00:00:00Z"),
        ]
    )
  }

  @Test
  func compilesMembershipWithMultipleParams() throws {
    let compiler = SubsetSQLCompiler()
    let compiled = try compiler.compile(
      .membership(field: "id", values: [.int(1), .int(2)])
    )

    #expect(compiled.whereClause == "id IN ($1, $2)")
    #expect(compiled.params == ["1": .int(1), "2": .int(2)])
    #expect(try compiled.encodedParamsJSON() == #"{"1":1,"2":2}"#)
  }

  @Test
  func compilesNestedBooleanLogicWithParentheses() throws {
    let compiler = SubsetSQLCompiler()
    let compiled = try compiler.compile(
      .and([
        .comparison(field: "status", op: .equal, value: .string("open")),
        .or([
          .comparison(field: "priority", op: .equal, value: .string("high")),
          .comparison(field: "priority", op: .equal, value: .string("urgent")),
        ]),
      ])
    )

    #expect(compiled.whereClause == "(status = $1 AND (priority = $2 OR priority = $3))")
    #expect(
      compiled.params
        == [
          "1": .string("open"),
          "2": .string("high"),
          "3": .string("urgent"),
        ]
    )
  }

  @Test
  func compilesOrderBy() throws {
    let compiler = SubsetSQLCompiler()
    let orderBy = try compiler.compileOrderBy([
      OrderBy(field: "created_at", direction: .descending),
      OrderBy(field: "id", direction: .ascending),
    ])
    #expect(orderBy == "created_at DESC, id ASC")
  }

  @Test
  func rejectsInvalidFieldNames() {
    let compiler = SubsetSQLCompiler()

    do {
      _ = try compiler.compile(
        .comparison(field: "id; DROP TABLE", op: .equal, value: .string("1"))
      )
      Issue.record("Expected invalid field name error")
    } catch SubsetSQLCompilerError.invalidField {
      return
    } catch {
      Issue.record("Unexpected error: \(error)")
    }
  }

  @Test
  func rejectsEmptyMembershipLists() {
    let compiler = SubsetSQLCompiler()

    do {
      _ = try compiler.compile(
        .membership(field: "id", values: [])
      )
      Issue.record("Expected empty membership list error")
    } catch SubsetSQLCompilerError.emptyMembershipList {
      return
    } catch {
      Issue.record("Unexpected error: \(error)")
    }
  }
}
