import Foundation
import Testing

@testable import ElectricSync

struct PredicateQueryParamsMapperTests {
  private typealias Descriptor = PredicateQueryParamsMapper<TestFilters>.FieldDescriptor

  @Test
  func returnsInitialFiltersWhenPredicateMissing() throws {
    let mapper = makeMapper()
    let filters = try mapper.filters(from: nil)
    #expect(filters.ids.isEmpty)
    #expect(filters.statuses.isEmpty)
  }

  @Test
  func extractsValuesForSupportedFields() throws {
    let mapper = makeMapper()
    let id = UUID()
    let predicate = SQLExpression(
      predicate: .and([
        .comparison(field: "id", op: .equal, value: .string(id.uuidString)),
        .membership(field: "status", values: [.string("draft"), .string("sent")]),
      ])
    )

    let filters = try mapper.filters(from: predicate)

    #expect(filters.ids == [id])
    #expect(Set(filters.statuses) == ["draft", "sent"])
  }

  @Test
  func throwsForUnsupportedOperator() {
    let mapper = makeMapper()
    let predicate = SQLExpression(
      predicate: .comparison(field: "id", op: .greaterThan, value: .string(UUID().uuidString))
    )

    do {
      _ = try mapper.filters(from: predicate)
      Issue.record("Expected unsupported operator error")
    } catch ElectricSyncError.fetchFailed {
      // Expected
    } catch {
      Issue.record("Unexpected error: \(error)")
    }
  }

  @Test
  func throwsForUnsupportedField() {
    let mapper = makeMapper()
    let predicate = SQLExpression(
      predicate: .comparison(field: "unknown", op: .equal, value: .string("value"))
    )

    do {
      _ = try mapper.filters(from: predicate)
      Issue.record("Expected unsupported field error")
    } catch ElectricSyncError.fetchFailed {
      // Expected
    } catch {
      Issue.record("Unexpected error: \(error)")
    }
  }

  @Test
  func throwsForOrPredicate() {
    let mapper = makeMapper()
    let value = PredicateValue.string(UUID().uuidString)
    let predicate = SQLExpression(
      predicate: .or([
        .comparison(field: "id", op: .equal, value: value),
        .comparison(field: "id", op: .equal, value: value),
      ])
    )

    do {
      _ = try mapper.filters(from: predicate)
      Issue.record("Expected OR predicate failure")
    } catch ElectricSyncError.fetchFailed {
      // Expected
    } catch {
      Issue.record("Unexpected error: \(error)")
    }
  }

  private func makeMapper() -> PredicateQueryParamsMapper<TestFilters> {
    let descriptors: [Descriptor] = [
      Descriptor(names: ["id"]) { filters, values in
        let identifiers = try values.map(Self.uuid(from:))
        filters.ids.append(contentsOf: identifiers)
      },
      Descriptor(names: ["status", "state"]) { filters, values in
        filters.statuses.append(contentsOf: values.compactMap(Self.string(from:)))
      },
    ]

    return PredicateQueryParamsMapper(
      context: "Test predicate",
      descriptors: descriptors,
      makeFilters: { TestFilters() }
    )
  }

  private static func uuid(from value: PredicateValue) throws -> UUID {
    if case .string(let raw) = value, let uuid = UUID(uuidString: raw) {
      return uuid
    }
    throw ElectricSyncError.fetchFailed("Predicate contains non-UUID value")
  }

  private static func string(from value: PredicateValue) -> String? {
    if case .string(let raw) = value {
      return raw
    }
    return nil
  }

  private struct TestFilters {
    var ids: [UUID] = []
    var statuses: [String] = []
  }
}
