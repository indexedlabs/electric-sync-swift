import Foundation

public enum PredicateValue: Hashable, Sendable {
  case string(String)
  case int(Int)
  case double(Double)
  case bool(Bool)
  case null

  public init(_ value: String) { self = .string(value) }
  public init(_ value: Int) { self = .int(value) }
  public init(_ value: Double) { self = .double(value) }
  public init(_ value: Bool) { self = .bool(value) }

  public func equals(_ other: PredicateValue) -> Bool {
    switch (self, other) {
    case (.string(let lhs), .string(let rhs)): return lhs == rhs
    case (.int(let lhs), .int(let rhs)): return lhs == rhs
    case (.double(let lhs), .double(let rhs)): return lhs == rhs
    case (.bool(let lhs), .bool(let rhs)): return lhs == rhs
    case (.int(let lhs), .double(let rhs)): return Double(lhs) == rhs
    case (.double(let lhs), .int(let rhs)): return lhs == Double(rhs)
    case (.null, .null): return true
    default: return false
    }
  }

  public func compare(to other: PredicateValue) -> ComparisonResult? {
    switch (self, other) {
    case (.int(let lhs), .int(let rhs)):
      return lhs == rhs ? .orderedSame : (lhs < rhs ? .orderedAscending : .orderedDescending)
    case (.double(let lhs), .double(let rhs)):
      return lhs == rhs ? .orderedSame : (lhs < rhs ? .orderedAscending : .orderedDescending)
    case (.int(let lhs), .double(let rhs)):
      let lhsValue = Double(lhs)
      return lhsValue == rhs
        ? .orderedSame : (lhsValue < rhs ? .orderedAscending : .orderedDescending)
    case (.double(let lhs), .int(let rhs)):
      let rhsValue = Double(rhs)
      return lhs == rhsValue
        ? .orderedSame : (lhs < rhsValue ? .orderedAscending : .orderedDescending)
    case (.string(let lhs), .string(let rhs)):
      return lhs == rhs ? .orderedSame : (lhs < rhs ? .orderedAscending : .orderedDescending)
    default:
      return nil
    }
  }

  public func canonicalDescription() -> String {
    switch self {
    case .string(let value):
      return "'\(value)'"
    case .int(let value):
      return "\(value)"
    case .double(let value):
      return "\(value)"
    case .bool(let value):
      return value ? "TRUE" : "FALSE"
    case .null:
      return "NULL"
    }
  }
}

extension PredicateValue: Codable {
  enum CodingKeys: String, CodingKey {
    case type
    case string
    case int
    case double
    case bool
  }

  public init(from decoder: Decoder) throws {
    let container = try decoder.container(keyedBy: CodingKeys.self)
    let type = try container.decode(String.self, forKey: .type)
    switch type {
    case "string":
      self = .string(try container.decode(String.self, forKey: .string))
    case "int":
      self = .int(try container.decode(Int.self, forKey: .int))
    case "double":
      self = .double(try container.decode(Double.self, forKey: .double))
    case "bool":
      self = .bool(try container.decode(Bool.self, forKey: .bool))
    case "null":
      self = .null
    default:
      throw DecodingError.dataCorruptedError(
        forKey: .type,
        in: container,
        debugDescription: "Unsupported predicate value \(type)"
      )
    }
  }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    switch self {
    case .string(let value):
      try container.encode("string", forKey: .type)
      try container.encode(value, forKey: .string)
    case .int(let value):
      try container.encode("int", forKey: .type)
      try container.encode(value, forKey: .int)
    case .double(let value):
      try container.encode("double", forKey: .type)
      try container.encode(value, forKey: .double)
    case .bool(let value):
      try container.encode("bool", forKey: .type)
      try container.encode(value, forKey: .bool)
    case .null:
      try container.encode("null", forKey: .type)
    }
  }
}

public enum ComparisonOperator: String, Codable, Sendable {
  case equal = "="
  case greaterThan = ">"
  case greaterThanOrEqual = ">="
  case lessThan = "<"
  case lessThanOrEqual = "<="
}

public indirect enum SyncPredicateExpression: Hashable, Sendable, Codable {
  case constant(Bool)
  case comparison(field: String, op: ComparisonOperator, value: PredicateValue)
  case membership(field: String, values: [PredicateValue])
  case and([SyncPredicateExpression])
  case or([SyncPredicateExpression])
  case not(SyncPredicateExpression)

  public static func equals(field: String, value: PredicateValue) -> SyncPredicateExpression {
    .comparison(field: field, op: .equal, value: value)
  }

  public static func greaterThan(field: String, value: PredicateValue)
    -> SyncPredicateExpression
  {
    .comparison(field: field, op: .greaterThan, value: value)
  }

  public static func greaterThanOrEqual(field: String, value: PredicateValue)
    -> SyncPredicateExpression
  {
    .comparison(field: field, op: .greaterThanOrEqual, value: value)
  }

  public static func lessThan(field: String, value: PredicateValue) -> SyncPredicateExpression {
    .comparison(field: field, op: .lessThan, value: value)
  }

  public static func lessThanOrEqual(field: String, value: PredicateValue)
    -> SyncPredicateExpression
  {
    .comparison(field: field, op: .lessThanOrEqual, value: value)
  }

  public static func `in`(field: String, values: [PredicateValue]) -> SyncPredicateExpression {
    .membership(field: field, values: values)
  }

  public func canonicalDescription() -> String {
    switch self {
    case .constant(let value):
      return value ? "TRUE" : "FALSE"
    case .comparison(let field, let op, let value):
      if case .null = value, op == .equal {
        return "\(field) IS NULL"
      }
      return "\(field) \(op.rawValue) \(value.canonicalDescription())"
    case .membership(let field, let values):
      let joined = values.map { $0.canonicalDescription() }.joined(separator: ",")
      return "\(field) IN (\(joined))"
    case .and(let expressions):
      return expressions.map { $0.canonicalDescription() }.joined(separator: " AND ")
    case .or(let expressions):
      return expressions.map { $0.canonicalDescription() }.joined(separator: " OR ")
    case .not(let expression):
      return "NOT (\(expression.canonicalDescription()))"
    }
  }
}

// MARK: - Predicate Logic Helpers

struct PredicateLogic {
  static func isSubset(
    subset: SyncPredicateExpression?,
    superset: SyncPredicateExpression?
  ) -> Bool {
    switch (subset, superset) {
    case (nil, nil):
      return true
    case (nil, _?):
      return false
    case (_?, nil):
      return true
    case (.some(let lhs), .some(let rhs)):
      return isSubsetInternal(lhs.normalized(), rhs.normalized())
    }
  }

  private static func isSubsetInternal(
    _ subset: SyncPredicateExpression,
    _ superset: SyncPredicateExpression
  ) -> Bool {
    switch (subset, superset) {
    case (.constant(false), _):
      return true
    case (_, .constant(true)):
      return true
    case (.constant(true), _):
      return false
    case (_, .constant(false)):
      return subset == .constant(false)
    case (.and(let lhs), .and(let rhs)):
      return rhs.allSatisfy { required in
        lhs.contains(where: { isSubsetInternal($0, required) })
      }
    case (.and(let lhs), _):
      return lhs.contains(where: { isSubsetInternal($0, superset) })
    case (_, .and(let rhs)):
      return rhs.allSatisfy { isSubsetInternal(subset, $0) }
    case (.or(let lhs), .or(let rhs)):
      return lhs.allSatisfy { option in
        rhs.contains(where: { isSubsetInternal(option, $0) })
      }
    case (.or(let lhs), _):
      return lhs.allSatisfy { isSubsetInternal($0, superset) }
    case (_, .or(let rhs)):
      return rhs.contains(where: { isSubsetInternal(subset, $0) })
    case (.not(let lhs), .not(let rhs)):
      return isSubsetInternal(rhs, lhs)
    case (.not, _), (_, .not):
      return false
    case (
      .comparison(let fieldA, let opA, let valueA), .comparison(let fieldB, let opB, let valueB)
    ):
      guard fieldA == fieldB else { return false }
      return compareComparisons(opA: opA, valueA: valueA, opB: opB, valueB: valueB)
    case (.membership(let fieldA, let valuesA), .membership(let fieldB, let valuesB)):
      guard fieldA == fieldB else { return false }
      let supersetValues = Set(valuesB)
      return valuesA.allSatisfy { supersetValues.contains($0) }
    case (.comparison(let fieldA, let opA, let valueA), .membership(let fieldB, let valuesB)):
      guard fieldA == fieldB, opA == .equal else { return false }
      return valuesB.contains(where: { $0.equals(valueA) })
    case (.membership(let fieldA, let valuesA), .comparison(let fieldB, let opB, let valueB)):
      guard fieldA == fieldB, opB == .equal else { return false }
      return valuesA.contains(where: { $0.equals(valueB) })
    default:
      return false
    }
  }

  static func minusWhere(
    requested: SyncPredicateExpression?,
    subtract: SyncPredicateExpression?
  ) -> SyncPredicateExpression? {
    let normalizedRequested = requested?.normalized()
    let normalizedSubtract = subtract?.normalized()

    if normalizedSubtract == nil {
      return normalizedRequested
    }
    guard let requested = normalizedRequested else {
      return normalizedSubtract.map { .not($0).normalized() }
    }

    guard let subtract = normalizedSubtract else {
      return requested
    }

    if requested == subtract || isSubset(subset: requested, superset: subtract) {
      return .constant(false)
    }

    if let common = findCommonConditions(lhs: requested, rhs: subtract), !common.isEmpty {
      let lhsRemainder = removeConditions(from: requested, removing: common)
      let rhsRemainder = removeConditions(from: subtract, removing: common)
      if let simplified = minusWhere(requested: lhsRemainder, subtract: rhsRemainder) {
        return combineConditions(common: common, remainder: simplified)
      }
    }

    if let difference = minusSameField(from: requested, subtract: subtract) {
      return difference
    }

    return nil
  }

  private static func compareComparisons(
    opA: ComparisonOperator,
    valueA: PredicateValue,
    opB: ComparisonOperator,
    valueB: PredicateValue
  ) -> Bool {
    guard let comparison = valueA.compare(to: valueB) else {
      return false
    }

    switch (opA, opB, comparison) {
    case (_, _, .orderedSame):
      if opA == .equal {
        return opB == .equal
      }
      return true
    case (.greaterThan, .greaterThan, .orderedDescending),
      (.greaterThanOrEqual, .greaterThanOrEqual, .orderedDescending),
      (.greaterThan, .greaterThanOrEqual, .orderedDescending):
      return true
    case (.lessThan, .lessThan, .orderedAscending),
      (.lessThanOrEqual, .lessThanOrEqual, .orderedAscending),
      (.lessThan, .lessThanOrEqual, .orderedAscending):
      return true
    default:
      return false
    }
  }

  private static func findCommonConditions(
    lhs: SyncPredicateExpression,
    rhs: SyncPredicateExpression
  ) -> [SyncPredicateExpression]? {
    let lhsConditions = extractConditions(from: lhs)
    let rhsConditions = extractConditions(from: rhs)

    let overlap = lhsConditions.filter { rhsConditions.contains($0) }
    return overlap.isEmpty ? nil : overlap
  }

  private static func extractConditions(
    from expression: SyncPredicateExpression
  ) -> [SyncPredicateExpression] {
    switch expression {
    case .and(let expressions):
      return expressions.flatMap { extractConditions(from: $0) }
    default:
      return [expression]
    }
  }

  private static func removeConditions(
    from expression: SyncPredicateExpression,
    removing conditions: [SyncPredicateExpression]
  ) -> SyncPredicateExpression? {
    switch expression {
    case .and(let expressions):
      let remaining = expressions.filter { !conditions.contains($0) }
      if remaining.isEmpty {
        return nil
      }
      if remaining.count == 1 {
        return remaining.first
      }
      return .and(remaining)
    default:
      return conditions.contains(expression) ? nil : expression
    }
  }

  private static func combineConditions(
    common: [SyncPredicateExpression],
    remainder: SyncPredicateExpression
  ) -> SyncPredicateExpression {
    var merged = common
    merged.append(remainder)
    return SyncPredicateExpression.and(merged).normalized()
  }

  private static func minusSameField(
    from: SyncPredicateExpression,
    subtract: SyncPredicateExpression
  ) -> SyncPredicateExpression? {
    switch (from, subtract) {
    case (.membership(let fieldA, let valuesA), .membership(let fieldB, let valuesB))
    where fieldA == fieldB:
      let remaining = valuesA.filter { candidate in
        !valuesB.contains(where: { $0.equals(candidate) })
      }
      return membershipResult(field: fieldA, values: remaining)

    case (.membership(let fieldA, let valuesA), .comparison(let fieldB, let opB, let valueB))
    where fieldA == fieldB && opB == .equal:
      let remaining = valuesA.filter { !$0.equals(valueB) }
      return membershipResult(field: fieldA, values: remaining)

    case (
      .comparison(let fieldA, let opA, let valueA), .comparison(let fieldB, let opB, let valueB)
    )
    where fieldA == fieldB:
      return minusRange(
        field: fieldA,
        fromOp: opA,
        fromValue: valueA,
        subtractOp: opB,
        subtractValue: valueB
      )

    default:
      return nil
    }
  }

  private static func membershipResult(
    field: String,
    values: [PredicateValue]
  ) -> SyncPredicateExpression? {
    if values.isEmpty {
      return .constant(false)
    }
    if values.count == 1, let value = values.first {
      return .comparison(field: field, op: .equal, value: value)
    }
    return .membership(field: field, values: values)
  }

  private static func minusRange(
    field: String,
    fromOp: ComparisonOperator,
    fromValue: PredicateValue,
    subtractOp: ComparisonOperator,
    subtractValue: PredicateValue
  ) -> SyncPredicateExpression? {
    guard let relation = fromValue.compare(to: subtractValue) else {
      return nil
    }

    func and(_ lhs: SyncPredicateExpression, _ rhs: SyncPredicateExpression)
      -> SyncPredicateExpression
    {
      .and([lhs, rhs])
    }

    switch (fromOp, subtractOp, relation) {
    case (.greaterThan, .greaterThan, .orderedAscending),
      (.greaterThanOrEqual, .greaterThanOrEqual, .orderedAscending),
      (.greaterThan, .greaterThanOrEqual, .orderedAscending),
      (.greaterThanOrEqual, .greaterThan, .orderedAscending),
      (.greaterThanOrEqual, .greaterThan, .orderedSame):
      return and(
        .comparison(field: field, op: fromOp, value: fromValue),
        .comparison(field: field, op: .lessThanOrEqual, value: subtractValue)
      )

    case (.lessThan, .lessThan, .orderedDescending),
      (.lessThanOrEqual, .lessThanOrEqual, .orderedDescending),
      (.lessThan, .lessThanOrEqual, .orderedDescending),
      (.lessThanOrEqual, .lessThan, .orderedDescending),
      (.lessThanOrEqual, .lessThan, .orderedSame):
      return and(
        .comparison(field: field, op: .greaterThanOrEqual, value: subtractValue),
        .comparison(field: field, op: fromOp, value: fromValue)
      )

    default:
      return nil
    }
  }
}

extension SyncPredicateExpression {
  func normalized() -> SyncPredicateExpression {
    switch self {
    case .constant, .comparison, .membership:
      return self
    case .not(let expression):
      let normalizedExpression = expression.normalized()
      switch normalizedExpression {
      case .constant(let value):
        return .constant(!value)
      case .not(let nested):
        return nested.normalized()
      default:
        return .not(normalizedExpression)
      }
    case .and(let expressions):
      var flattened: [SyncPredicateExpression] = []
      for expression in expressions {
        switch expression.normalized() {
        case .constant(true):
          continue
        case .constant(false):
          return .constant(false)
        case .and(let nested):
          appendUnique(nested, to: &flattened)
        case let normalized:
          appendUnique([normalized], to: &flattened)
        }
      }
      switch flattened.count {
      case 0:
        return .constant(true)
      case 1:
        return flattened[0]
      default:
        return .and(flattened)
      }
    case .or(let expressions):
      var flattened: [SyncPredicateExpression] = []
      for expression in expressions {
        switch expression.normalized() {
        case .constant(false):
          continue
        case .constant(true):
          return .constant(true)
        case .or(let nested):
          appendUnique(nested, to: &flattened)
        case let normalized:
          appendUnique([normalized], to: &flattened)
        }
      }
      switch flattened.count {
      case 0:
        return .constant(false)
      case 1:
        return flattened[0]
      default:
        return .or(flattened)
      }
    }
  }

  private func appendUnique(
    _ expressions: [SyncPredicateExpression],
    to target: inout [SyncPredicateExpression]
  ) {
    for expression in expressions where !target.contains(expression) {
      target.append(expression)
    }
  }

  func toJSONData() -> Data? {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    return try? encoder.encode(normalized())
  }

  static func fromJSON(_ string: String) -> SyncPredicateExpression? {
    guard let data = string.data(using: .utf8) else { return nil }
    let decoder = JSONDecoder()
    return try? decoder.decode(SyncPredicateExpression.self, from: data).normalized()
  }
}
