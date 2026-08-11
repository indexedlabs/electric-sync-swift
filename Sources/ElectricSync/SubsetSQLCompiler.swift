import Foundation

public enum SubsetSQLCompilerError: Error {
  case invalidField(String)
  case emptyMembershipList(field: String)
  case unsupportedNullComparison(field: String, op: ComparisonOperator)
}

public enum SubsetSQLParamValue: Hashable, Sendable, Codable {
  case string(String)
  case int(Int)
  case double(Double)
  case bool(Bool)
  case null

  public init(_ value: PredicateValue) {
    switch value {
    case .string(let string):
      self = .string(string)
    case .int(let int):
      self = .int(int)
    case .double(let double):
      self = .double(double)
    case .bool(let bool):
      self = .bool(bool)
    case .null:
      self = .null
    }
  }

  public init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()

    if container.decodeNil() {
      self = .null
      return
    }
    if let bool = try? container.decode(Bool.self) {
      self = .bool(bool)
      return
    }
    if let int = try? container.decode(Int.self) {
      self = .int(int)
      return
    }
    if let double = try? container.decode(Double.self) {
      self = .double(double)
      return
    }
    self = .string(try container.decode(String.self))
  }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.singleValueContainer()
    switch self {
    case .string(let string):
      try container.encode(string)
    case .int(let int):
      try container.encode(int)
    case .double(let double):
      try container.encode(double)
    case .bool(let bool):
      try container.encode(bool)
    case .null:
      try container.encodeNil()
    }
  }
}

public struct SubsetSQLCompilation: Sendable, Equatable {
  public let whereClause: String
  public let params: [String: SubsetSQLParamValue]

  public init(whereClause: String, params: [String: SubsetSQLParamValue]) {
    self.whereClause = whereClause
    self.params = params
  }

  public func encodedParamsJSON() throws -> String {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let data = try encoder.encode(params)
    return String(decoding: data, as: UTF8.self)
  }
}

public struct SubsetSQLCompiler: Sendable {
  public init() {}

  public func compile(
    _ expression: SyncPredicateExpression
  ) throws -> SubsetSQLCompilation {
    var nextParamIndex = 0
    var params: [String: SubsetSQLParamValue] = [:]
    let whereClause = try compileExpression(
      expression,
      nextParamIndex: &nextParamIndex,
      params: &params
    )
    return SubsetSQLCompilation(whereClause: whereClause, params: params)
  }

  public func compileOrderBy(_ orderBy: [OrderBy]) throws -> String {
    try orderBy.map { item in
      let field = try validateField(item.field)
      let direction: String = switch item.direction {
      case .ascending:
        "ASC"
      case .descending:
        "DESC"
      }
      return "\(field) \(direction)"
    }.joined(separator: ", ")
  }

  private func compileExpression(
    _ expression: SyncPredicateExpression,
    nextParamIndex: inout Int,
    params: inout [String: SubsetSQLParamValue]
  ) throws -> String {
    switch expression {
    case .constant(let value):
      return value ? "TRUE" : "FALSE"

    case .comparison(let field, let op, let value):
      let validatedField = try validateField(field)
      if case .null = value {
        guard op == .equal else {
          throw SubsetSQLCompilerError.unsupportedNullComparison(field: validatedField, op: op)
        }
        return "\(validatedField) IS NULL"
      }
      nextParamIndex += 1
      let index = nextParamIndex
      params[String(index)] = SubsetSQLParamValue(value)
      return "\(validatedField) \(op.rawValue) $\(index)"

    case .membership(let field, let values):
      let validatedField = try validateField(field)
      guard !values.isEmpty else {
        throw SubsetSQLCompilerError.emptyMembershipList(field: validatedField)
      }
      var placeholders: [String] = []
      placeholders.reserveCapacity(values.count)
      for value in values {
        nextParamIndex += 1
        let index = nextParamIndex
        params[String(index)] = SubsetSQLParamValue(value)
        placeholders.append("$\(index)")
      }
      return "\(validatedField) IN (\(placeholders.joined(separator: ", ")))"

    case .and(let expressions):
      if expressions.isEmpty { return "TRUE" }
      if expressions.count == 1 {
        return try compileExpression(expressions[0], nextParamIndex: &nextParamIndex, params: &params)
      }
      let compiled = try expressions.map {
        try compileExpression($0, nextParamIndex: &nextParamIndex, params: &params)
      }
      return "(\(compiled.joined(separator: " AND ")))"

    case .or(let expressions):
      if expressions.isEmpty { return "FALSE" }
      if expressions.count == 1 {
        return try compileExpression(expressions[0], nextParamIndex: &nextParamIndex, params: &params)
      }
      let compiled = try expressions.map {
        try compileExpression($0, nextParamIndex: &nextParamIndex, params: &params)
      }
      return "(\(compiled.joined(separator: " OR ")))"

    case .not(let inner):
      let compiled = try compileExpression(inner, nextParamIndex: &nextParamIndex, params: &params)
      return "NOT (\(compiled))"
    }
  }

  private func validateField(_ field: String) throws -> String {
    let segments = field.split(separator: ".", omittingEmptySubsequences: false)
    guard !segments.isEmpty else {
      throw SubsetSQLCompilerError.invalidField(field)
    }

    var normalizedSegments: [String] = []
    normalizedSegments.reserveCapacity(segments.count)

    for segment in segments {
      let segmentString = String(segment)
      guard isValidIdentifierSegment(segmentString) else {
        throw SubsetSQLCompilerError.invalidField(field)
      }
      normalizedSegments.append(sqlColumnName(for: segmentString))
    }

    return normalizedSegments.joined(separator: ".")
  }

  private func sqlColumnName(for segment: String) -> String {
    switch segment {
    case "dateTimeValue":
      return "datetime_value"
    case "endDateTimeValue":
      return "end_datetime_value"
    case "startDateTime":
      return "start_datetime"
    case "endDateTime":
      return "end_datetime"
    default:
      return toSnakeCase(segment)
    }
  }

  private func isValidIdentifierSegment(_ segment: String) -> Bool {
    guard !segment.isEmpty else { return false }

    var iterator = segment.unicodeScalars.makeIterator()
    guard let first = iterator.next() else { return false }
    guard CharacterSet.letters.contains(first) || first == "_" else { return false }

    while let scalar = iterator.next() {
      if CharacterSet.alphanumerics.contains(scalar) { continue }
      if scalar == "_" { continue }
      return false
    }

    return true
  }

  private func toSnakeCase(_ input: String) -> String {
    guard !input.isEmpty else { return input }

    let scalars = Array(input.unicodeScalars)
    var output = String.UnicodeScalarView()
    output.reserveCapacity(scalars.count + 4)

    func isLowercase(_ scalar: Unicode.Scalar) -> Bool {
      CharacterSet.lowercaseLetters.contains(scalar)
    }

    func isUppercase(_ scalar: Unicode.Scalar) -> Bool {
      CharacterSet.uppercaseLetters.contains(scalar)
    }

    func isNumber(_ scalar: Unicode.Scalar) -> Bool {
      CharacterSet.decimalDigits.contains(scalar)
    }

    for index in scalars.indices {
      let scalar = scalars[index]
      let isUpper = isUppercase(scalar)

      if isUpper {
        if index > 0 {
          let prev = scalars[index - 1]
          let next: Unicode.Scalar? = index + 1 < scalars.count ? scalars[index + 1] : nil

          if isLowercase(prev) || isNumber(prev) {
            output.append("_")
          } else if isUppercase(prev), let next, isLowercase(next) {
            output.append("_")
          }
        }

        let lowercased = String(scalar).lowercased()
        output.append(contentsOf: lowercased.unicodeScalars)
      } else {
        output.append(scalar)
      }
    }

    return String(output)
  }
}
