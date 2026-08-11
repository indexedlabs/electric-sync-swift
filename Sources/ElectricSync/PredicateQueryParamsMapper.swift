import Foundation

/// Generic helper that walks `SyncPredicateExpression` trees and extracts model-specific query
/// parameter values.
///
/// The mapper only understands conjunctions (AND). Callers provide `FieldDescriptor`s that describe
/// the supported columns/aliases along with closures that mutate the filter container. Each
/// descriptor can opt into comparison operators and membership (`IN`) support. Any structure outside
/// of those constraints (e.g., OR, NOT, constant FALSE) throws `ElectricSyncError` so we fail fast
/// instead of issuing an Electric query that the backend would reject.
public struct PredicateQueryParamsMapper<Filters> {
  public struct FieldDescriptor {
    public let names: [String]
    public let supportedComparisons: Set<ComparisonOperator>
    public let supportsMembership: Bool
    public let assign: (inout Filters, [PredicateValue]) throws -> Void

    public init(
      names: [String],
      supportedComparisons: Set<ComparisonOperator> = [.equal],
      supportsMembership: Bool = true,
      assign: @escaping (inout Filters, [PredicateValue]) throws -> Void
    ) {
      precondition(!names.isEmpty, "Predicate field descriptor requires at least one name")
      self.names = names
      self.supportedComparisons = supportedComparisons
      self.supportsMembership = supportsMembership
      self.assign = assign
    }
  }

  private let context: String
  private let descriptors: [String: FieldDescriptor]
  private let makeFilters: () -> Filters

  public init(
    context: String = "Predicate",
    descriptors: [FieldDescriptor],
    makeFilters: @escaping () -> Filters
  ) {
    self.context = context
    self.descriptors = Self.normalize(descriptors)
    self.makeFilters = makeFilters
  }

  public func filters(from expression: SQLExpression?) throws -> Filters {
    var filters = makeFilters()
    guard let predicate = expression?.predicate else { return filters }
    try parse(predicate, into: &filters)
    return filters
  }

  private func parse(_ expression: SyncPredicateExpression, into filters: inout Filters) throws {
    switch expression {
    case .constant(true):
      return
    case .constant(false):
      throw ElectricSyncError.fetchFailed("\(context) resolved to FALSE")
    case .comparison(let field, let op, let value):
      try handleComparison(field: field, op: op, value: value, filters: &filters)
    case .membership(let field, let values):
      try handleMembership(field: field, values: values, filters: &filters)
    case .and(let expressions):
      for expression in expressions {
        try parse(expression, into: &filters)
      }
    case .or:
      throw ElectricSyncError.fetchFailed("\(context) does not support OR predicates")
    case .not:
      throw ElectricSyncError.fetchFailed("\(context) does not support NOT predicates")
    }
  }

  private func handleComparison(
    field: String,
    op: ComparisonOperator,
    value: PredicateValue,
    filters: inout Filters
  ) throws {
    let descriptor = try descriptor(for: field)
    guard descriptor.supportedComparisons.contains(op) else {
      throw ElectricSyncError.fetchFailed(
        "\(context) unsupported comparison operator \(op.rawValue) for field \(field)"
      )
    }
    try descriptor.assign(&filters, [value])
  }

  private func handleMembership(
    field: String,
    values: [PredicateValue],
    filters: inout Filters
  ) throws {
    let descriptor = try descriptor(for: field)
    guard descriptor.supportsMembership else {
      throw ElectricSyncError.fetchFailed(
        "\(context) does not support membership predicates for field \(field)"
      )
    }
    try descriptor.assign(&filters, values)
  }

  private func descriptor(for field: String) throws -> FieldDescriptor {
    let normalized = field.lowercased()
    if let descriptor = descriptors[normalized] {
      return descriptor
    }
    throw ElectricSyncError.fetchFailed("\(context) unsupported predicate field: \(field)")
  }

  private static func normalize(_ descriptors: [FieldDescriptor]) -> [String: FieldDescriptor] {
    var lookup: [String: FieldDescriptor] = [:]
    for descriptor in descriptors {
      for name in descriptor.names {
        lookup[name.lowercased()] = descriptor
      }
    }
    return lookup
  }
}

extension PredicateQueryParamsMapper.FieldDescriptor {
  public static func uuidField(
    _ name: String,
    aliases: [String] = [],
    allowMembership: Bool = true,
    assign: @escaping (inout Filters, [UUID]) throws -> Void
  ) -> Self {
    let allNames = [name] + aliases
    return Self(
      names: allNames,
      supportedComparisons: [.equal],
      supportsMembership: allowMembership
    ) { filters, values in
      let uuids = try values.map { value -> UUID in
        if case .string(let raw) = value, let uuid = UUID(uuidString: raw) {
          return uuid
        }
        throw ElectricSyncError.fetchFailed(
          "Predicate contains non-UUID value for field \(name)"
        )
      }
      try assign(&filters, uuids)
    }
  }

  public static func boolField(
    _ name: String,
    aliases: [String] = [],
    allowMembership: Bool = false,
    assign: @escaping (inout Filters, [Bool]) throws -> Void
  ) -> Self {
    let allNames = [name] + aliases
    return Self(
      names: allNames,
      supportedComparisons: [.equal],
      supportsMembership: allowMembership
    ) { filters, values in
      let bools = try values.map { value -> Bool in
        if case .bool(let raw) = value {
          return raw
        }
        throw ElectricSyncError.fetchFailed(
          "Predicate contains non-boolean value for field \(name)"
        )
      }
      try assign(&filters, bools)
    }
  }
}
