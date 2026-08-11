import Foundation

/// Parser for Electric's structured `message.key` format.
///
/// Upstream encodes keys as: `"schema"."table"/"pk1"/"pk2"...` with escaping:
/// - `/` is escaped as `//` in values
/// - `.` is escaped as `..` in schema/table names
/// - `nil` PK values are encoded as `_`
public struct ElectricRowKey: Hashable, Sendable {
  public let schema: String?
  public let table: String
  public let primaryKeyComponents: [String?]

  public init(schema: String?, table: String, primaryKeyComponents: [String?]) {
    self.schema = schema
    self.table = table
    self.primaryKeyComponents = primaryKeyComponents
  }

  public static func parse(_ key: String) -> ElectricRowKey? {
    let segments = splitAndUnescapeDoubled(key, delimiter: "/")
    guard let head = segments.first, !head.isEmpty else { return nil }

    let headParts = splitAndUnescapeDoubled(head, delimiter: ".")
    guard !headParts.isEmpty else { return nil }

    let schema: String?
    let table: String

    if headParts.count >= 2 {
      schema = stripQuotes(headParts[0])
      table = stripQuotes(headParts[1])
    } else {
      schema = nil
      table = stripQuotes(headParts[0])
    }

    let pkComponents: [String?] = segments.dropFirst().map { segment in
      if segment == "_" { return nil }
      return stripQuotes(segment)
    }

    return ElectricRowKey(schema: schema, table: table, primaryKeyComponents: pkComponents)
  }

  /// Produces the precise key spelling Electric uses for a materialized row.
  ///
  /// Ownership records retain the wire key verbatim, so local reconstruction
  /// must use this encoder rather than a table-specific UUID shortcut. In
  /// particular, index and join tables commonly have composite primary keys.
  public func encoded() -> String {
    Self.encode(
      schema: schema,
      table: table,
      primaryKeyComponents: primaryKeyComponents
    )
  }

  public static func encode(
    schema: String?,
    table: String,
    primaryKeyComponents: [String?]
  ) -> String {
    let head =
      if let schema {
        "\"\(escape(schema, delimiter: "."))\".\"\(escape(table, delimiter: "."))\""
      } else {
        "\"\(escape(table, delimiter: "."))\""
      }
    return
      ([head]
      + primaryKeyComponents.map { component in
        guard let component else { return "_" }
        return "\"\(escape(component, delimiter: "/"))\""
      }).joined(separator: "/")
  }

  public func primaryKeyComponent(at index: Int) -> String? {
    guard index >= 0, index < primaryKeyComponents.count else { return nil }
    return primaryKeyComponents[index]
  }

  private static func stripQuotes(_ value: String) -> String {
    guard value.count >= 2, value.first == "\"", value.last == "\"" else { return value }
    return String(value.dropFirst().dropLast())
  }

  private static func escape(_ value: String, delimiter: Character) -> String {
    value.replacingOccurrences(of: String(delimiter), with: String(repeating: delimiter, count: 2))
  }

  private static func splitAndUnescapeDoubled(_ value: String, delimiter: Character) -> [String] {
    var parts: [String] = []
    parts.reserveCapacity(4)

    var current = ""
    current.reserveCapacity(value.count)

    var idx = value.startIndex
    while idx < value.endIndex {
      let ch = value[idx]
      if ch == delimiter {
        let next = value.index(after: idx)
        if next < value.endIndex, value[next] == delimiter {
          current.append(delimiter)
          idx = value.index(after: next)
          continue
        }

        parts.append(current)
        current = ""
        idx = next
        continue
      }

      current.append(ch)
      idx = value.index(after: idx)
    }

    parts.append(current)
    return parts
  }
}
