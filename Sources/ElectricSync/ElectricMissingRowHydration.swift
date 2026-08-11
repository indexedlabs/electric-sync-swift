import Foundation

public enum ElectricMissingRowHydration {
  public static func singleFieldDescriptor(
    rowKeys: [String],
    field: String,
    extractValue: (String) -> String?
  ) -> QueryDescriptor? {
    let values = Array(Set(rowKeys.compactMap(extractValue))).sorted()
    guard !values.isEmpty else { return nil }

    return QueryDescriptor(
      predicate: SQLExpression(
        predicate: .membership(
          field: field,
          values: values.map(PredicateValue.string)
        )
      ),
      orderBy: [],
      limit: nil
    )
  }

  public static func primaryKeyComponentDescriptor(
    rowKeys: [String],
    field: String,
    componentIndex: Int = 0
  ) -> QueryDescriptor? {
    singleFieldDescriptor(
      rowKeys: rowKeys,
      field: field,
      extractValue: { rowKey in
        if let parsed = ElectricRowKey.parse(rowKey) {
          return parsed.primaryKeyComponent(at: componentIndex)
        }
        return componentIndex == 0 ? rowKey : nil
      }
    )
  }
}
