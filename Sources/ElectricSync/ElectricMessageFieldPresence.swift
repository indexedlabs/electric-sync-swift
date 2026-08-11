import Foundation

public enum ElectricFieldPresenceState: String, Sendable, Equatable {
  case absent
  case explicitNull = "explicit_null"
  case presentValue = "present_value"
}

public struct ElectricValueFieldPresence: Sendable, Equatable {
  public let presentKeys: Set<String>
  public let explicitNullKeys: Set<String>

  public init(
    presentKeys: Set<String> = [],
    explicitNullKeys: Set<String> = []
  ) {
    self.presentKeys = presentKeys
    self.explicitNullKeys = explicitNullKeys.intersection(presentKeys)
  }

  public var isEmpty: Bool {
    presentKeys.isEmpty
  }

  public func state(for key: String) -> ElectricFieldPresenceState {
    guard presentKeys.contains(key) else { return .absent }
    if explicitNullKeys.contains(key) {
      return .explicitNull
    }
    return .presentValue
  }
}

public struct ElectricPayloadFieldPresence: Sendable, Equatable {
  public let value: ElectricValueFieldPresence?
  public let oldValue: ElectricValueFieldPresence?

  public init(
    value: ElectricValueFieldPresence? = nil,
    oldValue: ElectricValueFieldPresence? = nil
  ) {
    self.value = value
    self.oldValue = oldValue
  }

  public var isEmpty: Bool {
    value == nil && oldValue == nil
  }
}

public enum ElectricPayloadFieldPresenceExtractor {
  public static func fromMessageData(
    _ messageData: Data
  ) throws -> ElectricPayloadFieldPresence? {
    let json = try JSONSerialization.jsonObject(with: messageData)
    guard let object = json as? [String: Any] else {
      return nil
    }
    return fromMessageJSONObject(object)
  }

  public static func fromMessageJSONObject(
    _ object: [String: Any]
  ) -> ElectricPayloadFieldPresence? {
    let valuePresence = valueFieldPresence(from: object["value"])
    let oldValuePresence = valueFieldPresence(from: object["old_value"])
    let presence = ElectricPayloadFieldPresence(value: valuePresence, oldValue: oldValuePresence)
    return presence.isEmpty ? nil : presence
  }

  public static func valueFieldPresence(
    from rawValue: Any?
  ) -> ElectricValueFieldPresence? {
    guard let valueObject = rawValue as? [String: Any] else { return nil }
    let presentKeys = Set(valueObject.keys)
    guard !presentKeys.isEmpty else { return nil }

    let explicitNullKeys = Set(
      valueObject.compactMap { key, value in
        value is NSNull ? key : nil
      }
    )

    return ElectricValueFieldPresence(
      presentKeys: presentKeys,
      explicitNullKeys: explicitNullKeys
    )
  }
}
