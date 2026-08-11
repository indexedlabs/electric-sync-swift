import Foundation

public enum ElectricProtocolQuarantineReason: String, Sendable, Equatable {
  case activeConditions = "active_conditions"
  case moveIn = "move_in"
  case tag
  case control
  case schema
  case version
}

public struct ElectricProtocolQuarantine: Sendable, Equatable {
  public let reason: ElectricProtocolQuarantineReason
  public let detail: String
  public let compatibilityMayChangeAfterFullBootstrap: Bool

  public init(
    reason: ElectricProtocolQuarantineReason,
    detail: String,
    compatibilityMayChangeAfterFullBootstrap: Bool = false
  ) {
    self.reason = reason
    self.detail = detail
    self.compatibilityMayChangeAfterFullBootstrap = compatibilityMayChangeAfterFullBootstrap
  }
}

public protocol ElectricProtocolIncompatibilityError: Error {
  var electricProtocolQuarantine: ElectricProtocolQuarantine { get }
}

public enum ElectricProtocolCompatibilityState: Sendable, Equatable {
  case compatible
  case incompatible(
    detail: String,
    compatibilityMayChangeAfterFullBootstrap: Bool = false
  )
}

public struct ElectricProtocolCapabilityPolicy: Sendable {
  public static let gateName = "electric_tagged_shape_protocol_1_7_7"

  /// Default runtime posture: the 1.7.7 tagged-shape protocol is always on. The
  /// server speaks it unconditionally, so a client that reports the capability
  /// as unavailable quarantines its own collections on the first
  /// `active_conditions` message. Flipping this is a tracker-generation
  /// boundary, so a client carrying a `.legacy` persisted epoch full-bootstraps
  /// once and then resumes normally.
  public static var enabled: Self {
    Self(isTaggedShapeProtocolEnabled: { true })
  }

  /// Legacy (pre-1.7.7) segment semantics. Retained for tests that pin wildcard
  /// behavior and the quarantine contract; not a default runtime path.
  public static var defaultOff: Self {
    Self(isTaggedShapeProtocolEnabled: { false })
  }

  private let isTaggedShapeProtocolEnabled: @Sendable () -> Bool
  private let schemaCompatibility: @Sendable () -> ElectricProtocolCompatibilityState
  private let versionCompatibility: @Sendable () -> ElectricProtocolCompatibilityState

  public init(
    isTaggedShapeProtocolEnabled: @escaping @Sendable () -> Bool,
    schemaCompatibility: @escaping @Sendable () -> ElectricProtocolCompatibilityState = {
      .compatible
    },
    versionCompatibility: @escaping @Sendable () -> ElectricProtocolCompatibilityState = {
      .compatible
    }
  ) {
    self.isTaggedShapeProtocolEnabled = isTaggedShapeProtocolEnabled
    self.schemaCompatibility = schemaCompatibility
    self.versionCompatibility = versionCompatibility
  }

  /// Live capability-gate readout shared with the membership tracker and the
  /// batch-application continuity fence so segment semantics, quarantine, and
  /// tracker-loss handling always agree on one gate.
  func isTaggedShapeCapabilityEnabled() -> Bool {
    isTaggedShapeProtocolEnabled()
  }

  public func semanticEpoch() -> ElectricProtocolSemanticEpoch {
    isTaggedShapeProtocolEnabled() ? .taggedShape1_7_7 : .legacy
  }

  func quarantine(for messages: [ElectricMessage]) -> ElectricProtocolQuarantine? {
    quarantine(for: messages, semanticEpoch: semanticEpoch())
  }

  func quarantine(
    for messages: [ElectricMessage],
    semanticEpoch: ElectricProtocolSemanticEpoch
  ) -> ElectricProtocolQuarantine? {
    if case .incompatible(let detail, let mayChange) = schemaCompatibility() {
      return ElectricProtocolQuarantine(
        reason: .schema,
        detail: detail,
        compatibilityMayChangeAfterFullBootstrap: mayChange
      )
    }

    if case .incompatible(let detail, let mayChange) = versionCompatibility() {
      return ElectricProtocolQuarantine(
        reason: .version,
        detail: detail,
        compatibilityMayChangeAfterFullBootstrap: mayChange
      )
    }

    if let detail = invalidControlDetail(in: messages) {
      return ElectricProtocolQuarantine(reason: .control, detail: detail)
    }

    if let detail = invalidTagDetail(in: messages) {
      return ElectricProtocolQuarantine(reason: .tag, detail: detail)
    }

    guard !semanticEpoch.isTaggedShapeCapabilityEnabled else { return nil }

    if messages.contains(where: { $0.activeConditions != nil }) {
      return ElectricProtocolQuarantine(
        reason: .activeConditions,
        detail: "tagged-shape capability is disabled"
      )
    }

    if messages.contains(where: { message in
      guard let event = message.event else { return false }
      if case .moveIn = event { return true }
      return false
    }) {
      return ElectricProtocolQuarantine(
        reason: .moveIn,
        detail: "move-in semantics require the tagged-shape capability gate"
      )
    }

    if messages.contains(where: containsNonParticipatingTagPosition) {
      return ElectricProtocolQuarantine(
        reason: .tag,
        detail: "non-participating tagged-shape positions require the capability gate"
      )
    }

    return nil
  }

  func quarantine(for error: Error) -> ElectricProtocolQuarantine? {
    if case .protocolQuarantined(let quarantine) = error as? ElectricSyncError {
      return quarantine
    }
    if let incompatibility = error as? any ElectricProtocolIncompatibilityError {
      return incompatibility.electricProtocolQuarantine
    }
    if let decodingError = error as? DecodingError {
      return quarantine(for: decodingError)
    }
    return nil
  }

  private func quarantine(for error: DecodingError) -> ElectricProtocolQuarantine? {
    let codingPath: [String]
    switch error {
    case .dataCorrupted(let context), .typeMismatch(_, let context),
      .valueNotFound(_, let context), .keyNotFound(_, let context):
      codingPath = context.codingPath.map(\.stringValue)
    @unknown default:
      return nil
    }

    guard let headersIndex = codingPath.lastIndex(of: "headers"),
      headersIndex + 1 < codingPath.endIndex
    else {
      return nil
    }

    let boundaryKey = codingPath[headersIndex + 1]
    switch boundaryKey {
    case "operation", "event", "control":
      return ElectricProtocolQuarantine(
        reason: .control,
        detail: "shape message carried an unsupported \(boundaryKey)"
      )
    case "tags", "removed_tags", "patterns":
      return ElectricProtocolQuarantine(
        reason: .tag,
        detail: "shape message carried an unsupported \(boundaryKey) value"
      )
    case "active_conditions":
      return ElectricProtocolQuarantine(
        reason: .activeConditions,
        detail: "shape message carried unsupported active_conditions"
      )
    case "schema", "schema_version":
      return ElectricProtocolQuarantine(
        reason: .schema,
        detail: "shape message carried an unsupported schema"
      )
    case "version", "protocol_version":
      return ElectricProtocolQuarantine(
        reason: .version,
        detail: "shape message carried an unsupported protocol version"
      )
    default:
      return nil
    }
  }

  private func invalidControlDetail(in messages: [ElectricMessage]) -> String? {
    for message in messages {
      switch message.control {
      case .upToDate where !message.isUpToDate:
        return "up-to-date control did not carry its completion marker"
      default:
        continue
      }
    }
    return nil
  }

  private func invalidTagDetail(in messages: [ElectricMessage]) -> String? {
    var batchTagSegmentCount: Int?
    var activeConditionCounts: [Int] = []

    for message in messages {
      if let activeConditions = message.activeConditions {
        activeConditionCounts.append(activeConditions.count)
      }
      let tags = (message.tags ?? []) + (message.removedTags ?? [])
      let segmentCounts = Set(tags.map(tagSegmentCount))
      if segmentCounts.count > 1 {
        return "one message carried membership tags with different segment counts"
      }

      if let messageTagSegmentCount = segmentCounts.first {
        if let batchTagSegmentCount, batchTagSegmentCount != messageTagSegmentCount {
          return "one batch carried membership tags with different segment counts"
        }
        batchTagSegmentCount = messageTagSegmentCount

        if let activeConditions = message.activeConditions,
          activeConditions.count != messageTagSegmentCount
        {
          return "active_conditions count did not match membership tag positions"
        }
      }
    }

    guard let batchTagSegmentCount else {
      return activeConditionCounts.isEmpty
        ? nil
        : "active_conditions did not carry a membership tag schema"
    }
    if activeConditionCounts.contains(where: { $0 != batchTagSegmentCount }) {
      return "active_conditions count did not match the batch membership tag schema"
    }
    for message in messages {
      guard let event = message.event else { continue }
      let patterns: [MovePattern]
      switch event {
      case .moveIn(let eventPatterns), .moveOut(let eventPatterns):
        patterns = eventPatterns
      }
      if patterns.contains(where: { $0.pos < 0 || $0.pos >= batchTagSegmentCount }) {
        return "move pattern position was outside the membership tag schema"
      }
    }

    return nil
  }

  private func containsNonParticipatingTagPosition(_ message: ElectricMessage) -> Bool {
    ((message.tags ?? []) + (message.removedTags ?? [])).contains { tag in
      tag.split(separator: "/", omittingEmptySubsequences: false).contains(where: { $0.isEmpty })
    }
  }

  private func tagSegmentCount(_ tag: String) -> Int {
    tag.split(separator: "/", omittingEmptySubsequences: false).count
  }
}
