import Foundation
import Testing

@testable import ElectricSync

struct ElectricProtocolQuarantineTests {
  @Test
  func capabilityDefaultsOffAndRejectsTaggedShapeSignals() throws {
    let policy = ElectricProtocolCapabilityPolicy.defaultOff

    #expect(
      policy.quarantine(for: [
        message(tags: ["family//owner"], activeConditions: [true, false, true])
      ])?.reason == .activeConditions
    )
    #expect(
      policy.quarantine(for: [
        message(event: .moveIn(patterns: [MovePattern(pos: 0, value: "family")]))
      ])?.reason == .moveIn
    )
    #expect(
      policy.quarantine(for: [message(tags: ["family//owner"])])?.reason == .tag
    )
    #expect(policy.quarantine(for: [message(tags: ["family/owner"])]) == nil)
  }

  @Test
  func enabledCapabilityAcceptsWellFormedActiveConditionBatch() throws {
    let policy = ElectricProtocolCapabilityPolicy(isTaggedShapeProtocolEnabled: { true })
    let messages = [
      message(tags: ["family//owner"], activeConditions: [true, false, true]),
      message(tags: ["family/member/owner"]),
    ]

    #expect(policy.quarantine(for: messages) == nil)
  }

  @Test
  func enabledCapabilityAcceptsWellFormedMoveIn() throws {
    let policy = ElectricProtocolCapabilityPolicy(isTaggedShapeProtocolEnabled: { true })

    #expect(
      policy.quarantine(for: [
        message(
          tags: ["family/member/owner"],
          event: .moveIn(patterns: [MovePattern(pos: 1, value: "member")])
        )
      ]) == nil
    )
    #expect(
      policy.quarantine(for: [
        message(event: .moveIn(patterns: [MovePattern(pos: 0, value: "family")]))
      ]) == nil
    )
  }

  @Test
  func disabledCapabilityStillQuarantinesMoveIn() throws {
    let policy = ElectricProtocolCapabilityPolicy.defaultOff

    #expect(
      policy.quarantine(for: [
        message(
          tags: ["family/member/owner"],
          event: .moveIn(patterns: [MovePattern(pos: 1, value: "member")])
        )
      ])?.reason == .moveIn
    )
  }

  @Test
  func capabilityCanBeRolledBackWithoutRebuildingTheClient() throws {
    let capability = ProtocolCapabilitySwitch(true)
    let policy = ElectricProtocolCapabilityPolicy(
      isTaggedShapeProtocolEnabled: { capability.value }
    )
    let messages = [
      message(tags: ["family//owner"], activeConditions: [true, false, true])
    ]

    #expect(policy.quarantine(for: messages) == nil)
    capability.set(false)
    #expect(policy.quarantine(for: messages)?.reason == .activeConditions)
  }

  @Test
  func malformedTagsAndControlsFailClosedEvenWhenCapabilityIsEnabled() throws {
    let policy = ElectricProtocolCapabilityPolicy(isTaggedShapeProtocolEnabled: { true })

    #expect(
      policy.quarantine(for: [
        message(tags: ["family/owner"], activeConditions: [true])
      ])?.reason == .tag
    )
    #expect(
      policy.quarantine(for: [
        message(
          tags: ["family/owner"],
          event: .moveIn(patterns: [MovePattern(pos: 2, value: "outside")])
        )
      ])?.reason == .tag
    )
    #expect(
      policy.quarantine(for: [
        message(control: .upToDate, isUpToDate: false)
      ])?.reason == .control
    )
  }

  @Test
  func knownNormalizedControlsRemainCompatible() throws {
    let policy = ElectricProtocolCapabilityPolicy(isTaggedShapeProtocolEnabled: { true })

    #expect(
      policy.quarantine(for: [
        message(control: .snapshotEnd)
      ]) == nil
    )
    #expect(
      policy.quarantine(for: [
        message(control: .mustRefetch)
      ]) == nil
    )
  }

  @Test
  func configuredSchemaAndVersionIncompatibilityAreQuarantined() throws {
    let schemaPolicy = ElectricProtocolCapabilityPolicy(
      isTaggedShapeProtocolEnabled: { false },
      schemaCompatibility: {
        .incompatible(
          detail: "schema 8 is unsupported",
          compatibilityMayChangeAfterFullBootstrap: true
        )
      }
    )
    let versionPolicy = ElectricProtocolCapabilityPolicy(
      isTaggedShapeProtocolEnabled: { false },
      versionCompatibility: { .incompatible(detail: "server protocol 9 is unsupported") }
    )

    #expect(schemaPolicy.quarantine(for: [message()])?.reason == .schema)
    #expect(
      schemaPolicy.quarantine(for: [message()])?.compatibilityMayChangeAfterFullBootstrap == true
    )
    #expect(versionPolicy.quarantine(for: [message()])?.reason == .version)
  }

  @Test(arguments: ["operation", "event", "control", "schema_version", "protocol_version"])
  func exactHeaderBoundaryDecodeFailuresAreQuarantined(_ field: String) throws {
    let error = decodingError(codingPath: ["0", "headers", field])
    let quarantine = ElectricProtocolCapabilityPolicy.defaultOff.quarantine(for: error)

    #expect(quarantine != nil)
  }

  @Test
  func unrelatedPayloadDecodingErrorIsNotQuarantined() throws {
    let error = decodingError(codingPath: ["0", "value", "name"])

    #expect(ElectricProtocolCapabilityPolicy.defaultOff.quarantine(for: error) == nil)
  }

  private func message(
    tags: [String]? = nil,
    activeConditions: [Bool]? = nil,
    event: ElectricEvent? = nil,
    control: ElectricMessage.Control? = nil,
    isUpToDate: Bool = false
  ) -> ElectricMessage {
    ElectricMessage(
      payload: Data(),
      isUpToDate: isUpToDate,
      control: control,
      tags: tags,
      activeConditions: activeConditions,
      event: event
    )
  }

  private func decodingError(codingPath: [String]) -> DecodingError {
    .dataCorrupted(
      DecodingError.Context(
        codingPath: codingPath.map(TestCodingKey.init),
        debugDescription: "unsupported test value"
      )
    )
  }
}

private struct TestCodingKey: CodingKey {
  let stringValue: String
  let intValue: Int?

  init(_ value: String) {
    stringValue = value
    intValue = Int(value)
  }

  init?(stringValue: String) {
    self.init(stringValue)
  }

  init?(intValue: Int) {
    self.init("\(intValue)")
  }
}

private final class ProtocolCapabilitySwitch: @unchecked Sendable {
  private let lock = NSLock()
  private var storedValue: Bool

  init(_ value: Bool) {
    storedValue = value
  }

  var value: Bool {
    lock.withLock { storedValue }
  }

  func set(_ value: Bool) {
    lock.withLock {
      storedValue = value
    }
  }
}
