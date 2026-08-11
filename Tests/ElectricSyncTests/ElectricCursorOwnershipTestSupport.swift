import Foundation

@testable import ElectricSync

struct RecordedElectricCursorLog: Sendable {
  let level: String
  let message: String
  let metadata: [String: String]
}

struct RecordedElectricCursorSpan: Sendable {
  let name: String
  let attributes: [String: String]
  let status: ElectricSyncSpanStatus
}

final class RecordingElectricCursorTelemetry: @unchecked Sendable, LogProvider,
  ElectricSyncTracer
{
  private let lock = NSLock()
  private var logs: [RecordedElectricCursorLog] = []
  private var spanNamesById: [UUID: String] = [:]
  private var spanAttributesById: [UUID: [String: String]] = [:]
  private var spans: [RecordedElectricCursorSpan] = []

  func log(_ level: LogLevel, message: String, metadata: [String: String]?) {
    lock.lock()
    logs.append(
      RecordedElectricCursorLog(
        level: level.rawValue,
        message: message,
        metadata: metadata ?? [:]
      )
    )
    lock.unlock()
  }

  func startSpan(name: String, attributes: [String: String]) -> any ElectricSyncSpan {
    let id = UUID()
    lock.lock()
    spanNamesById[id] = name
    spanAttributesById[id] = attributes
    lock.unlock()
    return RecordingElectricCursorSpanHandle(id: id, telemetry: self)
  }

  func recordedLogs() -> [RecordedElectricCursorLog] {
    lock.lock()
    defer { lock.unlock() }
    return logs
  }

  func recordedSpans() -> [RecordedElectricCursorSpan] {
    lock.lock()
    defer { lock.unlock() }
    return spans
  }

  fileprivate func setSpanAttribute(id: UUID, key: String, value: String) {
    lock.lock()
    defer { lock.unlock() }
    var attributes = spanAttributesById[id] ?? [:]
    attributes[key] = value
    spanAttributesById[id] = attributes
  }

  fileprivate func endSpan(id: UUID, status: ElectricSyncSpanStatus) {
    lock.lock()
    defer { lock.unlock() }
    spans.append(
      RecordedElectricCursorSpan(
        name: spanNamesById[id] ?? "<unknown>",
        attributes: spanAttributesById[id] ?? [:],
        status: status
      )
    )
    spanNamesById.removeValue(forKey: id)
    spanAttributesById.removeValue(forKey: id)
  }
}

private final class RecordingElectricCursorSpanHandle: @unchecked Sendable, ElectricSyncSpan {
  private let id: UUID
  private let telemetry: RecordingElectricCursorTelemetry
  private let lock = NSLock()
  private var hasEnded = false

  init(id: UUID, telemetry: RecordingElectricCursorTelemetry) {
    self.id = id
    self.telemetry = telemetry
  }

  func setAttribute(key: String, value: String) {
    lock.lock()
    defer { lock.unlock() }
    guard !hasEnded else { return }
    telemetry.setSpanAttribute(id: id, key: key, value: value)
  }

  func end(status: ElectricSyncSpanStatus) {
    lock.lock()
    defer { lock.unlock() }
    guard !hasEnded else { return }
    hasEnded = true
    telemetry.endSpan(id: id, status: status)
  }
}
