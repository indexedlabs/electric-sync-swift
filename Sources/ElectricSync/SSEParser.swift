import Foundation

public enum SSEParseError: Error {
  case invalidUTF8
}

/// Minimal SSE parser that emits joined `data:` payloads (as UTF-8 strings).
/// - Ignores comment lines (`:`) and unsupported fields (`event:`, `id:`, `retry:`).
/// - Emits an event when encountering a blank line.
public struct SSEParser: Sendable {
  private var buffer = Data()
  private var pendingDataLines: [String] = []

  public init() {}

  /// Feed raw bytes and return any completed SSE `data:` events.
  public mutating func feed(_ chunk: Data) throws -> [String] {
    if !chunk.isEmpty {
      buffer.append(chunk)
    }

    var output: [String] = []

    while let newlineIndex = buffer.firstIndex(of: 0x0A) { // \n
      let lineData = buffer.prefix(upTo: newlineIndex)
      buffer.removeSubrange(..<buffer.index(after: newlineIndex))

      guard var line = String(data: lineData, encoding: .utf8) else {
        throw SSEParseError.invalidUTF8
      }

      if line.hasSuffix("\r") {
        line.removeLast()
      }

      // Blank line => dispatch event (if any data was collected)
      if line.isEmpty {
        if !pendingDataLines.isEmpty {
          output.append(pendingDataLines.joined(separator: "\n"))
          pendingDataLines.removeAll(keepingCapacity: true)
        }
        continue
      }

      // Comment/keep-alive line.
      if line.hasPrefix(":") {
        continue
      }

      // data lines. Support both `data:` and `data: `.
      if line.hasPrefix("data:") {
        var value = line.dropFirst("data:".count)
        if value.first == " " {
          value = value.dropFirst()
        }
        pendingDataLines.append(String(value))
        continue
      }

      // Ignore all other SSE fields.
    }

    return output
  }

  /// Call at end-of-stream to flush any pending `data:` lines (if needed).
  public mutating func finish() -> [String] {
    guard !pendingDataLines.isEmpty else { return [] }
    let event = pendingDataLines.joined(separator: "\n")
    pendingDataLines.removeAll(keepingCapacity: true)
    return [event]
  }
}
