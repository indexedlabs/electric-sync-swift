import Foundation

@testable import ElectricSync

final class RecordingLogProvider: LogProvider, @unchecked Sendable {
  struct Entry: Equatable {
    let level: LogLevel
    let message: String
    let metadata: [String: String]?
  }

  private let lock = NSLock()
  private var recorded = [Entry]()

  func log(_ level: LogLevel, message: String, metadata: [String: String]?) {
    lock.withLock {
      recorded.append(.init(level: level, message: message, metadata: metadata))
    }
  }

  func entries() -> [Entry] {
    lock.withLock { recorded }
  }
}
