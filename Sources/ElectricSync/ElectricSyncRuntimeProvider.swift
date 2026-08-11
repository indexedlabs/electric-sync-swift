import Foundation

/// Supplies time, identity, and cancellable delays for ElectricSync operations.
public struct ElectricSyncRuntimeProvider: Sendable {
  private let nowClosure: @Sendable () -> Date
  private let makeUUIDClosure: @Sendable () -> UUID
  private let sleepClosure: @Sendable (Duration) async throws -> Void

  public init(
    now: @escaping @Sendable () -> Date,
    makeUUID: @escaping @Sendable () -> UUID,
    sleep: @escaping @Sendable (Duration) async throws -> Void
  ) {
    self.nowClosure = now
    self.makeUUIDClosure = makeUUID
    self.sleepClosure = sleep
  }

  public func now() -> Date {
    nowClosure()
  }

  /// Returns a fresh identifier for operation and ownership tracking.
  ///
  /// The provider must return a distinct value on every call. ElectricSync uses these values as
  /// dictionary keys and task ownership tokens, so reusing one can incorrectly merge unrelated work.
  public func makeUUID() -> UUID {
    makeUUIDClosure()
  }

  public func sleep(for duration: Duration) async throws {
    try await sleepClosure(duration)
  }

  public static let live = Self(
    now: { Date() },
    makeUUID: { UUID() },
    sleep: { duration in
      try await Task.sleep(for: duration)
    }
  )
}
