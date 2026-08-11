import Foundation

/// A host-owned session identity used to fence work across account lifecycles.
public struct ElectricSyncSession: Hashable, Sendable {
  public let generation: Int
  public let identifier: String

  public init(generation: Int, identifier: String) {
    self.generation = generation
    self.identifier = identifier
  }
}

/// Supplies the host lifecycle behavior ElectricSync needs without owning authentication.
public struct ElectricSyncSessionProvider: Sendable {
  private let captureAuthenticatedSessionClosure: @Sendable () -> ElectricSyncSession?
  private let isCurrentClosure: @Sendable (ElectricSyncSession) -> Bool
  private let registerTeardownHandlerClosure:
    @Sendable (@escaping @Sendable () async -> Void) -> UUID
  private let unregisterTeardownHandlerClosure: @Sendable (UUID) -> Void

  public init(
    captureAuthenticatedSession: @escaping @Sendable () -> ElectricSyncSession?,
    isCurrent: @escaping @Sendable (ElectricSyncSession) -> Bool,
    registerTeardownHandler: @escaping @Sendable (@escaping @Sendable () async -> Void) -> UUID,
    unregisterTeardownHandler: @escaping @Sendable (UUID) -> Void
  ) {
    self.captureAuthenticatedSessionClosure = captureAuthenticatedSession
    self.isCurrentClosure = isCurrent
    self.registerTeardownHandlerClosure = registerTeardownHandler
    self.unregisterTeardownHandlerClosure = unregisterTeardownHandler
  }

  public func captureAuthenticatedSession() -> ElectricSyncSession? {
    captureAuthenticatedSessionClosure()
  }

  public func isCurrent(_ session: ElectricSyncSession) -> Bool {
    isCurrentClosure(session)
  }

  public func registerTeardownHandler(
    _ handler: @escaping @Sendable () async -> Void
  ) -> UUID {
    registerTeardownHandlerClosure(handler)
  }

  public func unregisterTeardownHandler(_ id: UUID) {
    unregisterTeardownHandlerClosure(id)
  }

  /// A stable session for hosts that do not have an account lifecycle to fence.
  public static var unmanaged: Self {
    let session = ElectricSyncSession(generation: 0, identifier: "unmanaged")
    return Self(
      captureAuthenticatedSession: { session },
      isCurrent: { $0 == session },
      registerTeardownHandler: { _ in UUID() },
      unregisterTeardownHandler: { _ in }
    )
  }
}
