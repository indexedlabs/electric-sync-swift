import Foundation

@testable import ElectricSync

final class TestSessionController: @unchecked Sendable {
  private let lock = NSLock()
  private var currentSession: ElectricSyncSession?
  private var generation: Int
  private var teardownHandlers: [UUID: @Sendable () async -> Void] = [:]

  init(initiallyAuthenticated: Bool = true) {
    generation = 0
    currentSession =
      initiallyAuthenticated
      ? ElectricSyncSession(generation: 0, identifier: "unmanaged")
      : nil
  }

  func captureAuthenticatedSession() -> ElectricSyncSession? {
    lock.withLock { currentSession }
  }

  func activate() -> ElectricSyncSession {
    lock.withLock {
      generation += 1
      let session = ElectricSyncSession(
        generation: generation,
        identifier: "session-\(generation)"
      )
      currentSession = session
      return session
    }
  }

  func beginTeardown() async {
    let handlers: [@Sendable () async -> Void] = lock.withLock {
      currentSession = nil
      return Array(teardownHandlers.values)
    }
    await withTaskGroup(of: Void.self) { group in
      for handler in handlers {
        group.addTask { await handler() }
      }
    }
  }

  func finishUnauthenticated() {
    lock.withLock { currentSession = nil }
  }

  func provider() -> ElectricSyncSessionProvider {
    ElectricSyncSessionProvider(
      captureAuthenticatedSession: { [weak self] in self?.captureAuthenticatedSession() },
      isCurrent: { [weak self] session in self?.captureAuthenticatedSession() == session },
      registerTeardownHandler: { [weak self] handler in
        guard let self else { return UUID() }
        return self.lock.withLock {
          let identifier = UUID()
          self.teardownHandlers[identifier] = handler
          return identifier
        }
      },
      unregisterTeardownHandler: { [weak self] identifier in
        let _: Void? = self?.lock.withLock {
          self?.teardownHandlers.removeValue(forKey: identifier)
        }
      }
    )
  }
}
