import Foundation

public enum ElectricLegacyBootstrapRolloutPolicy {
  public static let maxConcurrentPerProcess = 1
  public static let minimumCompletedForExpansion = 100
  public static let maximumFailureRateForExpansion = 0.01
  public static let maximumP95DurationMillisecondsForExpansion = 30_000.0
  public static let minimumCompletedForRollback = 20
  public static let failureRateForRollback = 0.05
  public static let p95DurationMillisecondsForRollback = 60_000.0
  public static let minimumExactCursorAdvanceRate = 0.99
  public static let stableExitWindowDays = 14
  public static let rollbackExitRequiresZeroLegacyResumeDays = 14
}

public struct ElectricLegacyBootstrapMetricsSnapshot: Equatable, Sendable {
  public let isEnabled: Bool
  public let queued: Int
  public let inFlight: Int
  public let admitted: Int
  public let completed: Int
  public let nonAdvancingCompleted: Int
  public let superseded: Int
  public let failed: Int
  public let cancelled: Int
  public let rejected: Int
  public let exactCursorAdvanced: Int
  public let decodedPayloadBytes: Int
}

private final class ElectricLegacyBootstrapMetricsAccumulator: @unchecked Sendable {
  private let lock = NSLock()
  private var decodedPayloadBytes = 0

  func recordDecodedPayloadBytes(_ count: Int) {
    guard count > 0 else { return }
    lock.lock()
    decodedPayloadBytes += count
    lock.unlock()
  }

  func snapshotDecodedPayloadBytes() -> Int {
    lock.lock()
    defer { lock.unlock() }
    return decodedPayloadBytes
  }
}

public struct ElectricLegacyBootstrapAdmission: Sendable {
  fileprivate let id: UUID
  fileprivate let metricsAccumulator: ElectricLegacyBootstrapMetricsAccumulator
  public let identity: ElectricReplicaIdentity
  public let legacyCursorCount: Int
  public let queueWaitMilliseconds: Double
  public let wasQueued: Bool

  func recordDecodedPayloadBytes(_ count: Int) {
    metricsAccumulator.recordDecodedPayloadBytes(count)
  }

  fileprivate var decodedPayloadBytes: Int {
    metricsAccumulator.snapshotDecodedPayloadBytes()
  }
}

public struct ElectricLegacyBootstrapAdmissionState: Equatable, Sendable {
  public let isEnabled: Bool
  public let queued: Int
  public let inFlight: Int
}

public enum ElectricLegacyBootstrapOutcome: Sendable {
  case completed(exactCursorAdvanced: Bool)
  case completedWithoutCursorAdvance
  case failed
  case cancelled
  case supersededByExactState
}

/// Bounds legacy-cursor cold bootstraps across every replica in one process.
///
/// Closing admission rejects queued work immediately. A bootstrap that already
/// holds the slot drains so an interrupted apply cannot leave cursor state
/// half-published.
public actor ElectricLegacyBootstrapAdmissionController {
  private struct Waiter {
    let id: UUID
    let identity: ElectricReplicaIdentity
    let legacyCursorCount: Int
    let enqueuedAt: Date
    let continuation: CheckedContinuation<ElectricLegacyBootstrapAdmission, Error>
  }

  private var isEnabled: Bool
  private var waiters: [Waiter] = []
  private var activeAdmissions: [UUID: ElectricLegacyBootstrapAdmission] = [:]
  private var admittedCount = 0
  private var completedCount = 0
  private var nonAdvancingCompletedCount = 0
  private var supersededCount = 0
  private var failedCount = 0
  private var cancelledCount = 0
  private var rejectedCount = 0
  private var exactCursorAdvancedCount = 0
  private var completedDecodedPayloadBytes = 0
  private let runtimeProvider: ElectricSyncRuntimeProvider

  public init(
    enabled: Bool = false,
    runtimeProvider: ElectricSyncRuntimeProvider = .live
  ) {
    self.isEnabled = enabled
    self.runtimeProvider = runtimeProvider
  }

  public func setEnabled(_ enabled: Bool) {
    guard isEnabled != enabled else { return }
    isEnabled = enabled

    guard enabled else {
      let rejectedWaiters = waiters
      waiters.removeAll()
      rejectedCount += rejectedWaiters.count
      for waiter in rejectedWaiters {
        waiter.continuation.resume(
          throwing: ElectricSyncError.legacyExactMissBootstrapDisabled
        )
      }
      return
    }

    admitNextIfPossible()
  }

  public func refreshEnabled(using currentValue: @Sendable () -> Bool) {
    setEnabled(currentValue())
  }

  public func acquire(
    identity: ElectricReplicaIdentity,
    legacyCursorCount: Int
  ) async throws -> ElectricLegacyBootstrapAdmission {
    try Task.checkCancellation()
    let waiterID = runtimeProvider.makeUUID()

    let admission = try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        enqueue(
          Waiter(
            id: waiterID,
            identity: identity,
            legacyCursorCount: legacyCursorCount,
            enqueuedAt: runtimeProvider.now(),
            continuation: continuation
          )
        )
      }
    } onCancel: {
      Task { await self.cancelWaiting(id: waiterID) }
    }

    do {
      try Task.checkCancellation()
      return admission
    } catch {
      finish(admission, outcome: .cancelled)
      throw error
    }
  }

  @discardableResult
  public func finish(
    _ admission: ElectricLegacyBootstrapAdmission,
    outcome: ElectricLegacyBootstrapOutcome
  ) -> ElectricLegacyBootstrapMetricsSnapshot {
    guard let activeAdmission = activeAdmissions.removeValue(forKey: admission.id) else {
      return metricsSnapshotValue()
    }
    completedDecodedPayloadBytes += activeAdmission.decodedPayloadBytes

    switch outcome {
    case .completed(let exactCursorAdvanced):
      completedCount += 1
      if exactCursorAdvanced {
        exactCursorAdvancedCount += 1
      }
    case .completedWithoutCursorAdvance:
      nonAdvancingCompletedCount += 1
    case .failed:
      failedCount += 1
    case .cancelled:
      cancelledCount += 1
    case .supersededByExactState:
      supersededCount += 1
    }

    admitNextIfPossible()
    return metricsSnapshotValue()
  }

  public func release(_ admission: ElectricLegacyBootstrapAdmission) {
    finish(admission, outcome: .completedWithoutCursorAdvance)
  }

  public func state() -> ElectricLegacyBootstrapAdmissionState {
    ElectricLegacyBootstrapAdmissionState(
      isEnabled: isEnabled,
      queued: waiters.count,
      inFlight: activeAdmissions.count
    )
  }

  public func metricsSnapshot() -> ElectricLegacyBootstrapMetricsSnapshot {
    metricsSnapshotValue()
  }

  private func enqueue(_ waiter: Waiter) {
    guard isEnabled else {
      rejectedCount += 1
      waiter.continuation.resume(
        throwing: ElectricSyncError.legacyExactMissBootstrapDisabled
      )
      return
    }

    guard activeAdmissions.isEmpty else {
      waiters.append(waiter)
      return
    }

    admit(waiter, wasQueued: false)
  }

  private func cancelWaiting(id: UUID) {
    guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
    let waiter = waiters.remove(at: index)
    cancelledCount += 1
    waiter.continuation.resume(throwing: CancellationError())
  }

  private func admitNextIfPossible() {
    guard isEnabled, activeAdmissions.isEmpty, !waiters.isEmpty else { return }
    admit(waiters.removeFirst(), wasQueued: true)
  }

  private func admit(_ waiter: Waiter, wasQueued: Bool) {
    let admission = ElectricLegacyBootstrapAdmission(
      id: waiter.id,
      metricsAccumulator: ElectricLegacyBootstrapMetricsAccumulator(),
      identity: waiter.identity,
      legacyCursorCount: waiter.legacyCursorCount,
      queueWaitMilliseconds: max(
        0, runtimeProvider.now().timeIntervalSince(waiter.enqueuedAt) * 1_000),
      wasQueued: wasQueued
    )
    activeAdmissions[waiter.id] = admission
    admittedCount += 1
    waiter.continuation.resume(returning: admission)
  }

  private func metricsSnapshotValue() -> ElectricLegacyBootstrapMetricsSnapshot {
    ElectricLegacyBootstrapMetricsSnapshot(
      isEnabled: isEnabled,
      queued: waiters.count,
      inFlight: activeAdmissions.count,
      admitted: admittedCount,
      completed: completedCount,
      nonAdvancingCompleted: nonAdvancingCompletedCount,
      superseded: supersededCount,
      failed: failedCount,
      cancelled: cancelledCount,
      rejected: rejectedCount,
      exactCursorAdvanced: exactCursorAdvancedCount,
      decodedPayloadBytes: completedDecodedPayloadBytes
        + activeAdmissions.values.reduce(0) { $0 + $1.decodedPayloadBytes }
    )
  }
}
