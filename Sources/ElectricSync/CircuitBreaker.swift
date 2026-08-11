import Foundation

/// Strategy interface so host apps can plug in their own backoff / circuit logic.
public protocol CircuitBreakerStrategy: Sendable {
  mutating func preflightDelay(now: Date) -> TimeInterval?
  mutating func recordSuccess()
  mutating func recordFailure(now: Date, reason: String?) -> TimeInterval
  mutating func reset()
}

/// Default exponential backoff implementation with optional jitter.
public struct ExponentialBackoffCircuitBreaker: CircuitBreakerStrategy {
  public enum State: Equatable, Sendable {
    case closed
    case halfOpen
    case open(until: Date)
  }

  private var state: State = .closed
  private var consecutiveFailures: Int = 0
  private var lastErrorReason: String?
  private var lastErrorTimestamp: Date?
  private var repeatedErrorCount: Int = 0
  private let baseDelay: TimeInterval
  private let maxDelay: TimeInterval
  private let failureThreshold: Int
  private let jitterRange: ClosedRange<Double>
  private let loopWindow: TimeInterval = 5  // seconds window to detect rapid repeated identical failures
  private let loopThreshold: Int = 5  // after this many repeats, force open state

  public init(
    baseDelay: TimeInterval = 0.5,
    maxDelay: TimeInterval = 30,
    failureThreshold: Int = 3,
    jitterRange: ClosedRange<Double> = 0.5...1.5
  ) {
    self.baseDelay = baseDelay
    self.maxDelay = maxDelay
    self.failureThreshold = max(1, failureThreshold)
    self.jitterRange = jitterRange
  }

  public mutating func preflightDelay(now: Date) -> TimeInterval? {
    switch state {
    case .closed, .halfOpen:
      return nil
    case .open(let until):
      if now < until {
        return until.timeIntervalSince(now)
      }
      state = .halfOpen
      return nil
    }
  }

  public mutating func recordSuccess() {
    state = .closed
    consecutiveFailures = 0
  }

  public mutating func recordFailure(now: Date, reason: String? = nil) -> TimeInterval {
    consecutiveFailures += 1

    let delay = nextDelay()

    // Detect tight failure loops (e.g., 409 cache revalidation storms)
    let currentReason = reason ?? "unknown"

    if let lastReason = lastErrorReason,
      let lastTimestamp = lastErrorTimestamp,
      now.timeIntervalSince(lastTimestamp) <= loopWindow,
      lastReason == currentReason
    {
      repeatedErrorCount += 1
    } else {
      repeatedErrorCount = 1
    }

    lastErrorTimestamp = now
    lastErrorReason = currentReason

    if repeatedErrorCount >= loopThreshold {
      // Force-open breaker for maxDelay to stop the storm
      state = .open(until: now.addingTimeInterval(maxDelay))
      return maxDelay
    }

    if state == .halfOpen || consecutiveFailures >= failureThreshold {
      state = .open(until: now.addingTimeInterval(delay))
    }

    return delay
  }

  public mutating func reset() {
    state = .closed
    consecutiveFailures = 0
    repeatedErrorCount = 0
    lastErrorReason = nil
    lastErrorTimestamp = nil
  }

  #if DEBUG
    public var stateForTesting: State { state }
    public var consecutiveFailuresForTesting: Int { consecutiveFailures }
  #endif

  private func nextDelay() -> TimeInterval {
    let exponent = max(0, consecutiveFailures - 1)
    let backoff = min(maxDelay, baseDelay * pow(2.0, Double(exponent)))
    let jitter = Double.random(in: jitterRange)
    return min(maxDelay, backoff * jitter)
  }
}
