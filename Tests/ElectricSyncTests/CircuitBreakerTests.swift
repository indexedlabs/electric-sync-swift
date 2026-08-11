import Foundation
import Testing

@testable import ElectricSync

struct CircuitBreakerTests {
  @Test
  func opensAfterThresholdAndUsesJitterlessBackoff() async throws {
    var breaker = ExponentialBackoffCircuitBreaker(
      baseDelay: 0.5,
      maxDelay: 4,
      failureThreshold: 2,
      jitterRange: 1.0...1.0  // deterministic for test
    )

    let t0 = Date(timeIntervalSince1970: 0)

    // First failure stays closed but returns base delay.
    let firstDelay = breaker.recordFailure(now: t0, reason: "test")
    #expect(abs(firstDelay - 0.5) < 0.001)
    #if DEBUG
      #expect(breaker.stateForTesting == .closed)
      #expect(breaker.consecutiveFailuresForTesting == 1)
    #endif

    // Second failure crosses threshold and opens.
    let secondDelay = breaker.recordFailure(now: t0, reason: "test")
    #expect(abs(secondDelay - 1.0) < 0.001)
    #if DEBUG
      if case .open(let until) = breaker.stateForTesting {
        #expect(abs(until.timeIntervalSince(t0) - 1.0) < 0.001)
      } else {
        Issue.record("Expected breaker to be open after crossing threshold")
      }
    #endif

    // While open, preflight should tell us to wait ~1s.
    let wait = breaker.preflightDelay(now: t0.addingTimeInterval(0.2))
    #expect(wait != nil && abs(wait! - 0.8) < 0.05)

    // After the window, it transitions to half-open, allowing a probe.
    let afterOpen = breaker.preflightDelay(now: t0.addingTimeInterval(1.1))
    #expect(afterOpen == nil)
  }

  @Test
  func resetClearsStateForLifecycleRestart() async throws {
    var breaker = ExponentialBackoffCircuitBreaker(
      baseDelay: 0.5,
      maxDelay: 4,
      failureThreshold: 1,
      jitterRange: 1.0...1.0
    )

    let t0 = Date(timeIntervalSince1970: 0)
    _ = breaker.recordFailure(now: t0, reason: "test")  // opens immediately because threshold=1

    // Simulate lifecycle reset (app open/close) via signal + reset.
    breaker.reset()

    #if DEBUG
      #expect(breaker.stateForTesting == .closed)
      #expect(breaker.consecutiveFailuresForTesting == 0)
    #endif

    let wait = breaker.preflightDelay(now: t0.addingTimeInterval(1))
    #expect(wait == nil)  // should not be blocked after lifecycle reset
  }
}
