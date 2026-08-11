import Foundation
import Testing

@testable import ElectricSync

struct DeduplicatedLoadSubsetTests {
  @Test
  func deduplicatesUnlimitedSubsetRequests() async throws {
    let recorder = CallRecorder()
    let dedupe = DeduplicatedLoadSubset { options in
      await recorder.record(options)
    }

    let first = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(10))
    )
    let second = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(20))
    )

    #expect(try await dedupe.loadSubset(first) == false)
    #expect(await recorder.count() == 1)

    #expect(try await dedupe.loadSubset(second) == true)
    #expect(await recorder.count() == 1)
  }

  @Test
  func doesNotDeduplicateLimitedCallsWithDifferentWhereClauses() async throws {
    let recorder = CallRecorder()
    let dedupe = DeduplicatedLoadSubset { options in
      await recorder.record(options)
    }

    let orderBy = [OrderBy(field: "age", direction: .ascending)]

    let first = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(10)),
      orderBy: orderBy,
      limit: 10
    )
    let second = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(20)),
      orderBy: orderBy,
      limit: 5
    )

    #expect(try await dedupe.loadSubset(first) == false)
    #expect(try await dedupe.loadSubset(second) == false)
    #expect(await recorder.count() == 2)
  }

  @Test
  func deduplicatesLimitedCallsWhenWhereIsEqualAndLimitIsSmaller() async throws {
    let recorder = CallRecorder()
    let dedupe = DeduplicatedLoadSubset { options in
      await recorder.record(options)
    }

    let orderBy = [OrderBy(field: "age", direction: .ascending)]
    let wherePredicate = SyncPredicateExpression.comparison(
      field: "age",
      op: .greaterThan,
      value: .int(10)
    )

    let first = LoadSubsetOptions(wherePredicate: wherePredicate, orderBy: orderBy, limit: 10)
    let second = LoadSubsetOptions(wherePredicate: wherePredicate, orderBy: orderBy, limit: 5)

    #expect(try await dedupe.loadSubset(first) == false)
    #expect(await recorder.count() == 1)
    #expect(try await dedupe.loadSubset(second) == true)
    #expect(await recorder.count() == 1)
  }

  @Test
  func deduplicatesLimitedCallsCoveredByUnlimitedSuperset() async throws {
    let recorder = CallRecorder()
    let dedupe = DeduplicatedLoadSubset { options in
      await recorder.record(options)
    }

    let unlimited = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(10))
    )
    let limited = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(20)),
      orderBy: [OrderBy(field: "age", direction: .ascending)],
      limit: 10
    )

    #expect(try await dedupe.loadSubset(unlimited) == false)
    #expect(await recorder.count() == 1)

    #expect(try await dedupe.loadSubset(limited) == true)
    #expect(await recorder.count() == 1)
  }

  @Test
  func subtractsAlreadyLoadedUnlimitedPredicates() async throws {
    let recorder = CallRecorder()
    let dedupe = DeduplicatedLoadSubset { options in
      await recorder.record(options)
    }

    let alreadyLoaded = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(20))
    )
    let broader = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(10))
    )

    _ = try await dedupe.loadSubset(alreadyLoaded)
    _ = try await dedupe.loadSubset(broader)

    #expect(await recorder.count() == 2)
    let second = await recorder.calls()[1]
    #expect(
      second.wherePredicate
        == .and([
          .comparison(field: "age", op: .greaterThan, value: .int(10)),
          .comparison(field: "age", op: .lessThanOrEqual, value: .int(20)),
        ])
    )
  }

  @Test
  func resetClearsLoadedState() async throws {
    let recorder = CallRecorder()
    let dedupe = DeduplicatedLoadSubset { options in
      await recorder.record(options)
    }

    let options = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(10))
    )

    _ = try await dedupe.loadSubset(options)
    #expect(try await dedupe.loadSubset(options) == true)
    #expect(await recorder.count() == 1)

    await dedupe.reset()

    _ = try await dedupe.loadSubset(options)
    #expect(await recorder.count() == 2)
  }

  @Test
  func waitsForInflightSupersetRatherThanStartingDuplicateFetch() async throws {
    let recorder = CallRecorder()
    let gate = AsyncGate()

    let dedupe = DeduplicatedLoadSubset { options in
      await recorder.record(options)
      if await recorder.count() == 1 {
        await gate.wait()
      }
    }

    let superset = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(10))
    )
    let subset = LoadSubsetOptions(
      wherePredicate: .comparison(field: "age", op: .greaterThan, value: .int(20))
    )

    let supersetTask = Task { try await dedupe.loadSubset(superset) }
    await recorder.waitForCount(1)

    let subsetTask = Task { try await dedupe.loadSubset(subset) }
    #expect(await recorder.count() == 1)

    await gate.open()

    #expect(try await supersetTask.value == false)
    #expect(try await subsetTask.value == true)
    #expect(await recorder.count() == 1)
  }
}

private actor CallRecorder {
  private var recorded: [LoadSubsetOptions] = []
  private var waiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

  func record(_ options: LoadSubsetOptions) {
    recorded.append(options)
    notifyWaitersIfNeeded()
  }

  func count() -> Int {
    recorded.count
  }

  func calls() -> [LoadSubsetOptions] {
    recorded
  }

  func waitForCount(_ count: Int) async {
    if recorded.count >= count { return }
    await withCheckedContinuation { continuation in
      waiters.append((count: count, continuation: continuation))
    }
  }

  private func notifyWaitersIfNeeded() {
    guard !waiters.isEmpty else { return }
    let currentCount = recorded.count
    var remaining: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
    for waiter in waiters {
      if currentCount >= waiter.count {
        waiter.continuation.resume()
      } else {
        remaining.append(waiter)
      }
    }
    waiters = remaining
  }
}

private actor AsyncGate {
  private var continuation: CheckedContinuation<Void, Never>?

  func wait() async {
    await withCheckedContinuation { continuation in
      self.continuation = continuation
    }
  }

  func open() {
    continuation?.resume()
    continuation = nil
  }
}
