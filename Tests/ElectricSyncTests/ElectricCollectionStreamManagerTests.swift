import Foundation
import Testing

@testable import ElectricSync

private final class LockedCounter: @unchecked Sendable {
  private let lock = NSLock()
  private var value: Int = 0
  private var waiters: [(minimum: Int, continuation: CheckedContinuation<Void, Never>)] = []

  func increment() {
    lock.lock()
    value += 1
    let readyWaiters = waiters.filter { value >= $0.minimum }
    waiters.removeAll { value >= $0.minimum }
    lock.unlock()

    for waiter in readyWaiters {
      waiter.continuation.resume()
    }
  }

  func read() -> Int {
    lock.lock()
    defer { lock.unlock() }
    return value
  }

  func wait(until minimum: Int) async {
    await withCheckedContinuation { continuation in
      lock.lock()
      guard value < minimum else {
        lock.unlock()
        continuation.resume()
        return
      }
      waiters.append((minimum, continuation))
      lock.unlock()
    }
  }
}

private final class DummyClient {}

private final class ControlledSleepRequest: @unchecked Sendable {
  private enum State {
    case pending
    case waiting(CheckedContinuation<Void, any Error>)
    case completed(Result<Void, any Error>)
  }

  let duration: Duration

  private let lock = NSLock()
  private let finished = LockedCounter()
  private var state: State = .pending

  init(duration: Duration) {
    self.duration = duration
  }

  func resume() {
    complete(with: .success(()))
  }

  func cancel() {
    complete(with: .failure(CancellationError()))
  }

  func waitUntilFinished() async {
    await finished.wait(until: 1)
  }

  fileprivate func suspend() async throws {
    defer { finished.increment() }
    try await withCheckedThrowingContinuation { continuation in
      lock.lock()
      switch state {
      case .pending:
        state = .waiting(continuation)
        lock.unlock()
      case .completed(let result):
        lock.unlock()
        continuation.resume(with: result)
      case .waiting:
        lock.unlock()
        Issue.record("A controlled sleep request cannot be suspended twice")
        continuation.resume(throwing: CancellationError())
      }
    }
  }

  private func complete(with result: Result<Void, any Error>) {
    lock.lock()
    switch state {
    case .pending:
      state = .completed(result)
      lock.unlock()
    case .waiting(let continuation):
      state = .completed(result)
      lock.unlock()
      continuation.resume(with: result)
    case .completed:
      lock.unlock()
    }
  }
}

private actor ControlledSleepRequestQueue {
  private var requests: [ControlledSleepRequest] = []
  private var waiters: [CheckedContinuation<ControlledSleepRequest, Never>] = []

  func enqueue(_ request: ControlledSleepRequest) {
    guard !waiters.isEmpty else {
      requests.append(request)
      return
    }
    waiters.removeFirst().resume(returning: request)
  }

  func next() async -> ControlledSleepRequest {
    if !requests.isEmpty {
      return requests.removeFirst()
    }
    return await withCheckedContinuation { continuation in
      waiters.append(continuation)
    }
  }
}

private final class ControlledAppClock: @unchecked Sendable {
  private let requests = ControlledSleepRequestQueue()

  var runtimeProvider: ElectricSyncRuntimeProvider {
    ElectricSyncRuntimeProvider(
      now: Date.init,
      makeUUID: UUID.init,
      sleep: { [self] duration in try await sleep(for: duration) }
    )
  }

  func nextSleep() async -> ControlledSleepRequest {
    await requests.next()
  }

  private func sleep(for duration: Duration) async throws {
    let request = ControlledSleepRequest(duration: duration)
    try await withTaskCancellationHandler(
      operation: {
        await requests.enqueue(request)
        try await request.suspend()
      },
      onCancel: {
        request.cancel()
      }
    )
  }
}

private func cancellationTrackedTask(
  _ cancels: LockedCounter,
  started: LockedCounter
) -> Task<Void, Never> {
  Task {
    await withTaskCancellationHandler(
      operation: {
        started.increment()
        try? await Task.sleep(for: .seconds(3_600))
      },
      onCancel: {
        cancels.increment()
      }
    )
  }
}

struct ElectricCollectionStreamManagerTests {
  @Test
  func logsWhenDistinctStreamOwnersSharePersistedCursorKey() throws {
    let diagnostics = ElectricCursorOwnershipDiagnostics()
    let manager = ElectricCollectionStreamManager(
      gcTime: 0,
      cursorOwnershipDiagnostics: diagnostics
    )
    let telemetry = RecordingElectricCursorTelemetry()
    let starts = LockedCounter()
    let firstClient = DummyClient()
    let secondClient = DummyClient()
    let persistedCursorKey = ElectricSyncClientImpl.syncStateKey(
      collectionIdentifier: "records",
      predicate: nil
    )

    let firstToken = manager.acquire(
      key: .init(tableName: "records", clientId: ObjectIdentifier(firstClient)),
      persistedCursorKeys: [persistedCursorKey],
      collectionIdentifier: "records",
      logger: telemetry,
      tracer: telemetry
    ) {
      starts.increment()
      return Task {}
    }
    let secondToken = manager.acquire(
      key: .init(tableName: "records", clientId: ObjectIdentifier(secondClient)),
      persistedCursorKeys: [persistedCursorKey],
      collectionIdentifier: "records",
      logger: telemetry,
      tracer: telemetry
    ) {
      starts.increment()
      return Task {}
    }

    #expect(starts.read() == 2)
    #expect(telemetry.recordedLogs().count == 1)
    let log = try #require(telemetry.recordedLogs().first)
    #expect(log.level == LogLevel.warning.rawValue)
    #expect(
      log.metadata[ElectricCursorOwnershipDiagnostics.collisionTypeAttribute]
        == "duplicate_stream_owners"
    )
    #expect(log.metadata[ElectricCursorOwnershipDiagnostics.ownerCountAttribute] == "2")
    #expect(
      log.metadata[ElectricCursorOwnershipDiagnostics.persistedCursorKeyAttribute]
        == persistedCursorKey
    )

    #expect(telemetry.recordedSpans().count == 1)
    let span = try #require(telemetry.recordedSpans().first)
    #expect(span.name == "electric.stream_owner.collision")
    #expect(span.attributes[ElectricCursorOwnershipDiagnostics.ownerCountAttribute] == "2")
    #expect(span.status == .failure)

    firstToken.cancel()
    secondToken.cancel()
  }

  @Test(.timeLimit(.minutes(1)))
  func startsOncePerKeyAndRefCountsSubscribers() async throws {
    let manager = ElectricCollectionStreamManager(gcTime: 0)
    let starts = LockedCounter()
    let taskStarts = LockedCounter()
    let cancels = LockedCounter()
    let key = ElectricCollectionStreamManager.Key(tableName: "records")

    let token1 = manager.acquire(key: key) {
      starts.increment()
      return cancellationTrackedTask(cancels, started: taskStarts)
    }

    let token2 = manager.acquire(key: key) {
      Issue.record("Stream should not start twice for the same key")
      return Task {}
    }

    #expect(starts.read() == 1)
    await taskStarts.wait(until: 1)

    token1.cancel()
    #expect(cancels.read() == 0)

    token2.cancel()
    await cancels.wait(until: 1)
    #expect(cancels.read() == 1)
  }

  @Test(.timeLimit(.minutes(1)))
  func cancelsAfterGCTimeWhenLastSubscriberLeaves() async throws {
    let clock = ControlledAppClock()

    do {
      let manager = ElectricCollectionStreamManager(runtimeProvider: clock.runtimeProvider)
      let taskStarts = LockedCounter()
      let cancels = LockedCounter()
      let key = ElectricCollectionStreamManager.Key(tableName: "spaces")

      let token = manager.acquire(key: key) {
        cancellationTrackedTask(cancels, started: taskStarts)
      }

      #expect(ElectricCollectionStreamManager.defaultGCTime == 5.0)
      await taskStarts.wait(until: 1)
      token.cancel()

      let gcSleep = await clock.nextSleep()
      #expect(gcSleep.duration == .seconds(5))
      #expect(cancels.read() == 0)

      gcSleep.resume()
      await gcSleep.waitUntilFinished()
      await cancels.wait(until: 1)
      #expect(cancels.read() == 1)
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func reacquireCancelsPendingGCTimer() async throws {
    let clock = ControlledAppClock()

    do {
      let manager = ElectricCollectionStreamManager(runtimeProvider: clock.runtimeProvider)
      let taskStarts = LockedCounter()
      let cancels = LockedCounter()
      let key = ElectricCollectionStreamManager.Key(tableName: "messages")

      let token1 = manager.acquire(key: key) {
        cancellationTrackedTask(cancels, started: taskStarts)
      }

      await taskStarts.wait(until: 1)
      token1.cancel()
      let firstGCSleep = await clock.nextSleep()
      #expect(firstGCSleep.duration == .seconds(5))
      #expect(cancels.read() == 0)

      let token2 = manager.acquire(key: key) {
        Issue.record("Stream should not be restarted when reacquiring before gcTime")
        return Task {}
      }

      await firstGCSleep.waitUntilFinished()
      #expect(cancels.read() == 0)

      token2.cancel()
      let secondGCSleep = await clock.nextSleep()
      #expect(secondGCSleep.duration == .seconds(5))
      #expect(cancels.read() == 0)

      secondGCSleep.resume()
      await secondGCSleep.waitUntilFinished()
      await cancels.wait(until: 1)
      #expect(cancels.read() == 1)
    }
  }

  @Test
  func startsSeparateStreamsForDifferentBasePredicates() async throws {
    let manager = ElectricCollectionStreamManager(gcTime: 0)
    let starts = LockedCounter()
    let client = DummyClient()
    let basePredicate1 = SQLExpression("status = 'open'")
    let basePredicate2 = SQLExpression("status = 'closed'")

    let key1 = ElectricCollectionStreamManager.Key(
      tableName: "records",
      basePredicate: basePredicate1,
      clientId: ObjectIdentifier(client)
    )
    let key2 = ElectricCollectionStreamManager.Key(
      tableName: "records",
      basePredicate: basePredicate2,
      clientId: ObjectIdentifier(client)
    )

    let token1 = manager.acquire(key: key1) {
      starts.increment()
      return Task.detached {}
    }

    let token2 = manager.acquire(key: key2) {
      starts.increment()
      return Task.detached {}
    }

    #expect(starts.read() == 2)
    token1.cancel()
    token2.cancel()
  }

  @Test(.timeLimit(.minutes(1)))
  func cancelAllCancelsTrackedStreamsImmediately() async throws {
    let manager = ElectricCollectionStreamManager(gcTime: 5)
    let taskStarts = LockedCounter()
    let cancels = LockedCounter()

    let token1 = manager.acquire(key: .init(tableName: "spaces")) {
      cancellationTrackedTask(cancels, started: taskStarts)
    }

    let token2 = manager.acquire(key: .init(tableName: "records")) {
      cancellationTrackedTask(cancels, started: taskStarts)
    }

    await taskStarts.wait(until: 2)
    manager.cancelAll()
    await cancels.wait(until: 2)

    #expect(cancels.read() == 2)

    token1.cancel()
    token2.cancel()
  }
}
