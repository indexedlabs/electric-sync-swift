import Foundation

/// Relays cancellation from a KeepSynced owner to the unstructured task that
/// produces its subscription stream. The owner can be cancelled before the
/// producer installs itself, so remember cancellation and apply it at install.
final class ElectricSubscriptionCancellationRelay: @unchecked Sendable {
  private let lock = NSLock()
  private var producerTask: Task<Void, Error>?
  private var isCancelled = false
  private var hasFinished = false
  private var finishWaiters: [CheckedContinuation<Void, Never>] = []

  func install(producerTask: Task<Void, Error>) {
    let shouldCancel = lock.withLock {
      guard !hasFinished else { return false }
      self.producerTask = producerTask
      return isCancelled
    }
    if shouldCancel {
      producerTask.cancel()
    }
  }

  func cancel() {
    let task = lock.withLock {
      isCancelled = true
      return producerTask
    }
    task?.cancel()
  }

  func finish() {
    let waiters = lock.withLock {
      hasFinished = true
      producerTask = nil
      let waiters = finishWaiters
      finishWaiters.removeAll()
      return waiters
    }
    for waiter in waiters {
      waiter.resume()
    }
  }

  var hasInstalledProducerTask: Bool {
    lock.withLock { producerTask != nil }
  }

  func waitForProducerTermination() async {
    guard !lock.withLock({ hasFinished }) else { return }
    await withCheckedContinuation { continuation in
      let shouldResume = lock.withLock {
        guard !hasFinished else { return true }
        finishWaiters.append(continuation)
        return false
      }
      if shouldResume {
        continuation.resume()
      }
    }
  }
}

extension ElectricCollection {
  /// Keep this collection's changes stream running while the returned token is retained.
  ///
  /// This is intentionally stream-identity keyed (not predicate keyed): the underlying stream is shared for
  /// compatible collections, so starting multiple streams would duplicate network work and race on metadata offsets.
  ///
  /// - Important: `circuitBreaker` is only applied when the underlying stream is first started.
  @discardableResult
  public func keepSynced(
    circuitBreaker: (any CircuitBreakerStrategy)? = nil
  ) -> ElectricCollectionStreamToken {
    guard let session = replica.client.sessionProvider.captureAuthenticatedSession() else {
      return ElectricCollectionStreamToken(onCancel: {})
    }
    return keepSynced(
      session: session,
      circuitBreaker: circuitBreaker
    )
  }

  @discardableResult
  public func keepSynced(
    session: ElectricSyncSession?,
    circuitBreaker: (any CircuitBreakerStrategy)? = nil
  ) -> ElectricCollectionStreamToken {
    guard let session else {
      return ElectricCollectionStreamToken(onCancel: {})
    }

    let streamSyncMode = configuration.syncMode
    return replica.acquireStream(syncMode: streamSyncMode) { [self] in
      Task.detached(priority: .high) {
        let cancellationRelay = ElectricSubscriptionCancellationRelay()
        let stream = subscribe(
          session: session,
          circuitBreaker: circuitBreaker,
          protocolSyncMode: streamSyncMode,
          cancellationRelay: cancellationRelay
        )
        await withTaskCancellationHandler {
          for await _ in stream {}
        } onCancel: {
          cancellationRelay.cancel()
        }
        await cancellationRelay.waitForProducerTermination()
      }
    }
  }

  @discardableResult
  public func keepSynced(
    manager _: ElectricCollectionStreamManager,
    session: ElectricSyncSession?,
    circuitBreaker: (any CircuitBreakerStrategy)? = nil
  ) -> ElectricCollectionStreamToken {
    keepSynced(
      session: session,
      circuitBreaker: circuitBreaker
    )
  }

  @discardableResult
  public func keepSynced(
    manager _: ElectricCollectionStreamManager,
    circuitBreaker: (any CircuitBreakerStrategy)? = nil
  ) -> ElectricCollectionStreamToken {
    guard let session = replica.client.sessionProvider.captureAuthenticatedSession() else {
      return ElectricCollectionStreamToken(onCancel: {})
    }
    return keepSynced(
      session: session,
      circuitBreaker: circuitBreaker
    )
  }
}
