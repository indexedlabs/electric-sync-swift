import Foundation

public enum ElectricSyncSpanStatus: String, Sendable {
  case success
  case failure
  case cancelled
}

public protocol ElectricSyncSpan: Sendable {
  func setAttribute(key: String, value: String)
  func end(status: ElectricSyncSpanStatus)
}

public protocol ElectricSyncTracer: Sendable {
  func startSpan(name: String, attributes: [String: String]) -> any ElectricSyncSpan
  func withSpan<T>(
    name: String,
    attributes: [String: String],
    operation: (_ span: any ElectricSyncSpan) throws -> T
  ) throws -> T
  func withAsyncSpan<T: Sendable>(
    name: String,
    attributes: [String: String],
    isolation: isolated (any Actor)?,
    operation: (_ span: any ElectricSyncSpan) async throws -> T
  ) async throws -> T
}

public struct NoopElectricSyncSpan: ElectricSyncSpan {
  public init() {}
  public func setAttribute(key _: String, value _: String) {}
  public func end(status _: ElectricSyncSpanStatus) {}
}

public struct NoopElectricSyncTracer: ElectricSyncTracer {
  public init() {}

  public func startSpan(name _: String, attributes _: [String: String]) -> any ElectricSyncSpan {
    NoopElectricSyncSpan()
  }
}

public extension ElectricSyncTracer {
  @discardableResult
  func withSpan<T>(
    name: String,
    attributes: [String: String] = [:],
    operation: (_ span: any ElectricSyncSpan) throws -> T
  ) throws -> T {
    let span = startSpan(name: name, attributes: attributes)
    do {
      let output = try operation(span)
      span.end(status: .success)
      return output
    } catch is CancellationError {
      span.end(status: .cancelled)
      throw CancellationError()
    } catch {
      span.setAttribute(key: "error.type", value: String(reflecting: type(of: error)))
      span.end(status: .failure)
      throw error
    }
  }

  @discardableResult
  @preconcurrency
  func withAsyncSpan<T: Sendable>(
    name: String,
    attributes: [String: String] = [:],
    isolation: isolated (any Actor)? = #isolation,
    operation: (_ span: any ElectricSyncSpan) async throws -> T
  ) async throws -> T {
    _ = isolation
    let span = startSpan(name: name, attributes: attributes)
    do {
      let output = try await operation(span)
      span.end(status: .success)
      return output
    } catch is CancellationError {
      span.end(status: .cancelled)
      throw CancellationError()
    } catch {
      span.setAttribute(key: "error.type", value: String(reflecting: type(of: error)))
      span.end(status: .failure)
      throw error
    }
  }
}

@discardableResult
internal func withElectricSyncSpan<T>(
  tracer: any ElectricSyncTracer,
  name: String,
  attributes: [String: String] = [:],
  operation: (_ span: any ElectricSyncSpan) throws -> T
) throws -> T {
  try tracer.withSpan(name: name, attributes: attributes, operation: operation)
}

@discardableResult
internal func withElectricAsyncSpan<T: Sendable>(
  tracer: any ElectricSyncTracer,
  name: String,
  attributes: [String: String] = [:],
  isolation: isolated (any Actor)? = #isolation,
  operation: (_ span: any ElectricSyncSpan) async throws -> T
) async throws -> T {
  _ = isolation
  return try await tracer.withAsyncSpan(
    name: name,
    attributes: attributes,
    isolation: isolation,
    operation: operation
  )
}

internal func electricThreadIsMainValue() -> String {
  Thread.isMainThread ? "true" : "false"
}

internal func electricSyncModeLabel(_ mode: ElectricCollectionSyncMode) -> String {
  switch mode {
  case .eager:
    return "eager"
  case .onDemand:
    return "on_demand"
  case .progressive:
    return "progressive"
  }
}

internal func electricTransportLabel(_ transport: ElectricLiveTransport) -> String {
  switch transport {
  case .longPoll:
    return "long_poll"
  case .sse:
    return "sse"
  case .sseWithFallback:
    return "sse_with_fallback"
  }
}

internal func electricMessageAttributes(_ messages: [ElectricMessage]) -> [String: String] {
  var mutationCount = 0
  var snapshotCount = 0
  var truncateCount = 0
  var upToDateCount = 0
  var snapshotEndCount = 0
  var subsetEndCount = 0
  var mustRefetchCount = 0
  var emptyPayloadCount = 0
  var payloadBytes = 0

  for message in messages {
    switch message.kind {
    case .mutation:
      mutationCount += 1
    case .snapshot:
      snapshotCount += 1
    case .truncate:
      truncateCount += 1
    }

    if message.payload.isEmpty {
      emptyPayloadCount += 1
    }
    payloadBytes += message.payload.count

    let isUpToDate = message.control == .upToDate || (message.control == nil && message.isUpToDate)
    if isUpToDate {
      upToDateCount += 1
    }

    if let control = message.control {
      switch control {
      case .upToDate:
        break
      case .snapshotEnd:
        snapshotEndCount += 1
      case .subsetEnd:
        subsetEndCount += 1
      case .mustRefetch:
        mustRefetchCount += 1
      }
    }
  }

  return [
    "message.count": "\(messages.count)",
    "message.kind.mutation.count": "\(mutationCount)",
    "message.kind.snapshot.count": "\(snapshotCount)",
    "message.kind.truncate.count": "\(truncateCount)",
    "message.control.up_to_date.count": "\(upToDateCount)",
    "message.control.snapshot_end.count": "\(snapshotEndCount)",
    "message.control.subset_end.count": "\(subsetEndCount)",
    "message.control.must_refetch.count": "\(mustRefetchCount)",
    "message.payload.bytes": "\(payloadBytes)",
    "message.payload.empty.count": "\(emptyPayloadCount)",
    "has_truncate": "\(truncateCount > 0)",
    "has_must_refetch": "\(mustRefetchCount > 0)",
    "has_up_to_date": "\(upToDateCount > 0)",
    "has_snapshot_end": "\(snapshotEndCount > 0)",
    "has_subset_end": "\(subsetEndCount > 0)",
  ]
}

internal func mergeTraceAttributes(
  _ base: [String: String],
  _ extra: [String: String]
) -> [String: String] {
  base.merging(extra) { _, new in new }
}
