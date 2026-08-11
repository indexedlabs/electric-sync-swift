import Foundation
import Testing

@testable import ElectricSync

struct TracingTests {
  enum SampleError: Error {
    case boom
  }

  @Test
  func withElectricSyncSpanMarksFailureAndRecordsErrorType() throws {
    let tracer = RecordingTracer()

    do {
      _ = try withElectricSyncSpan(
        tracer: tracer,
        name: "electric.sync.failure_test",
        attributes: ["stage": "unit_test"]
      ) { _ in
        throw SampleError.boom
      }
      Issue.record("Expected failure from withElectricSyncSpan")
    } catch {}

    let span = try #require(tracer.firstCompleted())
    #expect(span.name == "electric.sync.failure_test")
    #expect(span.status == .failure)
    #expect(span.attributes["stage"] == "unit_test")
    #expect(span.attributes["error.type"]?.contains("SampleError") == true)
  }

  @Test
  func withElectricAsyncSpanMarksCancelledStatus() async throws {
    let tracer = RecordingTracer()

    do {
      _ = try await withElectricAsyncSpan(
        tracer: tracer,
        name: "electric.sync.cancel_test"
      ) { _ in
        throw CancellationError()
      }
      Issue.record("Expected cancellation from withElectricAsyncSpan")
    } catch is CancellationError {
      // expected
    } catch {
      Issue.record("Expected CancellationError, got \(error)")
    }

    let span = try #require(tracer.firstCompleted())
    #expect(span.name == "electric.sync.cancel_test")
    #expect(span.status == .cancelled)
    #expect(span.attributes["error.type"] == nil)
  }

  @Test
  func withElectricAsyncSpanMarksSuccessWhenOperationCompletes() async throws {
    let tracer = RecordingTracer()
    let output = try await withElectricAsyncSpan(
      tracer: tracer,
      name: "electric.sync.success_test",
      attributes: ["stage": "query_apply"]
    ) { _ in
      42
    }

    #expect(output == 42)
    let span = try #require(tracer.firstCompleted())
    #expect(span.name == "electric.sync.success_test")
    #expect(span.status == .success)
    #expect(span.attributes["stage"] == "query_apply")
  }

  @Test
  func withElectricAsyncSpanUsesTracerActiveContextForNestedSpans() async throws {
    let tracer = ActiveContextRecordingTracer()

    _ = try await withElectricAsyncSpan(
      tracer: tracer,
      name: "electric.parent",
      attributes: ["stage": "parent"]
    ) { _ in
      try await withElectricAsyncSpan(
        tracer: tracer,
        name: "electric.child",
        attributes: ["stage": "child"]
      ) { _ in
        "ok"
      }
    }

    let spans = tracer.completedSpans()
    let parent = try #require(spans.first(where: { $0.name == "electric.parent" }))
    let child = try #require(spans.first(where: { $0.name == "electric.child" }))
    #expect(child.parentID == parent.id)
    #expect(parent.parentID == nil)
  }

  @Test
  func electricMessageAttributesExposeControlAndPayloadSignals() {
    let messages = [
      ElectricMessage(
        payload: Data([0x01, 0x02]),
        offset: "1",
        handle: "handle-1",
        cursor: nil,
        isUpToDate: false,
        kind: .mutation,
        control: nil
      ),
      ElectricMessage(
        payload: Data(),
        offset: "2",
        handle: "handle-2",
        cursor: nil,
        isUpToDate: true,
        kind: .snapshot,
        control: .upToDate
      ),
      ElectricMessage(
        payload: Data(),
        offset: "3",
        handle: "handle-3",
        cursor: nil,
        isUpToDate: false,
        kind: .truncate,
        control: .mustRefetch
      ),
    ]

    let attributes = electricMessageAttributes(messages)
    #expect(attributes["message.count"] == "3")
    #expect(attributes["message.kind.mutation.count"] == "1")
    #expect(attributes["message.kind.snapshot.count"] == "1")
    #expect(attributes["message.kind.truncate.count"] == "1")
    #expect(attributes["message.control.up_to_date.count"] == "1")
    #expect(attributes["message.control.must_refetch.count"] == "1")
    #expect(attributes["has_must_refetch"] == "true")
    #expect(attributes["has_up_to_date"] == "true")
    #expect(attributes["message.payload.empty.count"] == "2")
  }
}

private struct CompletedSpan: Sendable {
  let name: String
  let attributes: [String: String]
  let status: ElectricSyncSpanStatus
}

private final class RecordingTracer: @unchecked Sendable, ElectricSyncTracer {
  private let lock = NSLock()
  private var spanNamesByID: [UUID: String] = [:]
  private var spanAttributesByID: [UUID: [String: String]] = [:]
  private var completedSpans: [CompletedSpan] = []

  func startSpan(name: String, attributes: [String: String]) -> any ElectricSyncSpan {
    let id = UUID()
    lock.lock()
    spanNamesByID[id] = name
    spanAttributesByID[id] = attributes
    lock.unlock()
    return RecordingSpan(id: id, recorder: self)
  }

  fileprivate func setAttribute(id: UUID, key: String, value: String) {
    lock.lock()
    defer { lock.unlock() }
    var attributes = spanAttributesByID[id] ?? [:]
    attributes[key] = value
    spanAttributesByID[id] = attributes
  }

  fileprivate func complete(id: UUID, status: ElectricSyncSpanStatus) {
    lock.lock()
    defer { lock.unlock() }
    let name = spanNamesByID[id] ?? "<unknown>"
    let attributes = spanAttributesByID[id] ?? [:]
    completedSpans.append(CompletedSpan(name: name, attributes: attributes, status: status))
    spanNamesByID.removeValue(forKey: id)
    spanAttributesByID.removeValue(forKey: id)
  }

  fileprivate func firstCompleted() -> CompletedSpan? {
    lock.lock()
    defer { lock.unlock() }
    return completedSpans.first
  }
}

private final class RecordingSpan: ElectricSyncSpan, @unchecked Sendable {
  private let id: UUID
  private let recorder: RecordingTracer
  private let lock = NSLock()
  private var hasEnded = false

  init(id: UUID, recorder: RecordingTracer) {
    self.id = id
    self.recorder = recorder
  }

  func setAttribute(key: String, value: String) {
    lock.lock()
    defer { lock.unlock() }
    guard !hasEnded else { return }
    recorder.setAttribute(id: id, key: key, value: value)
  }

  func end(status: ElectricSyncSpanStatus) {
    lock.lock()
    defer { lock.unlock() }
    guard !hasEnded else { return }
    hasEnded = true
    recorder.complete(id: id, status: status)
  }
}

private struct ActiveContextCompletedSpan: Sendable {
  let id: UUID
  let name: String
  let parentID: UUID?
  let status: ElectricSyncSpanStatus
}

private final class ActiveContextRecordingTracer: @unchecked Sendable, ElectricSyncTracer {
  private struct SpanState {
    let id: UUID
    let name: String
    let parentID: UUID?
  }

  private let lock = NSLock()
  private var activeStack: [UUID] = []
  private var spanStates: [UUID: SpanState] = [:]
  private var completed: [ActiveContextCompletedSpan] = []

  func startSpan(name: String, attributes _: [String: String]) -> any ElectricSyncSpan {
    let id = UUID()
    lock.lock()
    let parentID = activeStack.last
    spanStates[id] = SpanState(id: id, name: name, parentID: parentID)
    lock.unlock()
    return ActiveContextSpan(id: id, recorder: self)
  }

  func withSpan<T>(
    name: String,
    attributes: [String: String],
    operation: (_ span: any ElectricSyncSpan) throws -> T
  ) throws -> T {
    let span = startSpan(name: name, attributes: attributes)
    guard let activeSpan = span as? ActiveContextSpan else {
      return try operation(span)
    }

    pushActive(id: activeSpan.id)
    do {
      let output = try operation(activeSpan)
      activeSpan.end(status: .success)
      popActive(id: activeSpan.id)
      return output
    } catch is CancellationError {
      activeSpan.end(status: .cancelled)
      popActive(id: activeSpan.id)
      throw CancellationError()
    } catch {
      activeSpan.end(status: .failure)
      popActive(id: activeSpan.id)
      throw error
    }
  }

  func withAsyncSpan<T: Sendable>(
    name: String,
    attributes: [String: String],
    isolation _: isolated (any Actor)?,
    operation: (_ span: any ElectricSyncSpan) async throws -> T
  ) async throws -> T {
    let span = startSpan(name: name, attributes: attributes)
    guard let activeSpan = span as? ActiveContextSpan else {
      return try await operation(span)
    }

    pushActive(id: activeSpan.id)
    do {
      let output = try await operation(activeSpan)
      activeSpan.end(status: .success)
      popActive(id: activeSpan.id)
      return output
    } catch is CancellationError {
      activeSpan.end(status: .cancelled)
      popActive(id: activeSpan.id)
      throw CancellationError()
    } catch {
      activeSpan.end(status: .failure)
      popActive(id: activeSpan.id)
      throw error
    }
  }

  private func pushActive(id: UUID) {
    lock.lock()
    activeStack.append(id)
    lock.unlock()
  }

  private func popActive(id: UUID) {
    lock.lock()
    if activeStack.last == id {
      activeStack.removeLast()
    } else if let index = activeStack.lastIndex(of: id) {
      activeStack.remove(at: index)
    }
    lock.unlock()
  }

  fileprivate func complete(id: UUID, status: ElectricSyncSpanStatus) {
    lock.lock()
    defer { lock.unlock() }
    guard let state = spanStates[id] else { return }
    completed.append(
      ActiveContextCompletedSpan(
        id: state.id,
        name: state.name,
        parentID: state.parentID,
        status: status
      )
    )
    spanStates.removeValue(forKey: id)
  }

  fileprivate func completedSpans() -> [ActiveContextCompletedSpan] {
    lock.lock()
    defer { lock.unlock() }
    return completed
  }
}

private final class ActiveContextSpan: ElectricSyncSpan, @unchecked Sendable {
  fileprivate let id: UUID
  private let recorder: ActiveContextRecordingTracer
  private let lock = NSLock()
  private var hasEnded = false

  init(id: UUID, recorder: ActiveContextRecordingTracer) {
    self.id = id
    self.recorder = recorder
  }

  func setAttribute(key _: String, value _: String) {}

  func end(status: ElectricSyncSpanStatus) {
    lock.lock()
    defer { lock.unlock() }
    guard !hasEnded else { return }
    hasEnded = true
    recorder.complete(id: id, status: status)
  }
}
