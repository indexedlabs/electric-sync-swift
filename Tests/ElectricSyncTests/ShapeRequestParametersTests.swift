import Foundation
import Testing

@testable import ElectricSync

/// `ElectricShapeRequest.requestParameters` — extra shape query parameters that
/// travel with a subset request without altering the base predicate.
///
/// The property that matters is the one that is expensive to get wrong:
/// **carrying parameters must not change which replica a request addresses.**
/// Replica identity derives from the base predicate, and the DNF membership
/// tracker is keyed on it, so a changed identity means a full re-bootstrap of
/// that collection for every existing client.
@Suite struct ShapeRequestParametersTests {
  /// Identity is unchanged across a request with and without parameters.
  ///
  /// This is the guarantee the feature rests on, pinned rather than asserted in
  /// prose. If `requestParameters` ever becomes part of identity, every caller
  /// that adds a parameter silently re-bootstraps its replica.
  @Test func requestParametersDoNotAffectReplicaIdentity() {
    let basePredicate = SQLExpression("tenant_id = 'tenant-1'")
    let request = ParameterTestRecord.createIdentifiedShapeRequest(
      where: basePredicate,
      orderBy: [],
      limit: nil,
      offset: "-1",
      handle: nil,
      cursor: nil,
      live: false
    )
    let withParameters = request.with(requestParameters: ["all_statuses": ["true"]])

    // Identity is derived from the base predicate (and the wire identity), so
    // deriving it from each request is how the client would address them.
    func identity(of request: ElectricShapeRequest) -> ElectricReplicaIdentity {
      ElectricReplicaIdentity(
        modelType: ParameterTestRecord.self,
        modelIdentifier: ParameterTestRecord.collectionIdentifier,
        basePredicate: request.predicate,
        wireIdentity: request.wireIdentity
      )
    }

    #expect(identity(of: withParameters) == identity(of: request))

    // The inputs that identity is built from are untouched. This is what would
    // break if a future change folded the parameters into the predicate to get
    // them onto the wire — the mistake this API exists to make unnecessary.
    #expect(withParameters.predicate == request.predicate)
    #expect(withParameters.wireIdentity == request.wireIdentity)
    #expect(withParameters.table == request.table)
    #expect(withParameters.subset == request.subset)
  }

  /// A request carries no parameters unless a caller asks for them, so existing
  /// behaviour is byte-identical.
  @Test func requestsCarryNoParametersByDefault() {
    let request = ElectricShapeRequest(table: "test_records", predicate: nil)
    #expect(request.requestParameters.isEmpty)

    let descriptor = QueryDescriptor(predicate: nil)
    #expect(descriptor.requestParameters.isEmpty)
  }

  /// Parameters survive every copy helper.
  ///
  /// The client rebuilds a request as it advances offset, handle and cursor
  /// through a fetch, so a copy helper that dropped them would lose the
  /// parameters on the second page — a bug that only shows up under
  /// pagination, which is the worst kind to find in the field.
  @Test func requestParametersSurviveEveryCopy() {
    let parameters = ["all_statuses": ["true"], "suggestion_ids": ["a", "b"]]
    let request = ElectricShapeRequest(
      table: "test_records",
      predicate: nil,
      requestParameters: parameters
    )

    #expect(request.updating(offset: "1", handle: "h", cursor: "c").requestParameters == parameters)
    #expect(request.with(log: .changesOnly).requestParameters == parameters)
    #expect(request.with(replica: .full).requestParameters == parameters)
    #expect(request.with(subset: nil).requestParameters == parameters)
    #expect(
      request.with(
        wireIdentity: ElectricShapeWireIdentity(endpoint: "/shapes/other", selectedColumns: ["id"])
      ).requestParameters == parameters)
  }

  /// Two queries differing only in their parameters are different requests and
  /// must not deduplicate onto one another.
  @Test func descriptorsWithDifferentParametersAreDistinct() {
    let plain = QueryDescriptor(predicate: SQLExpression("id = '1'"))
    let parameterised = QueryDescriptor(
      predicate: SQLExpression("id = '1'"),
      requestParameters: ["all_statuses": ["true"]]
    )

    #expect(plain != parameterised)
    #expect(plain.hashValue != parameterised.hashValue || plain != parameterised)
  }
}

private struct ParameterTestRecord: ElectricCollectionModel, Codable, Equatable {
  let id: String

  static var tableName: String { "test_records" }
  static var electricShapeWireIdentity: ElectricShapeWireIdentity {
    ElectricShapeWireIdentity(endpoint: "/shapes/test-records", selectedColumns: ["id"])
  }

  static func createShapeRequest(
    where predicate: SQLExpression?,
    orderBy: [OrderBy],
    limit: Int?,
    offset: String?,
    handle: String?,
    cursor: String?,
    live: Bool
  ) -> ElectricShapeRequest {
    ElectricShapeRequest(
      table: tableName,
      predicate: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live
    )
  }

  static func processMessage(
    _ message: ElectricMessage,
    transaction _: Any?
  ) throws -> ProcessedMessage<ParameterTestRecord> {
    let record = try JSONDecoder().decode(ParameterTestRecord.self, from: message.payload)
    return ProcessedMessage(
      records: [record],
      metadata: StoreMetadata(
        offset: message.offset,
        handle: message.handle,
        cursor: message.cursor,
        operation: .insert
      )
    )
  }
}
