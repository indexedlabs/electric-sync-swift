import Foundation

/// Declares whether rows in a local table may exist without durable ownership
/// from the Electric shape currently rebuilding its tracker.
public enum ElectricLocalTableOwnership: String, Sendable {
  /// Electric ownership must account for every local row before a cold resume.
  case exclusive
  /// Other local writers may add rows, but every Electric-owned row must still
  /// be valid and materialized before a cold resume.
  case shared
}

/// Domain models conform to `ElectricCollectionModel` so the collection client can orchestrate fetch + cache steps
/// without knowing about storage (GRDB, CoreData, etc).
public protocol ElectricCollectionModel: Sendable {

  /// Table or collection identifier used for metadata tracking.
  static var tableName: String { get }
  /// Identifier used to persist sync state for the collection (defaults to `tableName`).
  static var collectionIdentifier: String { get }
  /// Exact endpoint, selected columns, and fixed options used by the shape transport.
  static var electricShapeWireIdentity: ElectricShapeWireIdentity { get }
  /// Collection-declared version stamp for the immutable base-shape definition.
  /// Changing it isolates replica identity and forces a full bootstrap.
  static var electricShapeDefinitionVersion: String { get }

  /// Create the Electric shape request for a predicate.
  static func createShapeRequest(
    where predicate: SQLExpression?,
    orderBy: [OrderBy],
    limit: Int?,
    offset: String?,
    handle: String?,
    cursor: String?,
    live: Bool
  ) -> ElectricShapeRequest

  /// Process the Electric message into domain records and persistence metadata.
  static func processMessage(
    _ message: ElectricMessage,
    transaction: Any?
  ) throws -> ProcessedMessage<Self>

  static func processMessage(
    _ message: ElectricMessage,
    deleteTombstoneContext: ElectricDeleteTombstoneContext?,
    transaction: Any?
  ) throws -> ProcessedMessage<Self>

  /// Optional targeted hydration query used when a partial update arrives for a row that is not
  /// materialized locally. Returning `nil` falls back to a broader transient refresh on the stream.
  static func hydrationQueryDescriptor(forMissingRowKeys keys: [String]) -> QueryDescriptor?

  /// A non-nil value opts a filtered replica into durable move-out fencing.
  static var moveOutTombstoneTimeToLive: TimeInterval? { get }

  /// Declares the local-writer topology used to validate cold tracker rebuilds.
  static var electricLocalTableOwnership: ElectricLocalTableOwnership { get }

  static func versionedRowForMoveOut(
    rowKey: String,
    transaction: Any?
  ) throws -> ElectricVersionedRowIdentifier?

  static func recordMoveOutTombstone(
    _ tombstone: ElectricMoveOutTombstone,
    transaction: Any?
  ) throws

  static func removeExpiredMoveOutTombstones(
    deletedBefore: Date,
    limit: Int,
    transaction: Any?
  ) throws -> Int

  static func truncate(transaction: Any?) throws

  static func deleteByKey(_ key: String, transaction: Any?) throws
}

extension ElectricCollectionModel {
  public static var collectionIdentifier: String { tableName }

  public static var electricShapeDefinitionVersion: String { "1" }

  static func createIdentifiedShapeRequest(
    where predicate: SQLExpression?,
    orderBy: [OrderBy],
    limit: Int?,
    offset: String?,
    handle: String?,
    cursor: String?,
    live: Bool
  ) -> ElectricShapeRequest {
    createShapeRequest(
      where: predicate,
      orderBy: orderBy,
      limit: limit,
      offset: offset,
      handle: handle,
      cursor: cursor,
      live: live
    ).with(wireIdentity: electricShapeWireIdentity)
  }

  public static func processMessage(
    _ message: ElectricMessage,
    deleteTombstoneContext _: ElectricDeleteTombstoneContext?,
    transaction: Any?
  ) throws -> ProcessedMessage<Self> {
    try processMessage(message, transaction: transaction)
  }

  public static func hydrationQueryDescriptor(forMissingRowKeys _: [String]) -> QueryDescriptor? {
    nil
  }

  public static var moveOutTombstoneTimeToLive: TimeInterval? { nil }

  public static var electricLocalTableOwnership: ElectricLocalTableOwnership { .exclusive }

  public static func versionedRowForMoveOut(
    rowKey _: String,
    transaction _: Any?
  ) throws -> ElectricVersionedRowIdentifier? {
    nil
  }

  public static func recordMoveOutTombstone(
    _: ElectricMoveOutTombstone,
    transaction _: Any?
  ) throws {}

  public static func removeExpiredMoveOutTombstones(
    deletedBefore _: Date,
    limit _: Int,
    transaction _: Any?
  ) throws -> Int {
    0
  }

  public static func truncate(transaction _: Any?) throws {}

  public static func deleteByKey(_: String, transaction _: Any?) throws {}
}
