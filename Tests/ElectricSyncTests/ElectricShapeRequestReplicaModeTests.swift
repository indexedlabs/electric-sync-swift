import Foundation
import Testing

@testable import ElectricSync

@Test func electricShapeRequestDefaultsToDefaultReplicaMode() {
  let request = ElectricShapeRequest(table: "record", predicate: nil)

  #expect(request.replica == .default)
}

@Test func electricShapeRequestReplicaModePinsElectricWireValues() {
  #expect(ElectricReplicaMode.default.rawValue == "default")
  #expect(ElectricReplicaMode.full.rawValue == "full")
}

@Test func electricShapeRequestTransformationsRetainFullReplicaMode() {
  let subset = ElectricSubsetRequest(
    whereClause: "id = $1",
    paramsJSON: #"{"1":"abc"}"#,
    orderByClause: nil,
    limit: 10,
    offset: nil
  )
  let wireIdentity = ElectricShapeWireIdentity(
    endpoint: "/shapes/record",
    selectedColumns: ["id", "name"]
  )
  let request = ElectricShapeRequest(
    table: "record",
    predicate: nil,
    offset: "-1",
    handle: "handle",
    cursor: "cursor",
    live: true,
    log: .changesOnly,
    replica: .full,
    subset: subset
  )

  #expect(request.replica == .full)
  #expect(
    request.updating(offset: "now", handle: "next", cursor: "next", live: false).replica == .full
  )
  #expect(request.with(log: nil).replica == .full)
  #expect(request.with(log: .full).replica == .full)
  #expect(request.with(subset: nil).replica == .full)
  #expect(request.with(subset: subset).replica == .full)
  #expect(request.with(wireIdentity: wireIdentity).replica == .full)
}

@Test func electricShapeRequestTransformationsRetainDefaultReplicaMode() {
  let request = ElectricShapeRequest(table: "record", predicate: nil)
  let wireIdentity = ElectricShapeWireIdentity(
    endpoint: "/shapes/record",
    selectedColumns: ["id"]
  )

  #expect(
    request.updating(offset: "now", handle: "h", cursor: "c").replica == .default
  )
  #expect(request.with(log: .changesOnly).replica == .default)
  #expect(request.with(subset: nil).replica == .default)
  #expect(request.with(wireIdentity: wireIdentity).replica == .default)
}

@Test func electricShapeRequestWithReplicaOverridesOnlyReplicaMode() {
  let subset = ElectricSubsetRequest(
    whereClause: "TRUE",
    paramsJSON: nil,
    orderByClause: nil,
    limit: nil,
    offset: nil
  )
  let request = ElectricShapeRequest(
    table: "record",
    predicate: nil,
    offset: "-1",
    handle: "handle",
    cursor: "cursor",
    live: true,
    log: .changesOnly,
    subset: subset
  )

  let full = request.with(replica: .full)

  #expect(full.replica == .full)
  #expect(full.wireIdentity == request.wireIdentity)
  #expect(full.table == request.table)
  #expect(full.offset == request.offset)
  #expect(full.handle == request.handle)
  #expect(full.cursor == request.cursor)
  #expect(full.live == request.live)
  #expect(full.log == request.log)
  #expect(full.subset == request.subset)
  #expect(full.with(replica: .default).replica == .default)
}
