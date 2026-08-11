import ElectricSync
import Foundation
import Testing

struct SSEParserTests {
  @Test func ignoresKeepAliveComments() throws {
    var parser = SSEParser()
    let out = try parser.feed(Data(": keep-alive\n\n".utf8))
    #expect(out.isEmpty)
  }

  @Test func parsesSingleEvent() throws {
    var parser = SSEParser()
    let out = try parser.feed(Data("data: {\"a\":1}\n\n".utf8))
    #expect(out == ["{\"a\":1}"])
  }

  @Test func parsesMultipleEventsInOneChunk() throws {
    var parser = SSEParser()
    let payload =
      "data: {\"a\":1}\n\n" +
      "data: {\"b\":2}\n\n"
    let out = try parser.feed(Data(payload.utf8))
    #expect(out == ["{\"a\":1}", "{\"b\":2}"])
  }

  @Test func joinsMultiLineData() throws {
    var parser = SSEParser()
    let payload =
      "data: {\"a\":1}\n" +
      "data: {\"b\":2}\n\n"
    let out = try parser.feed(Data(payload.utf8))
    #expect(out == ["{\"a\":1}\n{\"b\":2}"])
  }

  @Test func handlesChunkBoundaries() throws {
    var parser = SSEParser()
    let out1 = try parser.feed(Data("data: {\"a\":".utf8))
    #expect(out1.isEmpty)

    let out2 = try parser.feed(Data("1}\n\n".utf8))
    #expect(out2 == ["{\"a\":1}"])
  }

  @Test func flushesPendingDataOnFinish() throws {
    var parser = SSEParser()
    _ = try parser.feed(Data("data: {\"a\":1}\n".utf8))
    #expect(parser.finish() == ["{\"a\":1}"])
  }
}
