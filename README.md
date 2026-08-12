# Electric Sync for Swift

A dependency-free Swift client for synchronizing [Electric](https://electric-sql.com) Shape streams into an app-owned local cache.

ElectricSync handles Shape requests, live SSE and long-poll transports, resumable stream state, subset snapshots, move-out semantics, and optimistic-write reconciliation. Your application supplies storage, HTTP, session, logging, and tracing providers, so the library does not prescribe a database, networking stack, authentication system, or observability backend.

## Requirements

- Swift 6.1+
- iOS 18+
- macOS 15+

## Installation

Add the package in Xcode or to your `Package.swift`:

```swift
dependencies: [
  .package(
    url: "https://github.com/indexedlabs/electric-sync-swift.git",
    from: "0.1.7"
  )
]
```

Then add `ElectricSync` to your target dependencies:

```swift
.product(name: "ElectricSync", package: "electric-sync-swift")
```

## Integration model

Implement the provider protocols appropriate to your application:

- `MetadataProvider` persists Shape offsets, handles, cursors, fetch coverage, and row ownership.
- `DataCacheProvider` reads locally cached rows.
- `HTTPClientProvider` performs one-shot and long-poll Shape requests.
- `HTTPStreamClientProvider` opens live SSE streams.
- `LogProvider` and `ElectricSyncTracer` connect the client to your observability stack.
- `ElectricSyncSessionProvider` and `ElectricSyncRuntimeProvider` inject session lifecycle, time, identifiers, and cancellable delays.

Domain models conform to `ElectricCollectionModel` and own their decoding and persistence behavior. ElectricSync writes through those model/provider boundaries; your application remains responsible for observing and rendering its local database.

## Development

Run the dependency boundary check and tests from the repository root:

```bash
./Scripts/check-dependency-boundaries.sh
swift test
```

The library target intentionally imports only Foundation and CryptoKit and has no SwiftPM target dependencies. GRDB is used only by the test target.

## Origins and attribution

Electric Sync for Swift is an independent Swift port. It adapts collection and
query behavior from [TanStack DB](https://github.com/TanStack/db) and implements
the [Electric](https://github.com/electric-sql/electric) Shape protocol and
client semantics. It is not affiliated with or endorsed by either project.

See [NOTICE](NOTICE) for the applicable third-party copyright and license
notices.

## License

Electric Sync for Swift is available under the Apache License 2.0. See [LICENSE](LICENSE).
