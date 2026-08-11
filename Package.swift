// swift-tools-version: 6.1

import PackageDescription

let package = Package(
  name: "ElectricSync",
  platforms: [
    .iOS(.v18),
    .macOS(.v15),
  ],
  products: [
    .library(
      name: "ElectricSync",
      targets: ["ElectricSync"]
    )
  ],
  dependencies: [
    .package(url: "https://github.com/groue/GRDB.swift.git", exact: "7.10.0"),
  ],
  targets: [
    .target(
      name: "ElectricSync",
      dependencies: []
    ),
    .testTarget(
      name: "ElectricSyncTests",
      dependencies: [
        "ElectricSync",
        .product(name: "GRDB", package: "GRDB.swift"),
      ]
    ),
  ]
)
