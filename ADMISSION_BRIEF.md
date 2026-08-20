LANE: bridged-mode-admission
You are in a clone of indexedlabs/electric-sync-swift on branch feat/bridged-compatible-mode-admission. This package is the Electric sync client the Indexed iOS app pins (currently 0.1.8). Your change unblocks indexed PR #3364 ("Bridge legacy cursor state across sync mode flips"). The blocking reviewer finding, verbatim:

"The pinned electric-sync-swift 0.1.8 client still classifies every exact-disabled cursor as `.legacyPreCutover` in `resumeSyncState`, while `rebuildSimpleTrackerIfAdmissible` accepts only `.exact`. The cold eager owner therefore requires a full bootstrap and requests `offset=-1`. The bridge still needs an admission mechanism for its validated compatible-mode migration."

## Task

In Sources/ElectricSync/ElectricSyncClient.swift (resume-source classification around line 1108, admission around 1224, call sites 1338-1360 and 1774+), implement an admission mechanism for a VALIDATED compatible-mode bridge:

- The app-side bridge (indexed repo, branch agent/legacy-cursor-mode-bridge, file ios/Index/LocalPackages/Shapes/Sources/Shapes/ElectricSyncProviders.swift — read it at /Users/mh/labs/indexed if you need the exact rename semantics; do NOT modify that repo) atomically renames collectionSyncState.id plus electricShapeRowOwnership.streamStateKey rows and tombstones from the old mode key to the new one, eager<->progressive only. It needs a way to attest that a legacy-keyed cursor was migrated with continuity intact, such that `rebuildSimpleTrackerIfAdmissible` admits it for statically-simple topologies instead of forcing offset=-1.
- Design within existing contracts: no second DNF protocol store (durable tracker persistence is forbidden); statically-simple shapes may rebuild tracker state from durable ownership metadata under exact validity keys — extend that rebuild eligibility to the bridged classification. A plausible shape: a new resume-source case (e.g. `.legacyBridged`) recognized when the persisted state carries a bridge attestation written during the app's atomic rename (representation must travel WITH the renamed state, not in a side store), admissible in `rebuildSimpleTrackerIfAdmissible` for statically-simple topologies only, eager<->progressive only, never into onDemand, never DNF.
- Package tests mirroring the existing resume/admission test style: bridged eager cursor admits without bootstrap; bridged state on a dnf topology is refused; attestation absent -> `.legacyPreCutover` unchanged; exact path unaffected.
- `swift test` until green.
- Do NOT tag or release.

## Deliverable

Commit (imperative mood), push the branch, open a PR in indexedlabs/electric-sync-swift, body quoting the finding and citing indexed PR #3364 and graph task indexed-task-689b. Do not modify the indexed repo. Do not run `mt`. Final line exactly one of: `DONE <pr-url>`, `BLOCKED <reason>`, or `TIMEOUT`.
