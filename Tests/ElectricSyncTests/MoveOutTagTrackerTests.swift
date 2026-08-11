import Foundation
import Testing

@testable import ElectricSync

struct MoveOutTagTrackerTests {
  // MARK: - Tagged-shape capability ON: an EMPTY slash-delimited segment is
  // NON_PARTICIPATING — not indexed and never matches a move pattern — and
  // there is no "_" wildcard; "_" is an ordinary segment value.
  @Test
  func nonParticipatingEmptySegmentIsNotIndexedAndNeverMatches() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a//c"],
      removedTags: nil
    )

    let keysFromNonParticipatingPattern = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 1, value: "b")]
    )
    #expect(keysFromNonParticipatingPattern.isEmpty)

    let keysFromEmptyValuePattern = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 1, value: "")]
    )
    #expect(keysFromEmptyValuePattern.isEmpty)

    let keysFromIndexedPattern = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 2, value: "c")]
    )
    #expect(keysFromIndexedPattern == ["k1"])
  }

  @Test
  func underscoreSegmentIsAnOrdinaryValueNotAWildcard() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/_/c"],
      removedTags: nil
    )

    // "_" does not wildcard-match other values at its position.
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]).isEmpty
    )
    // "_" is indexed and matched exactly like any other segment value.
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "_")]) == ["k1"]
    )
  }

  // MARK: - Legacy capability OFF: "_" is the wildcard segment —
  // it stays out of the value-keyed index, but it matches ANY move pattern
  // value addressed at its position (reachability comes from the per-position
  // wildcard bucket). This contract must hold unchanged until tagged-shape
  // 1.7.7 activation.

  @Test
  func legacyWildcardMatchesAnyRemovalAtItsOwnPosition() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/_/c"],
      removedTags: nil
    )

    // The wildcard at position 1 matches a removal for any value there even
    // though "_" is never a key in the value-keyed index.
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]) == ["k1"]
    )
    // Full removal cleaned the wildcard bucket and the value index together.
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "x")]).isEmpty
    )
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 0, value: "a")]).isEmpty
    )
    #expect(tracker.tags(for: "k1") == nil)
  }

  @Test
  func legacyWildcardPatternValueUnderscoreRemovesWildcardRow() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/_/c"],
      removedTags: nil
    )

    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "_")]) == ["k1"]
    )
  }

  @Test
  func removingOneWildcardTagKeepsAnotherWildcardTagReachable() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/_", "b/_"],
      removedTags: nil
    )

    tracker.applyTagDelta(
      key: "k1",
      operation: .update,
      tags: nil,
      removedTags: ["a/_"]
    )

    #expect(tracker.tags(for: "k1") == ["b/_"])
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "anything")]) == ["k1"]
    )
    #expect(tracker.tags(for: "k1") == nil)
  }

  @Test
  func legacyWildcardRowIsReachableForMoveInConditionRestore() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/_"],
      removedTags: nil,
      activeConditions: [true, false]
    )

    // The wildcard bucket also feeds move-in candidate selection: a row whose
    // tag carries "_" at the pattern position is reachable for condition
    // restoration.
    tracker.processMoveIn(patterns: [MovePattern(pos: 1, value: "anything")])
    #expect(tracker.activeConditions(for: "k1") == [true, true])
  }

  @Test
  func taggedModeUnderscoreAtPatternPositionDoesNotMatchOtherValues() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/_/c"],
      removedTags: nil
    )

    // Same row, tagged mode: "_" is an ordinary value with exact-only
    // matching, so a pos-1 removal for another value does not touch it.
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]).isEmpty
    )
    #expect(tracker.tags(for: "k1") == ["a/_/c"])
  }

  @Test
  func wildcardBucketResetsWithTheTrackerGeneration() throws {
    let taggedShapeEnabled = LockIsolatedFlag(false)
    let tracker = MoveOutTagTracker(
      isTaggedShapeProtocolEnabled: { taggedShapeEnabled.value }
    )
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/_/c"],
      removedTags: nil
    )
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]) == ["k1"]
    )

    // Epoch flip across a reset: the tagged generation must not inherit the
    // legacy wildcard bucket — the same tag now survives a pos-1 removal.
    tracker.reset()
    taggedShapeEnabled.value = true
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/_/c"],
      removedTags: nil
    )
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]).isEmpty
    )
    #expect(tracker.tags(for: "k1") == ["a/_/c"])
  }

  @Test
  func legacyWildcardMatchesMoveOutRemovalForCandidateRows() throws {
    let tracker = MoveOutTagTracker()
    // The row is a candidate via "a/b"; its sibling tag carries the wildcard
    // at the addressed position and must be removed by the same move-out, so
    // the row fully evicts instead of leaving stale membership behind.
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/b", "c/_"],
      removedTags: nil
    )

    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]) == ["k1"]
    )
    #expect(tracker.tags(for: "k1") == nil)
  }

  @Test
  func legacyWildcardRowSurvivesWhenOnlyExactTagMatches() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/b", "c/d"],
      removedTags: nil
    )

    // Without a wildcard, only the exactly-matching tag is removed and the
    // row survives on its remaining tag.
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]).isEmpty
    )
    #expect(tracker.tags(for: "k1") == ["c/d"])
  }

  @Test
  func capabilityRollbackAfterResetRestoresLegacyWildcardSemantics() throws {
    let taggedShapeEnabled = LockIsolatedFlag(true)
    let tracker = MoveOutTagTracker(
      isTaggedShapeProtocolEnabled: { taggedShapeEnabled.value }
    )

    // Capability ON: "_" is an ordinary value, no wildcard matching.
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/b", "c/_"],
      removedTags: nil
    )
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]).isEmpty
    )
    #expect(tracker.tags(for: "k1") == ["c/_"])

    // Rollback: the gate flips off across a reset/full-bootstrap boundary and
    // the legacy wildcard contract applies to the new generation.
    tracker.reset()
    taggedShapeEnabled.value = false

    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/b", "c/_"],
      removedTags: nil
    )
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]) == ["k1"]
    )
  }

  @Test
  func capabilityFlipWithoutResetDoesNotChangeSegmentSemanticsMidGeneration() throws {
    let taggedShapeEnabled = LockIsolatedFlag(false)
    let tracker = MoveOutTagTracker(
      isTaggedShapeProtocolEnabled: { taggedShapeEnabled.value }
    )

    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/b", "c/_"],
      removedTags: nil
    )

    // The gate flips mid-generation; the resolved legacy semantics hold until
    // the next reset so index and matcher never disagree.
    taggedShapeEnabled.value = true
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 1, value: "b")]) == ["k1"]
    )
  }

  @Test
  func ignoresTagLengthMismatches() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/b/c"],
      removedTags: nil
    )

    tracker.applyTagDelta(
      key: "k1",
      operation: .update,
      tags: ["x/y"],
      removedTags: nil
    )

    let keysFromMismatchedTagPattern = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 0, value: "x")]
    )
    #expect(keysFromMismatchedTagPattern.isEmpty)

    let keysFromOriginalTagPattern = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 0, value: "a")]
    )
    #expect(keysFromOriginalTagPattern == ["k1"])
  }

  @Test
  func removedTagsRequireExactStringMatch() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["hash1/_/hash3"],
      removedTags: nil
    )

    tracker.applyTagDelta(
      key: "k1",
      operation: .update,
      tags: nil,
      removedTags: ["hash1/hash2/hash3"]
    )

    let keysToDelete = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 2, value: "hash3")]
    )
    #expect(keysToDelete == ["k1"])
  }

  @Test
  func deleteOperationClearsTagState() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "k1",
      operation: .insert,
      tags: ["a/b/c"],
      removedTags: nil
    )

    tracker.applyTagDelta(
      key: "k1",
      operation: .delete,
      tags: nil,
      removedTags: nil
    )

    let keysToDelete = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 0, value: "a")]
    )
    #expect(keysToDelete.isEmpty)
  }

  // Electric joins membership-tag segments with "/" (sync-service
  // `shape.ex` `Enum.join("/")`), and TanStack DB's `tag-index` splits on the
  // same delimiter. The tracker must too, so position-based `{pos, value}`
  // move-out patterns can match a multi-condition subquery tag. Regression: the
  // tracker previously split on "|", collapsing every multi-segment tag into a
  // single segment, so move-outs for any multi-condition shape silently never
  // evicted.
  @Test
  func slashDelimitedTagMovesOutAtFirstPosition() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "row-1",
      operation: .insert,
      tags: ["hashA/hashB"],
      removedTags: nil
    )
    let evicted = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 0, value: "hashA")]
    )
    #expect(evicted == ["row-1"])
  }

  @Test
  func slashDelimitedTagMovesOutAtSecondPosition() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "row-1",
      operation: .insert,
      tags: ["hashA/hashB"],
      removedTags: nil
    )
    let evicted = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 1, value: "hashB")]
    )
    #expect(evicted == ["row-1"])
  }

  @Test
  func slashDelimitedRowSurvivesNonMatchingMoveOut() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "row-1",
      operation: .insert,
      tags: ["hashA/hashB"],
      removedTags: nil
    )
    // "hashA" is at position 0, not position 1 — a pos-1 pattern must not evict.
    let evicted = tracker.processMoveOut(
      patterns: [MoveOutPattern(pos: 1, value: "hashA")]
    )
    #expect(evicted.isEmpty)
  }

  // Two rows can carry the SAME tag string (identical subquery membership).
  // `removedTags` evicts the shared parse-cache entry per tag string, so the
  // second row's removal used to skip `removeTagFromIndex` and orphan that row
  // in the position index. The orphan is benign for eviction outcomes
  // (`processMoveOut` re-validates candidates against `rowTags`), so this is a
  // consistency/parity guard against the shared-tag removal path rather than a
  // behavior-changing repro — it must stay correct as the index removal hardens
  // toward TanStack's cache-free parse.
  @Test
  func sharedTagRemovedFromBothRowsKeepsEvictionConsistent() throws {
    let tracker = MoveOutTagTracker()
    tracker.applyTagDelta(
      key: "row-1",
      operation: .insert,
      tags: ["h1/h2"],
      removedTags: nil
    )
    tracker.applyTagDelta(
      key: "row-2",
      operation: .insert,
      tags: ["h1/h2", "h3/h4"],
      removedTags: nil
    )

    // row-1's removal evicts the shared "h1/h2" parse-cache entry; row-2's
    // removal must still drop row-2 from the index for "h1/h2".
    tracker.applyTagDelta(
      key: "row-1",
      operation: .update,
      tags: nil,
      removedTags: ["h1/h2"]
    )
    tracker.applyTagDelta(
      key: "row-2",
      operation: .update,
      tags: nil,
      removedTags: ["h1/h2"]
    )

    // Neither row is a member of "h1" anymore.
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 0, value: "h1")]).isEmpty
    )
    // row-2 is still evictable via its surviving "h3/h4" tag; row-1 has none.
    #expect(
      tracker.processMoveOut(patterns: [MoveOutPattern(pos: 0, value: "h3")]) == ["row-2"]
    )
  }

  // MARK: - DNF active_conditions matrix

  // tags.test.ts "should keep row visible when only one disjunct is deactivated
  // (DNF partial)" + "should delete row when all disjuncts are deactivated (DNF full)"
  @Test
  func dnfRowSurvivesUntilEveryDisjunctIsDeactivated() throws {
    let tracker = taggedShapeTracker()
    // Two disjuncts: disjunct 0 participates at position 0, disjunct 1 at position 1.
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    // Move-out at position 0 — disjunct 0 fails, disjunct 1 still satisfied.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]).isEmpty
    )
    #expect(tracker.tags(for: "1") != nil)

    // Move-out at position 1 — no disjunct remains: exactly one removal.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 1, value: "hash_b")]) == ["1"]
    )
    #expect(tracker.tags(for: "1") == nil)
    #expect(tracker.activeConditions(for: "1") == nil)
  }

  // tags.test.ts "should keep row alive when one disjunct lost but another
  // keeps it visible (multi-disjunct)"
  @Test
  func dnfMultiPositionDisjunctFailsWhenAnyOfItsPositionsDeactivates() throws {
    let tracker = taggedShapeTracker()
    // Disjunct 0 covers positions [0, 1]; disjunct 1 covers position [2].
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b/", "//hash_c"],
      removedTags: nil,
      activeConditions: [true, true, true]
    )

    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]).isEmpty
    )
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 2, value: "hash_c")]) == ["1"]
    )
  }

  // tags.test.ts "should activate correct positions on move-in"
  @Test
  func moveInReactivatesConditionPositionForRetainedRow() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, false]
    )

    tracker.processMoveIn(patterns: [MovePattern(pos: 1, value: "hash_b")])
    #expect(tracker.activeConditions(for: "1") == [true, true])

    // Position 1 was re-activated, so losing position 0 keeps the row.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]).isEmpty
    )
    #expect(tracker.tags(for: "1") != nil)
  }

  // tags.test.ts "should support move-out then move-in then move-out cycle"
  @Test
  func moveOutMoveInMoveOutCycleUsesRestoredPosition() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]).isEmpty
    )
    tracker.processMoveIn(patterns: [MovePattern(pos: 0, value: "hash_a")])
    // Disjunct 0 was re-activated by the move-in, so losing position 1 keeps the row.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 1, value: "hash_b")]).isEmpty
    )
    #expect(tracker.tags(for: "1") != nil)
  }

  // tags.test.ts "should not resurrect deleted rows on move-in (tag index
  // cleaned up)" — plus later reinsertion through a fresh change message.
  @Test
  func moveInDoesNotResurrectFullyRemovedRowAndLaterChangeReinserts() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    // Single disjunct [0, 1] fails when either position deactivates.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]) == ["1"]
    )

    // Move-in cannot restore a fully removed row: index and conditions are gone.
    tracker.processMoveIn(patterns: [MovePattern(pos: 0, value: "hash_a")])
    #expect(tracker.tags(for: "1") == nil)
    #expect(tracker.activeConditions(for: "1") == nil)
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 1, value: "hash_b")]).isEmpty
    )

    // Only a later Electric change message re-establishes membership.
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )
    #expect(tracker.tags(for: "1") == ["hash_a/hash_b"])
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 1, value: "hash_b")]) == ["1"]
    )
  }

  // tags.test.ts "should overwrite active_conditions when server re-sends row
  // (move-in overwrite)"
  @Test
  func resentRowOverwritesActiveConditions() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b"],
      removedTags: nil,
      activeConditions: [true, false]
    )

    tracker.applyTagDelta(
      key: "1",
      operation: .update,
      tags: ["hash_a/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )
    #expect(tracker.activeConditions(for: "1") == [true, true])

    // Single disjunct [0, 1]: deactivating position 0 removes the row.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]) == ["1"]
    )
  }

  // tags.test.ts "should handle mixed rows: some with active_conditions, some without"
  @Test
  func mixedDnfAndSimpleRowsApplyTheirOwnMoveOutSemantics() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "dnf",
      operation: .insert,
      tags: ["hash_a/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )
    tracker.applyTagDelta(
      key: "simple",
      operation: .insert,
      tags: ["hash_a/hash_c"],
      removedTags: nil
    )

    // DNF row survives via disjunct 1; simple row's only tag matches and empties.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]) == ["simple"]
    )
    #expect(tracker.tags(for: "dnf") != nil)
  }

  // tags.test.ts "should handle multiple patterns deactivating the same row in one call"
  @Test
  func multiplePatternsInOneMoveOutDeactivateTheSameRowOnce() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    let removed = tracker.processMoveOut(
      patterns: [
        MovePattern(pos: 0, value: "hash_a"),
        MovePattern(pos: 1, value: "hash_b"),
      ]
    )
    #expect(removed == ["1"])
  }

  // tags.test.ts "should not cause phantom deletes from orphaned tag index
  // entries" + "should clean up ALL tag index entries when row is deleted by
  // move-out"
  @Test
  func fullDnfRemovalLeavesNoOrphanedIndexEntries() throws {
    let tracker = taggedShapeTracker()
    // Two disjuncts [[0, 1], [2, 3]], all positions active.
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["X/X//", "//X/X"],
      removedTags: nil,
      activeConditions: [true, true, true, true]
    )

    // Both disjuncts lose their second position in one call → removed.
    let removed = tracker.processMoveOut(
      patterns: [MovePattern(pos: 1, value: "X"), MovePattern(pos: 3, value: "X")]
    )
    #expect(removed == ["1"])

    // Re-insert with new hashes; stale "X" patterns must have no effect.
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["Y/Y//", "//Y/Y"],
      removedTags: nil,
      activeConditions: [true, true, true, true]
    )
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "X")]).isEmpty
    )
    #expect(tracker.activeConditions(for: "1") == [true, true, true, true])

    // A legitimate deactivation at position 2 leaves disjunct 0 fully active.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 2, value: "Y")]).isEmpty
    )
    #expect(tracker.tags(for: "1") != nil)
  }

  @Test
  func deleteOperationClearsActiveConditionState() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    tracker.applyTagDelta(key: "1", operation: .delete, tags: nil, removedTags: nil)
    #expect(tracker.activeConditions(for: "1") == nil)
    #expect(tracker.tags(for: "1") == nil)
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]).isEmpty
    )
  }

  @Test
  func resetClearsDnfStateAndContinuity() throws {
    let tracker = taggedShapeTracker()
    tracker.establishContinuity()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    tracker.reset()

    #expect(tracker.isContinuityEstablished == false)
    #expect(tracker.tags(for: "1") == nil)
    #expect(tracker.activeConditions(for: "1") == nil)
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]).isEmpty
    )
  }

  @Test
  func moveInBeforeAnyTaggedMessageIsIgnored() throws {
    let tracker = taggedShapeTracker()
    tracker.processMoveIn(patterns: [MovePattern(pos: 0, value: "hash_a")])
    #expect(tracker.tagsByRowKey().isEmpty)
  }

  @Test
  func rowVisibleEvaluatesDnfExactly() throws {
    // tags.test.ts "rowVisible should evaluate DNF correctly"
    #expect(
      MoveOutTagTracker.rowVisible(
        activeConditions: [true, false],
        disjunctPositions: [[0], [1]]
      )
    )
    #expect(
      !MoveOutTagTracker.rowVisible(
        activeConditions: [false, false],
        disjunctPositions: [[0], [1]]
      )
    )
    #expect(
      !MoveOutTagTracker.rowVisible(
        activeConditions: [true, false],
        disjunctPositions: [[0, 1]]
      )
    )
    #expect(
      MoveOutTagTracker.rowVisible(
        activeConditions: [true, true],
        disjunctPositions: [[0, 1]]
      )
    )
  }

  @Test
  func deriveDisjunctPositionsExtractsParticipatingPositions() throws {
    // tags.test.ts "deriveDisjunctPositions should extract participating
    // positions per disjunct" — empty segments are non-participating.
    let derived = MoveOutTagTracker.deriveDisjunctPositions([
      ["hash_a", "", "hash_b"],
      ["", "hash_c", ""],
    ])
    #expect(derived == [[0, 2], [1]])
  }

  // MARK: - Duplicate / replay idempotence across service restarts

  @Test
  func replayedTagDeltaIsIdempotentForTagsIndexAndConditions() throws {
    let tracker = taggedShapeTracker()
    for _ in 0..<2 {
      tracker.applyTagDelta(
        key: "1",
        operation: .insert,
        tags: ["hash_a/", "/hash_b"],
        removedTags: nil,
        activeConditions: [true, true]
      )
    }

    #expect(tracker.tags(for: "1") == ["/hash_b", "hash_a/"])
    #expect(tracker.activeConditions(for: "1") == [true, true])

    // Replayed state behaves exactly like single application: one disjunct
    // loss keeps the row, losing both evicts it exactly once.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]).isEmpty
    )
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 1, value: "hash_b")]) == ["1"]
    )
  }

  @Test
  func replayedMoveOutAfterEvictionEvictsNothing() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    let patterns = [MovePattern(pos: 0, value: "hash_a")]
    #expect(tracker.processMoveOut(patterns: patterns) == ["1"])
    // A replayed move-out addresses membership that no longer exists.
    #expect(tracker.processMoveOut(patterns: patterns).isEmpty)
    #expect(tracker.tags(for: "1") == nil)
  }

  @Test
  func replayedMoveInIsIdempotent() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, false]
    )

    for _ in 0..<2 {
      tracker.processMoveIn(patterns: [MovePattern(pos: 1, value: "hash_b")])
    }
    #expect(tracker.activeConditions(for: "1") == [true, true])
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]).isEmpty
    )
  }

  @Test
  func duplicateTagStringsInOneMessageIndexOnce() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b", "hash_a/hash_b"],
      removedTags: nil
    )

    #expect(tracker.tags(for: "1") == ["hash_a/hash_b"])
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]) == ["1"]
    )
  }

  // MARK: - Malformed move patterns and conditions (tag-index.ts guards)

  @Test
  func negativeMovePatternPositionIsIgnored() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: -1, value: "hash_a")]).isEmpty
    )
    tracker.processMoveIn(patterns: [MovePattern(pos: -1, value: "hash_a")])
    #expect(tracker.activeConditions(for: "1") == [true, true])
  }

  @Test
  func moveInForSimpleRowWithoutActiveConditionsIsIgnored() throws {
    let tracker = taggedShapeTracker()
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b"],
      removedTags: nil
    )

    // Upstream processMoveInEvent skips rows without stored active conditions.
    tracker.processMoveIn(patterns: [MovePattern(pos: 0, value: "hash_a")])
    #expect(tracker.activeConditions(for: "1") == nil)

    // Simple-shape move-out semantics remain untouched by the ignored move-in.
    #expect(
      tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "hash_a")]) == ["1"]
    )
  }

  @Test
  func moveInPositionBeyondStoredConditionsLeavesConditionsUnchanged() throws {
    let tracker = taggedShapeTracker()
    // Malformed short condition vector relative to the two tag positions; the
    // tracker guards position access exactly like upstream (`pattern.pos <
    // activeConditions.length`) instead of trapping.
    tracker.applyTagDelta(
      key: "1",
      operation: .insert,
      tags: ["hash_a/hash_b"],
      removedTags: nil,
      activeConditions: [false]
    )

    tracker.processMoveIn(patterns: [MovePattern(pos: 1, value: "hash_b")])
    #expect(tracker.activeConditions(for: "1") == [false])
  }

  // MARK: - Cross-row active-condition overlap

  @Test
  func sharedPositionValueDeactivatesEachRowsOwnConditions() throws {
    let tracker = taggedShapeTracker()
    // Both rows participate at position 0 with the same value; only row "b"
    // has a second disjunct keeping it visible after position 0 deactivates.
    tracker.applyTagDelta(
      key: "a",
      operation: .insert,
      tags: ["shared/", "/hash_a"],
      removedTags: nil,
      activeConditions: [true, false]
    )
    tracker.applyTagDelta(
      key: "b",
      operation: .insert,
      tags: ["shared/", "/hash_b"],
      removedTags: nil,
      activeConditions: [true, true]
    )

    let removed = tracker.processMoveOut(patterns: [MovePattern(pos: 0, value: "shared")])
    #expect(removed == ["a"])
    #expect(tracker.tags(for: "b") != nil)
    #expect(tracker.activeConditions(for: "b") == [false, true])
  }
}

private func taggedShapeTracker() -> MoveOutTagTracker {
  MoveOutTagTracker(isTaggedShapeProtocolEnabled: { true })
}

private final class LockIsolatedFlag: @unchecked Sendable {
  private let lock = NSLock()
  private var storage: Bool

  init(_ value: Bool) {
    self.storage = value
  }

  var value: Bool {
    get { lock.withLock { storage } }
    set { lock.withLock { storage = newValue } }
  }
}
