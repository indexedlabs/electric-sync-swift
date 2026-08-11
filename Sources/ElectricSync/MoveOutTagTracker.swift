import Foundation

/// Process-local Electric membership tracker.
///
/// Segment semantics are capability-scoped so the legacy simple-shape contract
/// stays intact until tagged-shape 1.7.7 activation:
/// - **Legacy (capability off)**: `_` is the wildcard segment — it is not
///   indexed at its position, and it matches any move pattern value during
///   removal. Empty segments are ordinary values.
/// - **Tagged (capability on)**: an empty slash-delimited segment is
///   NON_PARTICIPATING — not indexed and never matched by a move pattern —
///   while `_` is an ordinary segment value.
///
/// Tagged-mode membership:
/// - Simple shapes (no `active_conditions`): a move-out removes matching tags
///   and the row is evicted when its tag set becomes empty.
/// - DNF shapes (`active_conditions` present): each tag is one disjunct;
///   `disjunctPositions` are derived once from the first tagged message, a
///   move-out deactivates one condition position, and the row is evicted only
///   when no disjunct has all of its participating positions active.
/// - A move-in silently re-activates condition positions for rows the tag
///   index still holds; it never re-publishes or resurrects a removed row.
///
/// The mode is resolved from the capability gate at the first tag fold of a
/// tracker generation and holds until `reset()`; a capability flip therefore
/// takes effect only across a reset/full-bootstrap boundary, never mid-stream.
///
/// Tracker state is process-local and valid only while it has continuously
/// observed its stream since a full bootstrap. `isContinuityEstablished`
/// records that observation; batch application refuses to fold tagged protocol
/// input into an unestablished tracker over a resumed cursor and forces a full
/// bootstrap instead. Durable ownership tags are membership state, not
/// continuity: DNF condition state never leaves the process.
public final class MoveOutTagTracker: @unchecked Sendable {
  private let lock = NSLock()
  private let isTaggedShapeProtocolEnabled: @Sendable () -> Bool

  private var tagCache: [String: [String]] = [:]
  private var rowTags: [String: Set<String>] = [:]
  private var tagIndex: [[String: Set<String>]] = []
  // Legacy-mode wildcard reachability refcounts: rows whose tags carry "_" at
  // a position. A row can have multiple wildcard tags at the same position, so
  // reachability must remain until the final one leaves. Legacy semantics keep
  // "_" out of the value-keyed index while still matching any removal addressed
  // at its position. Tagged mode never populates or consults this.
  private var wildcardRowsByPosition: [[String: Int]] = []
  private var tagLength: Int?
  private var rowActiveConditions: [String: [Bool]] = [:]
  private var disjunctPositions: [[Int]]?
  private var continuityEstablished = false
  private var resolvedTaggedMode: Bool?

  public init(isTaggedShapeProtocolEnabled: @escaping @Sendable () -> Bool = { false }) {
    self.isTaggedShapeProtocolEnabled = isTaggedShapeProtocolEnabled
  }

  public func reset() {
    lock.withLock {
      tagCache.removeAll(keepingCapacity: false)
      rowTags.removeAll(keepingCapacity: false)
      tagIndex.removeAll(keepingCapacity: false)
      wildcardRowsByPosition.removeAll(keepingCapacity: false)
      tagLength = nil
      rowActiveConditions.removeAll(keepingCapacity: false)
      disjunctPositions = nil
      continuityEstablished = false
      resolvedTaggedMode = nil
    }
  }

  func rebuildSimpleMembership(_ tagsByRowKey: [String: [String]], taggedMode: Bool) {
    reset()
    for (key, tags) in tagsByRowKey {
      applyTagDelta(key: key, operation: .insert, tags: tags, removedTags: nil)
    }
    establishContinuity(taggedMode: taggedMode)
  }

  var isContinuityEstablished: Bool {
    lock.withLock { continuityEstablished }
  }

  /// Continuity is epoch-scoped: establishing it pins the current capability
  /// mode for this generation so a later live gate flip is detectable as a
  /// tracker-generation boundary instead of silently reusing state that was
  /// folded under the other segment semantics.
  func establishContinuity() {
    establishContinuity(taggedMode: isTaggedShapeProtocolEnabled())
  }

  func establishContinuity(taggedMode: Bool) {
    lock.withLock {
      continuityEstablished = true
      if resolvedTaggedMode == nil {
        resolvedTaggedMode = taggedMode
      }
    }
  }

  /// The capability epoch this tracker generation is pinned to, or nil when
  /// no fold or continuity has resolved it yet. Batch application compares
  /// this against the live gate and full-bootstraps on a mismatch.
  var pinnedTaggedSegmentSemantics: Bool? {
    lock.withLock { resolvedTaggedMode }
  }

  /// Copies the process-local DNF condition state (active conditions and
  /// derived disjunct positions) from another tracker of the same shape.
  /// Durable-ownership batch application seeds a fresh per-batch tracker from
  /// persisted tags, adopts DNF state from the owner-generation tracker before
  /// folding, and copies the updated state back after the batch commits.
  func adoptDNFState(from other: MoveOutTagTracker) {
    guard other !== self else { return }
    let (conditions, positions) = other.dnfStateSnapshot()
    lock.withLock {
      rowActiveConditions = conditions
      if disjunctPositions == nil {
        disjunctPositions = positions
      }
    }
  }

  private func dnfStateSnapshot() -> ([String: [Bool]], [[Int]]?) {
    lock.withLock { (rowActiveConditions, disjunctPositions) }
  }

  public func applyTagDelta(
    key: String,
    operation: StoreMetadata.StoreOperation,
    tags: [String]?,
    removedTags: [String]?,
    activeConditions: [Bool]? = nil
  ) {
    lock.withLock {
      if operation == .delete {
        clearTagsForRow(key: key)
        return
      }

      guard tags != nil || removedTags != nil else { return }

      let taggedMode = resolveTaggedMode()
      var rowTagSet = rowTags[key] ?? Set<String>()

      if let removedTags {
        for tag in removedTags {
          // Re-derive the parsed segments unconditionally (parseTag re-parses on
          // a cache miss) and drop the row's index entries whenever it actually
          // carried the tag. Previously the index removal was gated on a *live*
          // tagCache entry while that entry was evicted per tag string, so two
          // rows sharing a tag string skipped the second row's
          // removeTagFromIndex — orphaning a rowId in the position index. Benign
          // for eviction outcomes (processMoveOut re-validates candidates
          // against rowTags) but it leaked index state and diverged from
          // TanStack's cache-free parseTag.
          if rowTagSet.remove(tag) != nil {
            removeTagFromIndex(tag: parseTag(tag), key: key, taggedMode: taggedMode)
          }
          tagCache.removeValue(forKey: tag)
        }
      }

      if let tags {
        for tag in tags {
          let parsed = parseTag(tag)
          if let tagLength {
            if parsed.count != tagLength { continue }
          } else {
            tagLength = parsed.count
            tagIndex = Array(repeating: [:], count: parsed.count)
            wildcardRowsByPosition = Array(repeating: [:], count: parsed.count)
          }

          if rowTagSet.insert(tag).inserted {
            addTagToIndex(tag: parsed, key: key, taggedMode: taggedMode)
          }
        }

        // Disjunct positions are fixed by the shape's WHERE clause; derive them
        // once from the first tagged message (TanStack `deriveDisjunctPositions`).
        if disjunctPositions == nil {
          disjunctPositions = Self.deriveDisjunctPositions(tags.map(parseTag))
        }
      }

      // Store active conditions when provided; the server overwrites them on
      // re-send (TanStack `processTagsForChangeMessage`).
      if let activeConditions, !activeConditions.isEmpty {
        rowActiveConditions[key] = activeConditions
      }

      if rowTagSet.isEmpty {
        rowTags.removeValue(forKey: key)
      } else {
        rowTags[key] = rowTagSet
      }
    }
  }

  public func processMoveOut(patterns: [MovePattern]) -> Set<String> {
    lock.withLock {
      guard let tagLength else { return [] }

      let taggedMode = resolveTaggedMode()
      var keysToDelete = Set<String>()

      for pattern in patterns {
        guard pattern.pos >= 0, pattern.pos < tagLength else { continue }
        let candidates = moveCandidates(
          position: pattern.pos,
          value: pattern.value,
          taggedMode: taggedMode
        )
        guard !candidates.isEmpty else { continue }

        for key in candidates {
          guard var rowTagSet = rowTags[key] else { continue }

          // DNF mode: deactivate the addressed condition and evaluate row
          // visibility. Tag index entries are preserved while any disjunct
          // remains visible so a later move-in can re-activate positions.
          if var activeConditions = rowActiveConditions[key], let disjunctPositions {
            if pattern.pos < activeConditions.count {
              activeConditions[pattern.pos] = false
            }
            rowActiveConditions[key] = activeConditions

            if !Self.rowVisible(
              activeConditions: activeConditions,
              disjunctPositions: disjunctPositions
            ) {
              for tag in rowTagSet {
                removeTagFromIndex(tag: parseTag(tag), key: key, taggedMode: taggedMode)
                tagCache.removeValue(forKey: tag)
              }
              rowTags.removeValue(forKey: key)
              rowActiveConditions.removeValue(forKey: key)
              keysToDelete.insert(key)
            }
            continue
          }

          // Simple shape (no active_conditions): remove matching tags and
          // evict when the tag set becomes empty. Legacy mode additionally
          // matches the `_` wildcard segment; tagged mode matches exact
          // segment values only and an empty (non-participating) segment
          // never matches.
          var removedAny = false
          var tagsToRemove: [String] = []
          for tag in rowTagSet {
            let parsed = parseTag(tag)
            guard pattern.pos < parsed.count else { continue }
            if segmentMatchesPattern(
              parsed[pattern.pos],
              value: pattern.value,
              taggedMode: taggedMode
            ) {
              tagsToRemove.append(tag)
            }
          }

          if !tagsToRemove.isEmpty {
            for tag in tagsToRemove {
              rowTagSet.remove(tag)
              removedAny = true
              removeTagFromIndex(tag: parseTag(tag), key: key, taggedMode: taggedMode)
            }
          }

          if removedAny {
            if rowTagSet.isEmpty {
              rowTags.removeValue(forKey: key)
              rowActiveConditions.removeValue(forKey: key)
              keysToDelete.insert(key)
            } else {
              rowTags[key] = rowTagSet
            }
          }
        }
      }

      return keysToDelete
    }
  }

  /// Silently re-activates condition positions for rows the tag index still
  /// holds (TanStack `processMoveInEvent`). Emits nothing: a fully removed row
  /// returns only through a later Electric change message.
  public func processMoveIn(patterns: [MovePattern]) {
    lock.withLock {
      guard let tagLength else { return }

      let taggedMode = resolveTaggedMode()
      for pattern in patterns {
        guard pattern.pos >= 0, pattern.pos < tagLength else { continue }
        let candidates = moveCandidates(
          position: pattern.pos,
          value: pattern.value,
          taggedMode: taggedMode
        )
        guard !candidates.isEmpty else { continue }

        for key in candidates {
          guard var activeConditions = rowActiveConditions[key] else { continue }
          if pattern.pos < activeConditions.count {
            activeConditions[pattern.pos] = true
            rowActiveConditions[key] = activeConditions
          }
        }
      }
    }
  }

  func tagsByRowKey() -> [String: [String]] {
    lock.withLock {
      rowTags.mapValues { $0.sorted() }
    }
  }

  func tags(for key: String) -> [String]? {
    lock.withLock {
      rowTags[key]?.sorted()
    }
  }

  func activeConditions(for key: String) -> [Bool]? {
    lock.withLock {
      rowActiveConditions[key]
    }
  }

  /// A row is visible when ANY disjunct has ALL of its participating positions
  /// active (TanStack `rowVisible`).
  static func rowVisible(
    activeConditions: [Bool],
    disjunctPositions: [[Int]]
  ) -> Bool {
    disjunctPositions.contains { positions in
      positions.allSatisfy { position in
        position < activeConditions.count && activeConditions[position]
      }
    }
  }

  /// For each tag (= disjunct), the indices of participating positions
  /// (TanStack `deriveDisjunctPositions`). Empty (non-participating) segments
  /// are excluded.
  static func deriveDisjunctPositions(_ parsedTags: [[String]]) -> [[Int]] {
    parsedTags.map { parsed in
      parsed.enumerated().compactMap { position, segment in
        segment.isEmpty ? nil : position
      }
    }
  }

  /// The segment semantics (legacy `_` wildcard versus tagged-shape
  /// NON_PARTICIPATING empty segments) are pinned for one tracker generation
  /// at the first fold and only re-resolved after `reset()`.
  private func resolveTaggedMode() -> Bool {
    if let resolvedTaggedMode { return resolvedTaggedMode }
    let taggedMode = isTaggedShapeProtocolEnabled()
    resolvedTaggedMode = taggedMode
    return taggedMode
  }

  private func segmentMatchesPattern(_ segment: String, value: String, taggedMode: Bool) -> Bool {
    if taggedMode {
      return !segment.isEmpty && segment == value
    }
    return segment == value || segment == "_"
  }

  private func isIndexedSegment(_ segment: String, taggedMode: Bool) -> Bool {
    taggedMode ? !segment.isEmpty : segment != "_"
  }

  /// Candidate rows for a move pattern at one position. The value-keyed index
  /// supplies exact matches; legacy mode additionally unions the wildcard
  /// bucket so a row whose tag carries "_" AT the pattern's position is
  /// reachable and can match through the wildcard branch of
  /// `segmentMatchesPattern` ("_" matches removals while staying out of the
  /// value-keyed index).
  private func moveCandidates(position: Int, value: String, taggedMode: Bool) -> Set<String> {
    var candidates = tagIndex[position][value] ?? []
    if !taggedMode, position < wildcardRowsByPosition.count {
      candidates.formUnion(wildcardRowsByPosition[position].keys)
    }
    return candidates
  }

  /// Electric joins a membership tag's segments with "/" (sync-service
  /// `shape.ex` `Enum.join("/")`), and TanStack DB's `tag-index.parseTag`
  /// splits on the same delimiter. Segment value-hashes are "/"-free, so a
  /// plain split is correct (no escaping needed).
  private func parseTag(_ tag: String) -> [String] {
    if let cached = tagCache[tag] { return cached }

    let parts = tag.split(separator: "/", omittingEmptySubsequences: false)
      .map(String.init)

    tagCache[tag] = parts
    return parts
  }

  private func addTagToIndex(tag: [String], key: String, taggedMode: Bool) {
    guard let tagLength else { return }

    for pos in 0..<tagLength {
      let value = tag[pos]
      guard isIndexedSegment(value, taggedMode: taggedMode) else {
        if !taggedMode, value == "_", pos < wildcardRowsByPosition.count {
          wildcardRowsByPosition[pos][key, default: 0] += 1
        }
        continue
      }
      var set = tagIndex[pos][value] ?? Set<String>()
      set.insert(key)
      tagIndex[pos][value] = set
    }
  }

  private func removeTagFromIndex(tag: [String], key: String, taggedMode: Bool) {
    guard let tagLength else { return }

    for pos in 0..<tagLength {
      let value = tag[pos]
      guard isIndexedSegment(value, taggedMode: taggedMode) else {
        if !taggedMode, value == "_", pos < wildcardRowsByPosition.count {
          let count = wildcardRowsByPosition[pos][key] ?? 0
          if count <= 1 {
            wildcardRowsByPosition[pos][key] = nil
          } else {
            wildcardRowsByPosition[pos][key] = count - 1
          }
        }
        continue
      }

      guard var set = tagIndex[pos][value] else { continue }
      set.remove(key)
      if set.isEmpty {
        tagIndex[pos].removeValue(forKey: value)
      } else {
        tagIndex[pos][value] = set
      }
    }
  }

  private func clearTagsForRow(key: String) {
    rowActiveConditions.removeValue(forKey: key)
    guard let existing = rowTags.removeValue(forKey: key) else { return }
    let taggedMode = resolveTaggedMode()
    for tag in existing {
      removeTagFromIndex(tag: parseTag(tag), key: key, taggedMode: taggedMode)
      tagCache.removeValue(forKey: tag)
    }
  }
}
