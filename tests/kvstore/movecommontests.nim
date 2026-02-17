import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable/results

import pkg/kvstore

proc moveTests*(ds: KVStore, key: Key) =
  ## Tests for moveKeys and moveKeysAtomic operations.

  suite "Move Operations":
    test "moveKeys moves multiple records under a prefix":
      let
        oldPrefix = (key / "move" / "old").tryGet()
        newPrefix = (key / "move" / "new").tryGet()
        k1 = (oldPrefix / "rec1").tryGet()
        k2 = (oldPrefix / "rec2").tryGet()
        k3 = (oldPrefix / "rec3").tryGet()

      (await ds.put(k1, "v1".toBytes)).tryGet()
      (await ds.put(k2, "v2".toBytes)).tryGet()
      (await ds.put(k3, "v3".toBytes)).tryGet()

      let conflicts = (await ds.moveKeys(oldPrefix, newPrefix)).tryGet()
      check conflicts.len == 0

      # Old keys should be gone
      check not (await ds.has(k1)).tryGet()
      check not (await ds.has(k2)).tryGet()
      check not (await ds.has(k3)).tryGet()

      # New keys should exist with correct values
      let
        nk1 = (newPrefix / "rec1").tryGet()
        nk2 = (newPrefix / "rec2").tryGet()
        nk3 = (newPrefix / "rec3").tryGet()

      check (await ds.get(nk1)).tryGet().val == "v1".toBytes
      check (await ds.get(nk2)).tryGet().val == "v2".toBytes
      check (await ds.get(nk3)).tryGet().val == "v3".toBytes

    test "moveKeys preserves record values":
      let
        oldPrefix = (key / "moveval" / "old").tryGet()
        newPrefix = (key / "moveval" / "new").tryGet()

      let largeData = newSeq[byte](4096)
      (await ds.put((oldPrefix / "large").tryGet(), largeData)).tryGet()
      (await ds.put((oldPrefix / "empty").tryGet(), newSeq[byte]())).tryGet()

      let conflicts = (await ds.moveKeys(oldPrefix, newPrefix)).tryGet()
      check conflicts.len == 0

      check (await ds.get((newPrefix / "large").tryGet())).tryGet().val == largeData
      check (await ds.get((newPrefix / "empty").tryGet())).tryGet().val.len == 0

    test "moveKeys with no matching records succeeds":
      let
        oldPrefix = (key / "moveempty" / "nonexistent").tryGet()
        newPrefix = (key / "moveempty" / "dest").tryGet()

      let conflicts = (await ds.moveKeys(oldPrefix, newPrefix)).tryGet()
      check conflicts.len == 0

    test "moveKeys moves a single record":
      let
        oldPrefix = (key / "movesingle" / "old").tryGet()
        newPrefix = (key / "movesingle" / "new").tryGet()
        k = (oldPrefix / "only").tryGet()

      (await ds.put(k, "solo".toBytes)).tryGet()

      let conflicts = (await ds.moveKeys(oldPrefix, newPrefix)).tryGet()
      check conflicts.len == 0

      check not (await ds.has(k)).tryGet()
      check (await ds.get((newPrefix / "only").tryGet())).tryGet().val == "solo".toBytes

    test "moveKeys does not affect records outside the prefix":
      let
        oldPrefix = (key / "movescope" / "old").tryGet()
        newPrefix = (key / "movescope" / "new").tryGet()
        outsideKey = (key / "movescope" / "outside").tryGet()

      (await ds.put((oldPrefix / "inside").tryGet(), "in".toBytes)).tryGet()
      (await ds.put(outsideKey, "out".toBytes)).tryGet()

      let conflicts = (await ds.moveKeys(oldPrefix, newPrefix)).tryGet()
      check conflicts.len == 0

      # Outside key should be untouched
      check (await ds.get(outsideKey)).tryGet().val == "out".toBytes

      # Inside key moved
      check not (await ds.has((oldPrefix / "inside").tryGet())).tryGet()
      check (await ds.get((newPrefix / "inside").tryGet())).tryGet().val == "in".toBytes

    test "moveKeys preserves nested key structure":
      let
        oldPrefix = (key / "movenest" / "old").tryGet()
        newPrefix = (key / "movenest" / "new").tryGet()

      (await ds.put((oldPrefix / "a" / "b").tryGet(), "ab".toBytes)).tryGet()
      (await ds.put((oldPrefix / "a" / "c").tryGet(), "ac".toBytes)).tryGet()
      (await ds.put((oldPrefix / "d").tryGet(), "d".toBytes)).tryGet()

      let conflicts = (await ds.moveKeys(oldPrefix, newPrefix)).tryGet()
      check conflicts.len == 0

      check (await ds.get((newPrefix / "a" / "b").tryGet())).tryGet().val == "ab".toBytes
      check (await ds.get((newPrefix / "a" / "c").tryGet())).tryGet().val == "ac".toBytes
      check (await ds.get((newPrefix / "d").tryGet())).tryGet().val == "d".toBytes

    test "moveKeysAtomic succeeds with no conflicts":
      let
        oldPrefix = (key / "moveatomic" / "old").tryGet()
        newPrefix = (key / "moveatomic" / "new").tryGet()

      (await ds.put((oldPrefix / "a").tryGet(), "va".toBytes)).tryGet()
      (await ds.put((oldPrefix / "b").tryGet(), "vb".toBytes)).tryGet()

      (await ds.moveKeysAtomic(oldPrefix, newPrefix)).tryGet()

      check not (await ds.has((oldPrefix / "a").tryGet())).tryGet()
      check not (await ds.has((oldPrefix / "b").tryGet())).tryGet()
      check (await ds.get((newPrefix / "a").tryGet())).tryGet().val == "va".toBytes
      check (await ds.get((newPrefix / "b").tryGet())).tryGet().val == "vb".toBytes

    test "moveKeysAtomic fails if destination key exists":
      let
        oldPrefix = (key / "moveconflict" / "old").tryGet()
        newPrefix = (key / "moveconflict" / "new").tryGet()

      (await ds.put((oldPrefix / "a").tryGet(), "from-old".toBytes)).tryGet()
      # Pre-populate destination
      (await ds.put((newPrefix / "a").tryGet(), "existing".toBytes)).tryGet()

      let result = await ds.moveKeysAtomic(oldPrefix, newPrefix)
      check result.isErr

      # Source should still exist (rolled back)
      check (await ds.get((oldPrefix / "a").tryGet())).tryGet().val == "from-old".toBytes
      # Destination should be unchanged
      check (await ds.get((newPrefix / "a").tryGet())).tryGet().val == "existing".toBytes

    test "moveKeysAtomic is all-or-nothing on partial conflict":
      let
        oldPrefix = (key / "moverollback" / "old").tryGet()
        newPrefix = (key / "moverollback" / "new").tryGet()

      (await ds.put((oldPrefix / "a").tryGet(), "va".toBytes)).tryGet()
      (await ds.put((oldPrefix / "b").tryGet(), "vb".toBytes)).tryGet()
      # Only conflict on "b"
      (await ds.put((newPrefix / "b").tryGet(), "existing-b".toBytes)).tryGet()

      let result = await ds.moveKeysAtomic(oldPrefix, newPrefix)
      check result.isErr

      # Both source keys should still exist (all-or-nothing rollback)
      check (await ds.get((oldPrefix / "a").tryGet())).tryGet().val == "va".toBytes
      check (await ds.get((oldPrefix / "b").tryGet())).tryGet().val == "vb".toBytes
      # "a" should NOT have been moved to destination
      check not (await ds.has((newPrefix / "a").tryGet())).tryGet()
      # Existing destination key should be unchanged
      check (await ds.get((newPrefix / "b").tryGet())).tryGet().val ==
        "existing-b".toBytes

    test "moveKeysAtomic with empty prefix succeeds":
      let
        oldPrefix = (key / "moveatomicempty" / "none").tryGet()
        newPrefix = (key / "moveatomicempty" / "dest").tryGet()

      (await ds.moveKeysAtomic(oldPrefix, newPrefix)).tryGet()

    test "moved records are queryable under new prefix":
      let
        oldPrefix = (key / "movequery" / "old").tryGet()
        newPrefix = (key / "movequery" / "new").tryGet()

      for i in 0 ..< 5:
        (await ds.put((oldPrefix / $i).tryGet(), ($i).toBytes)).tryGet()

      (await ds.moveKeysAtomic(oldPrefix, newPrefix)).tryGet()

      # Query under new prefix should find all records
      let q = Query.init(Key.init(newPrefix.id & "/*").tryGet(), value = true)
      let iter = (await ds.query(q)).tryGet()
      defer:
        (await iter.dispose()).tryGet()

      var count = 0
      while not iter.finished:
        let maybeRecord = (await iter.next()).tryGet()
        if record =? maybeRecord:
          count += 1
      check count == 5

      # Query under old prefix should find nothing
      let qOld = Query.init(Key.init(oldPrefix.id & "/*").tryGet(), value = true)
      let iterOld = (await ds.query(qOld)).tryGet()
      defer:
        (await iterOld.dispose()).tryGet()

      var oldCount = 0
      while not iterOld.finished:
        let maybeRecord = (await iterOld.next()).tryGet()
        if record =? maybeRecord:
          oldCount += 1
      check oldCount == 0

    # =========================================================================
    # Exact key (self) matching tests
    # =========================================================================

    test "moveKeys moves the prefix key itself (not just children)":
      let
        oldPrefix = (key / "moveself" / "old").tryGet()
        newPrefix = (key / "moveself" / "new").tryGet()

      # Store a record AT the prefix key (no child segment)
      (await ds.put(oldPrefix, "self-value".toBytes)).tryGet()

      let conflicts = (await ds.moveKeys(oldPrefix, newPrefix)).tryGet()
      check conflicts.len == 0

      check not (await ds.has(oldPrefix)).tryGet()
      check (await ds.get(newPrefix)).tryGet().val == "self-value".toBytes

    test "moveKeys moves both prefix key and children":
      let
        oldPrefix = (key / "moveboth" / "old").tryGet()
        newPrefix = (key / "moveboth" / "new").tryGet()

      # Store at the prefix key itself AND children
      (await ds.put(oldPrefix, "self".toBytes)).tryGet()
      (await ds.put((oldPrefix / "child1").tryGet(), "c1".toBytes)).tryGet()
      (await ds.put((oldPrefix / "child2").tryGet(), "c2".toBytes)).tryGet()

      let conflicts = (await ds.moveKeys(oldPrefix, newPrefix)).tryGet()
      check conflicts.len == 0

      # All old keys gone
      check not (await ds.has(oldPrefix)).tryGet()
      check not (await ds.has((oldPrefix / "child1").tryGet())).tryGet()
      check not (await ds.has((oldPrefix / "child2").tryGet())).tryGet()

      # All moved to new prefix
      check (await ds.get(newPrefix)).tryGet().val == "self".toBytes
      check (await ds.get((newPrefix / "child1").tryGet())).tryGet().val == "c1".toBytes
      check (await ds.get((newPrefix / "child2").tryGet())).tryGet().val == "c2".toBytes

    test "moveKeysAtomic moves the prefix key itself":
      let
        oldPrefix = (key / "moveatomicself" / "old").tryGet()
        newPrefix = (key / "moveatomicself" / "new").tryGet()

      (await ds.put(oldPrefix, "atomic-self".toBytes)).tryGet()

      (await ds.moveKeysAtomic(oldPrefix, newPrefix)).tryGet()

      check not (await ds.has(oldPrefix)).tryGet()
      check (await ds.get(newPrefix)).tryGet().val == "atomic-self".toBytes

    # =========================================================================
    # Multi-prefix move tests
    # =========================================================================

    test "moveKeysAtomic multi-prefix moves multiple prefix pairs atomically":
      let
        oldA = (key / "movemulti" / "oldA").tryGet()
        newA = (key / "movemulti" / "newA").tryGet()
        oldB = (key / "movemulti" / "oldB").tryGet()
        newB = (key / "movemulti" / "newB").tryGet()

      (await ds.put((oldA / "1").tryGet(), "a1".toBytes)).tryGet()
      (await ds.put((oldA / "2").tryGet(), "a2".toBytes)).tryGet()
      (await ds.put((oldB / "x").tryGet(), "bx".toBytes)).tryGet()

      (await ds.moveKeysAtomic(@[(oldA, newA), (oldB, newB)])).tryGet()

      # All old keys gone
      check not (await ds.has((oldA / "1").tryGet())).tryGet()
      check not (await ds.has((oldA / "2").tryGet())).tryGet()
      check not (await ds.has((oldB / "x").tryGet())).tryGet()

      # All moved to new prefixes
      check (await ds.get((newA / "1").tryGet())).tryGet().val == "a1".toBytes
      check (await ds.get((newA / "2").tryGet())).tryGet().val == "a2".toBytes
      check (await ds.get((newB / "x").tryGet())).tryGet().val == "bx".toBytes

    test "moveKeysAtomic multi-prefix rolls back all on conflict":
      let
        oldA = (key / "movemultirollback" / "oldA").tryGet()
        newA = (key / "movemultirollback" / "newA").tryGet()
        oldB = (key / "movemultirollback" / "oldB").tryGet()
        newB = (key / "movemultirollback" / "newB").tryGet()

      (await ds.put((oldA / "1").tryGet(), "a1".toBytes)).tryGet()
      (await ds.put((oldB / "x").tryGet(), "bx".toBytes)).tryGet()
      # Pre-populate destination for B — causes conflict on second pair
      (await ds.put((newB / "x").tryGet(), "existing".toBytes)).tryGet()

      let result = await ds.moveKeysAtomic(@[(oldA, newA), (oldB, newB)])
      check result.isErr

      # ALL source keys should still exist (entire transaction rolled back)
      check (await ds.get((oldA / "1").tryGet())).tryGet().val == "a1".toBytes
      check (await ds.get((oldB / "x").tryGet())).tryGet().val == "bx".toBytes
      # Pair A should NOT have been moved (rolled back)
      check not (await ds.has((newA / "1").tryGet())).tryGet()
      # Existing destination key unchanged
      check (await ds.get((newB / "x").tryGet())).tryGet().val == "existing".toBytes

    test "moveKeysAtomic multi-prefix with exact key and children":
      let
        oldLeafs = (key / "movefinalize" / "leafs" / "tmp").tryGet()
        newLeafs = (key / "movefinalize" / "leafs" / "real").tryGet()
        oldMeta = (key / "movefinalize" / "meta" / "tmp").tryGet()
        newMeta = (key / "movefinalize" / "meta" / "real").tryGet()

      # Simulate overlay finalization: leafs are children, metadata is exact key
      (await ds.put((oldLeafs / "0").tryGet(), "leaf0".toBytes)).tryGet()
      (await ds.put((oldLeafs / "1").tryGet(), "leaf1".toBytes)).tryGet()
      (await ds.put((oldLeafs / "2").tryGet(), "leaf2".toBytes)).tryGet()
      (await ds.put(oldMeta, "overlay-metadata".toBytes)).tryGet()

      (await ds.moveKeysAtomic(@[(oldLeafs, newLeafs), (oldMeta, newMeta)])).tryGet()

      # All old keys gone
      check not (await ds.has((oldLeafs / "0").tryGet())).tryGet()
      check not (await ds.has((oldLeafs / "1").tryGet())).tryGet()
      check not (await ds.has((oldLeafs / "2").tryGet())).tryGet()
      check not (await ds.has(oldMeta)).tryGet()

      # All at new locations
      check (await ds.get((newLeafs / "0").tryGet())).tryGet().val == "leaf0".toBytes
      check (await ds.get((newLeafs / "1").tryGet())).tryGet().val == "leaf1".toBytes
      check (await ds.get((newLeafs / "2").tryGet())).tryGet().val == "leaf2".toBytes
      check (await ds.get(newMeta)).tryGet().val == "overlay-metadata".toBytes

    test "moveKeysAtomic multi-prefix with empty list succeeds":
      let moves: seq[(Key, Key)] = @[]
      (await ds.moveKeysAtomic(moves)).tryGet()

    test "moveKeysAtomic multi-prefix single pair delegates correctly":
      let
        oldPrefix = (key / "movemultisingle" / "old").tryGet()
        newPrefix = (key / "movemultisingle" / "new").tryGet()

      (await ds.put((oldPrefix / "a").tryGet(), "va".toBytes)).tryGet()

      (await ds.moveKeysAtomic(@[(oldPrefix, newPrefix)])).tryGet()

      check not (await ds.has((oldPrefix / "a").tryGet())).tryGet()
      check (await ds.get((newPrefix / "a").tryGet())).tryGet().val == "va".toBytes
