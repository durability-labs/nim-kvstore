import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable/results

import pkg/kvstore

proc moveTests*(ds: KVStore, key: Key) =
  ## Tests for moveKeysAtomic and dropPrefix operations.

  suite "Move Operations":
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
      check result.error of KVConflictError

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
      check result.error of KVConflictError

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
      check result.error of KVConflictError

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

    # =========================================================================
    # dropPrefix tests
    # =========================================================================

    test "dropPrefix deletes all keys under a prefix":
      let prefix = (key / "drop" / "tree").tryGet()

      (await ds.put((prefix / "a").tryGet(), "va".toBytes)).tryGet()
      (await ds.put((prefix / "b").tryGet(), "vb".toBytes)).tryGet()
      (await ds.put((prefix / "nested" / "c").tryGet(), "vc".toBytes)).tryGet()

      (await ds.dropPrefix(prefix)).tryGet()

      check not (await ds.has((prefix / "a").tryGet())).tryGet()
      check not (await ds.has((prefix / "b").tryGet())).tryGet()
      check not (await ds.has((prefix / "nested" / "c").tryGet())).tryGet()

    test "dropPrefix also deletes the prefix key itself":
      let prefix = (key / "dropself" / "node").tryGet()

      (await ds.put(prefix, "self".toBytes)).tryGet()
      (await ds.put((prefix / "child").tryGet(), "child".toBytes)).tryGet()

      (await ds.dropPrefix(prefix)).tryGet()

      check not (await ds.has(prefix)).tryGet()
      check not (await ds.has((prefix / "child").tryGet())).tryGet()

    test "dropPrefix is idempotent on empty prefix":
      let prefix = (key / "dropempty" / "nothing").tryGet()

      (await ds.dropPrefix(prefix)).tryGet() # no-op
      (await ds.dropPrefix(prefix)).tryGet() # still no-op

    test "dropPrefix does not affect keys outside the prefix":
      let
        prefix = (key / "dropscope" / "target").tryGet()
        outside = (key / "dropscope" / "keep").tryGet()

      (await ds.put((prefix / "x").tryGet(), "gone".toBytes)).tryGet()
      (await ds.put(outside, "safe".toBytes)).tryGet()

      (await ds.dropPrefix(prefix)).tryGet()

      check not (await ds.has((prefix / "x").tryGet())).tryGet()
      check (await ds.get(outside)).tryGet().val == "safe".toBytes

    test "dropPrefix multi drops multiple prefixes atomically":
      let
        p1 = (key / "dropmulti" / "p1").tryGet()
        p2 = (key / "dropmulti" / "p2").tryGet()

      (await ds.put((p1 / "a").tryGet(), "a".toBytes)).tryGet()
      (await ds.put((p1 / "b").tryGet(), "b".toBytes)).tryGet()
      (await ds.put((p2 / "x").tryGet(), "x".toBytes)).tryGet()

      (await ds.dropPrefix(@[p1, p2])).tryGet()

      check not (await ds.has((p1 / "a").tryGet())).tryGet()
      check not (await ds.has((p1 / "b").tryGet())).tryGet()
      check not (await ds.has((p2 / "x").tryGet())).tryGet()

    test "dropPrefix multi with empty list is a no-op":
      let prefixes: seq[Key] = @[]
      (await ds.dropPrefix(prefixes)).tryGet()
