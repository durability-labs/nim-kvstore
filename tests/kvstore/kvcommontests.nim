import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable/results

import pkg/kvstore
import pkg/kvstore/kvstore

proc basicStoreTests*(ds: KVStore, key: Key, bytes: seq[byte], otherBytes: seq[byte]) =
  var record: RawRecord
  test "put":
    (await ds.put(key, bytes)).tryGet()

  test "get":
    record = (await ds.get(key)).tryGet()
    check:
      record.val == bytes
      record.token == 1

  test "put update":
    record.val = otherBytes
    (await ds.put(record)).tryGet()

  test "get updated":
    record = (await ds.get(key)).tryGet()
    check:
      record.val == otherBytes
      record.token == 2

  test "delete":
    (await ds.delete(KeyRecord.init(key, record.token))).tryGet()

  test "contains":
    check:
      not await (key in ds)

  var records: seq[RawRecord]

  test "put many":
    for k in 0 ..< 100:
      records.add(RawRecord.init(Key.init(key.id, $k).tryGet, @[k.byte]))

    check (await ds.put(records)).tryGet.len == 0 # 0 means we've inserted all records

    for k in records:
      check:
        (await ds.has(k.key)).tryGet

  test "get many":
    let fetched = (await ds.get(records.mapIt(it.key))).tryGet

    check fetched.len == records.len

    for r in records:
      let f = fetched.filterIt(it.key == r.key)[0]
      check:
        f.val == r.val
        f.token == 1

    records = fetched

  test "delete records":
    let skipped =
      (await ds.delete(records.mapIt(KeyRecord.init(it.key, it.token)))).tryGet
    check skipped.len == 0 # all deletions should succeed

    for k in records:
      check:
        not (await ds.has(k.key)).tryGet

  test "put detects stale token conflicts":
    let conflictKey = (key / "conflict").tryGet()
    (await ds.put(RawRecord.init(conflictKey, "initial".toBytes))).tryGet()

    let current = (await ds.get(conflictKey)).tryGet()

    let fresh = RawRecord.init(conflictKey, "fresh".toBytes, current.token)
    check (await ds.put(@[fresh])).tryGet.len == 0

    let updated = (await ds.get(conflictKey)).tryGet()
    check:
      updated.val == "fresh".toBytes
      updated.token == current.token + 1

    let stale = RawRecord.init(conflictKey, "stale".toBytes, current.token)
    let skipped = (await ds.put(@[stale])).tryGet()
    check skipped.len == 1
    check skipped[0] == conflictKey

    let afterConflict = (await ds.get(conflictKey)).tryGet()
    check:
      afterConflict.val == "fresh".toBytes
      afterConflict.token == updated.token

  test "delete ignores conflicting tokens":
    let deleteKey = ((key / "delete").tryGet() / "conflict").tryGet()
    (await ds.put(RawRecord.init(deleteKey, "value".toBytes))).tryGet()
    let current = (await ds.get(deleteKey)).tryGet()

    let staleDelete = KeyRecord.init(deleteKey, current.token - 1)
    let skippedStale = (await ds.delete(@[staleDelete])).tryGet
    check skippedStale.len == 1 # stale token should be skipped
    check skippedStale[0] == deleteKey

    let skippedCurrent =
      (await ds.delete(@[KeyRecord.init(current.key, current.token)])).tryGet
    check skippedCurrent.len == 0 # current token should succeed
    check not (await ds.has(deleteKey)).tryGet()

proc atomicBatchTests*(ds: KVStore, key: Key, supportsAtomic: bool) =
  ## Tests for atomic batch API behavior.
  ## Pass supportsAtomic=true for SQLite, false for FSKVStore.

  test "supportsAtomicBatch returns expected value":
    check ds.supportsAtomicBatch() == supportsAtomic

  if supportsAtomic:
    # Tests for backends that support atomic batch

    test "putAtomic - all succeed when no conflicts":
      let k1 = (key / "atomic" / "put1").tryGet()
      let k2 = (key / "atomic" / "put2").tryGet()

      let conflicts = (
        await ds.putAtomic(
          @[RawRecord.init(k1, @[1'u8], 0), RawRecord.init(k2, @[2'u8], 0)]
        )
      ).tryGet()

      check conflicts.len == 0
      check (await ds.has(k1)).tryGet()
      check (await ds.has(k2)).tryGet()

    test "putAtomic - rollback on any conflict":
      let k1 = (key / "atomic" / "rollback1").tryGet()
      let k2 = (key / "atomic" / "rollback2").tryGet()

      # Insert k1 first
      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()

      # Try atomic batch with k1 (wrong token) and k2 (new)
      let conflicts = (
        await ds.putAtomic(
          @[
            RawRecord.init(k1, @[10'u8], 999), # Wrong token
            RawRecord.init(k2, @[20'u8], 0), # Would succeed alone
          ]
        )
      ).tryGet()

      check conflicts.len == 1
      check conflicts[0] == k1
      # k2 should NOT exist (rollback)
      check (await ds.get(k2)).isErr

    test "deleteAtomic - all succeed when tokens match":
      let k1 = (key / "atomic" / "del1").tryGet()
      let k2 = (key / "atomic" / "del2").tryGet()

      # Insert both
      discard (
        await ds.put(@[RawRecord.init(k1, @[1'u8], 0), RawRecord.init(k2, @[2'u8], 0)])
      ).tryGet()

      let r1 = (await ds.get(k1)).tryGet()
      let r2 = (await ds.get(k2)).tryGet()

      let conflicts = (
        await ds.deleteAtomic(
          @[KeyRecord.init(k1, r1.token), KeyRecord.init(k2, r2.token)]
        )
      ).tryGet()

      check conflicts.len == 0
      check (await ds.get(k1)).isErr
      check (await ds.get(k2)).isErr
  else:
    # Tests for backends that don't support atomic batch

    test "putAtomic - returns error for unsupported backend":
      let k1 = (key / "atomic" / "unsupported").tryGet()
      let result = await ds.putAtomic(@[RawRecord.init(k1, @[1'u8], 0)])
      check result.isErr

    test "deleteAtomic - returns error for unsupported backend":
      let k1 = (key / "atomic" / "unsupported").tryGet()
      let result = await ds.deleteAtomic(@[KeyRecord.init(k1, 1)])
      check result.isErr

proc atomicRetryHelperTests*(ds: KVStore, key: Key) =
  ## Tests for atomic retry helpers (tryPutAtomic, tryDeleteAtomic).
  ## Only run on backends that support atomic batch.

  if not ds.supportsAtomicBatch():
    return

  test "tryPutAtomic - succeeds when no conflicts":
    let k1 = (key / "tryatomic" / "put1").tryGet()
    let k2 = (key / "tryatomic" / "put2").tryGet()

    let result = await ds.tryPutAtomic(
      @[RawRecord.init(k1, "v1".toBytes, 0), RawRecord.init(k2, "v2".toBytes, 0)]
    )
    check result.isOk
    check (await ds.has(k1)).tryGet()
    check (await ds.has(k2)).tryGet()

  test "tryPutAtomic - fails without middleware on conflict":
    let k1 = (key / "tryatomic" / "noMw1").tryGet()
    let k2 = (key / "tryatomic" / "noMw2").tryGet()

    # Insert k1 first
    (await ds.put(RawRecord.init(k1, "existing".toBytes, 0))).tryGet()

    let result = await ds.tryPutAtomic(
      @[
        RawRecord.init(k1, "v1".toBytes, 0), # Conflict
        RawRecord.init(k2, "v2".toBytes, 0), # Would succeed alone
      ],
      middleware = nil,
    )

    check result.isErr
    # k2 should NOT be committed (all-or-nothing)
    check (await ds.get(k2)).isErr

  test "tryPutAtomic - middleware resolves conflicts":
    let k1 = (key / "tryatomic" / "mw1").tryGet()
    let k2 = (key / "tryatomic" / "mw2").tryGet()

    # Insert k1 first
    (await ds.put(RawRecord.init(k1, "existing".toBytes, 0))).tryGet()

    var middlewareCalled = false
    let middleware = proc(
        all: seq[RawRecord], conflicts: seq[Key]
    ): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
      middlewareCalled = true
      let fresh = ?(await ds.get(conflicts))
      var updated: seq[RawRecord]
      for rec in all:
        if rec.key in conflicts:
          for f in fresh:
            if f.key == rec.key:
              updated.add(RawRecord.init(rec.key, rec.val, f.token))
              break
        else:
          updated.add(rec)
      success(updated)

    let result = await ds.tryPutAtomic(
      @[RawRecord.init(k1, "updated".toBytes, 0), RawRecord.init(k2, "v2".toBytes, 0)],
      middleware = middleware,
    )

    check result.isOk
    check middlewareCalled
    let rec1 = (await ds.get(k1)).tryGet()
    let rec2 = (await ds.get(k2)).tryGet()
    check rec1.val == "updated".toBytes
    check rec2.val == "v2".toBytes

  test "tryPutAtomic - fails after max retries":
    let k1 = (key / "tryatomic" / "maxRetry").tryGet()

    (await ds.put(RawRecord.init(k1, "existing".toBytes, 0))).tryGet()

    let badMiddleware = proc(
        all: seq[RawRecord], conflicts: seq[Key]
    ): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
      success(all) # No fix

    let result = await ds.tryPutAtomic(
      @[RawRecord.init(k1, "v1".toBytes, 0)], maxRetries = 2, middleware = badMiddleware
    )

    check result.isErr
    check result.error of KVStoreMaxRetriesError

  test "tryPutAtomic - single record succeeds":
    let k1 = (key / "tryatomic" / "single").tryGet()
    let result = await ds.tryPutAtomic(RawRecord.init(k1, "v1".toBytes, 0))
    check result.isOk

  test "tryPutAtomic - empty batch succeeds":
    let result = await ds.tryPutAtomic(newSeq[RawRecord]())
    check result.isOk

  test "tryDeleteAtomic - succeeds when tokens match":
    let k1 = (key / "tryatomic" / "del1").tryGet()
    let k2 = (key / "tryatomic" / "del2").tryGet()

    discard (
      await ds.put(
        @[RawRecord.init(k1, "v1".toBytes, 0), RawRecord.init(k2, "v2".toBytes, 0)]
      )
    ).tryGet()

    let r1 = (await ds.get(k1)).tryGet()
    let r2 = (await ds.get(k2)).tryGet()

    let result = await ds.tryDeleteAtomic(
      @[KeyRecord.init(k1, r1.token), KeyRecord.init(k2, r2.token)]
    )

    check result.isOk
    check (await ds.get(k1)).isErr
    check (await ds.get(k2)).isErr

  test "tryDeleteAtomic - fails without middleware on conflict":
    let k1 = (key / "tryatomic" / "delNoMw1").tryGet()
    let k2 = (key / "tryatomic" / "delNoMw2").tryGet()

    discard (
      await ds.put(
        @[RawRecord.init(k1, "v1".toBytes, 0), RawRecord.init(k2, "v2".toBytes, 0)]
      )
    ).tryGet()

    let r1 = (await ds.get(k1)).tryGet()

    let result = await ds.tryDeleteAtomic(
      @[
        KeyRecord.init(k1, r1.token), KeyRecord.init(k2, 999'u64) # Wrong token
      ],
      middleware = nil,
    )

    check result.isErr
    # k1 should NOT be deleted (all-or-nothing)
    check (await ds.get(k1)).isOk

  test "tryDeleteAtomic - single record succeeds":
    let k1 = (key / "tryatomic" / "delSingle").tryGet()
    (await ds.put(RawRecord.init(k1, "v1".toBytes, 0))).tryGet()
    let rec = (await ds.get(k1)).tryGet()

    let result = await ds.tryDeleteAtomic(KeyRecord.init(k1, rec.token))
    check result.isOk

  test "tryDeleteAtomic - empty batch succeeds":
    let result = await ds.tryDeleteAtomic(newSeq[KeyRecord]())
    check result.isOk

proc hasBatchTests*(ds: KVStore, key: Key) =
  ## Tests for batch has operations.

  suite "Test Batch Has":
    test "has batch - all exist":
      let k1 = (key / "hasbatch" / "all1").tryGet()
      let k2 = (key / "hasbatch" / "all2").tryGet()
      let k3 = (key / "hasbatch" / "all3").tryGet()

      # Create keys
      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()
      (await ds.put(RawRecord.init(k2, @[2'u8], 0))).tryGet()
      (await ds.put(RawRecord.init(k3, @[3'u8], 0))).tryGet()

      # Check all exist
      let existing = (await ds.has(@[k1, k2, k3])).tryGet()
      check existing.len == 3
      check k1 in existing
      check k2 in existing
      check k3 in existing

    test "has batch - none exist":
      let k1 = (key / "hasbatch" / "none1").tryGet()
      let k2 = (key / "hasbatch" / "none2").tryGet()
      let k3 = (key / "hasbatch" / "none3").tryGet()

      # Don't create any keys, just check
      let existing = (await ds.has(@[k1, k2, k3])).tryGet()
      check existing.len == 0

    test "has batch - partial exist":
      let k1 = (key / "hasbatch" / "partial1").tryGet()
      let k2 = (key / "hasbatch" / "partial2").tryGet()
      let k3 = (key / "hasbatch" / "partial3").tryGet()

      # Create only k1 and k3
      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()
      (await ds.put(RawRecord.init(k3, @[3'u8], 0))).tryGet()

      # Check all three, only k1 and k3 should exist
      let existing = (await ds.has(@[k1, k2, k3])).tryGet()
      check existing.len == 2
      check k1 in existing
      check k2 notin existing
      check k3 in existing

    test "has batch - empty input":
      let existing = (await ds.has(newSeq[Key]())).tryGet()
      check existing.len == 0

    test "has batch - single key exists":
      let k1 = (key / "hasbatch" / "single1").tryGet()
      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()

      let existing = (await ds.has(@[k1])).tryGet()
      check existing.len == 1
      check k1 in existing

    test "has batch - single key missing":
      let k1 = (key / "hasbatch" / "singlemiss").tryGet()

      let existing = (await ds.has(@[k1])).tryGet()
      check existing.len == 0

    test "has single-key helper - exists":
      let k1 = (key / "hasbatch" / "helper1").tryGet()
      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()

      let exists = (await ds.has(k1)).tryGet()
      check exists == true

    test "has single-key helper - missing":
      let k1 = (key / "hasbatch" / "helpermiss").tryGet()

      let exists = (await ds.has(k1)).tryGet()
      check exists == false

    test "has batch - preserves Key identity":
      # Verify the returned keys are equal to the input keys
      let k1 = (key / "hasbatch" / "identity1").tryGet()
      let k2 = (key / "hasbatch" / "identity2").tryGet()

      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()
      (await ds.put(RawRecord.init(k2, @[2'u8], 0))).tryGet()

      let existing = (await ds.has(@[k1, k2])).tryGet()
      check existing.len == 2
      # Check keys are equal (by value comparison)
      for e in existing:
        check (e == k1 or e == k2)

    test "has batch - deduplicates input keys":
      # Duplicate keys in input should be deduplicated in result
      let k1 = (key / "hasbatch" / "dedup1").tryGet()
      let k2 = (key / "hasbatch" / "dedup2").tryGet()

      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()
      (await ds.put(RawRecord.init(k2, @[2'u8], 0))).tryGet()

      # Input has duplicates
      let existing = (await ds.has(@[k1, k2, k1, k2, k1])).tryGet()
      check existing.len == 2 # Should deduplicate
      check k1 in existing
      check k2 in existing

    test "has batch - preserves input order":
      # Result should be in input order
      let k1 = (key / "hasbatch" / "order1").tryGet()
      let k2 = (key / "hasbatch" / "order2").tryGet()
      let k3 = (key / "hasbatch" / "order3").tryGet()

      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()
      (await ds.put(RawRecord.init(k2, @[2'u8], 0))).tryGet()
      (await ds.put(RawRecord.init(k3, @[3'u8], 0))).tryGet()

      # Request in specific order: k3, k1, k2
      let existing = (await ds.has(@[k3, k1, k2])).tryGet()
      check existing.len == 3
      check existing[0] == k3
      check existing[1] == k1
      check existing[2] == k2
