import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable/results

import pkg/kvstore

proc basicStoreTests*(
    ds: KVStore, key: Key, bytes: seq[byte], otherBytes: seq[byte]
) =
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
      
      let conflicts = (await ds.putAtomic(@[
        RawRecord.init(k1, @[1'u8], 0),
        RawRecord.init(k2, @[2'u8], 0),
      ])).tryGet()
      
      check conflicts.len == 0
      check (await ds.has(k1)).tryGet()
      check (await ds.has(k2)).tryGet()
    
    test "putAtomic - rollback on any conflict":
      let k1 = (key / "atomic" / "rollback1").tryGet()
      let k2 = (key / "atomic" / "rollback2").tryGet()
      
      # Insert k1 first
      (await ds.put(RawRecord.init(k1, @[1'u8], 0))).tryGet()
      
      # Try atomic batch with k1 (wrong token) and k2 (new)
      let conflicts = (await ds.putAtomic(@[
        RawRecord.init(k1, @[10'u8], 999),  # Wrong token
        RawRecord.init(k2, @[20'u8], 0),     # Would succeed alone
      ])).tryGet()
      
      check conflicts.len == 1
      check conflicts[0] == k1
      # k2 should NOT exist (rollback)
      check (await ds.get(k2)).isErr
    
    test "deleteAtomic - all succeed when tokens match":
      let k1 = (key / "atomic" / "del1").tryGet()
      let k2 = (key / "atomic" / "del2").tryGet()
      
      # Insert both
      discard (await ds.put(@[
        RawRecord.init(k1, @[1'u8], 0),
        RawRecord.init(k2, @[2'u8], 0),
      ])).tryGet()
      
      let r1 = (await ds.get(k1)).tryGet()
      let r2 = (await ds.get(k2)).tryGet()
      
      let conflicts = (await ds.deleteAtomic(@[
        KeyRecord.init(k1, r1.token),
        KeyRecord.init(k2, r2.token),
      ])).tryGet()
      
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
