import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils

import pkg/datastore

proc basicStoreTests*(
    ds: Datastore, key: Key, bytes: seq[byte], otherBytes: seq[byte]
) =
  var record: RawRecord
  test "put":
    (await ds.put(key, bytes)).tryGet()

  test "get":
    record = (await get[seq[byte]](ds, key)).tryGet()
    check:
      record.val == bytes
      record.token == 1

  test "put update":
    record.val = otherBytes
    (await ds.put(record)).tryGet()

  test "get updated":
    record = (await get[seq[byte]](ds, key)).tryGet()
    check:
      record.val == otherBytes
      record.token == 2

  test "delete":
    (await ds.delete(record)).tryGet()

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
    let fetched = (await get[seq[byte]](ds, records.mapIt(it.key))).tryGet

    check fetched.len == records.len

    for r in records:
      let f = fetched.filterIt(it.key == r.key)[0]
      check:
        f.val == r.val
        f.token == 1

    records = fetched

  test "delete records":
    let skipped = (await ds.delete(records)).tryGet
    check skipped.len == 0 # all deletions should succeed

    for k in records:
      check:
        not (await ds.has(k.key)).tryGet

  test "put detects stale token conflicts":
    let conflictKey = (key / "conflict").tryGet()
    (await ds.put(RawRecord.init(conflictKey, "initial".toBytes))).tryGet()

    let current = (await get[seq[byte]](ds, conflictKey)).tryGet()

    let fresh = RawRecord.init(conflictKey, "fresh".toBytes, current.token)
    check (await ds.put(@[fresh])).tryGet.len == 0

    let updated = (await get[seq[byte]](ds, conflictKey)).tryGet()
    check:
      updated.val == "fresh".toBytes
      updated.token == current.token + 1

    let stale = RawRecord.init(conflictKey, "stale".toBytes, current.token)
    let skipped = (await ds.put(@[stale])).tryGet()
    check skipped.len == 1
    if skipped.len == 1:
      check skipped[0] == conflictKey

    let afterConflict = (await get[seq[byte]](ds, conflictKey)).tryGet()
    check:
      afterConflict.val == "fresh".toBytes
      afterConflict.token == updated.token

  test "delete ignores conflicting tokens":
    let deleteKey = ((key / "delete").tryGet() / "conflict").tryGet()
    (await ds.put(RawRecord.init(deleteKey, "value".toBytes))).tryGet()
    let current = (await get[seq[byte]](ds, deleteKey)).tryGet()

    let staleDelete = RawRecord.init(deleteKey, current.val, current.token - 1)
    let skippedStale = (await ds.delete(@[staleDelete])).tryGet
    check skippedStale.len == 1 # stale token should be skipped
    check skippedStale[0] == deleteKey

    let skippedCurrent = (await ds.delete(@[current])).tryGet
    check skippedCurrent.len == 0 # current token should succeed
    check not (await ds.has(deleteKey)).tryGet()
