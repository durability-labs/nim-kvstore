import std/options
import std/os
import std/sequtils
from std/algorithm import sort, reversed
import std/tables

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable

import pkg/kvstore

import ../kvcommontests
import ../querycommontests

suite "Test Basic SQLiteKVStore":

  let
    ds = SQLiteKVStore.new(Memory).tryGet()
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  teardownAll:
    (await ds.close()).tryGet()

  basicStoreTests(ds, key, bytes, otherBytes)
  helperTests(ds, key)
  atomicBatchTests(ds, key, supportsAtomic = true)

suite "Test Read Only SQLiteKVStore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    filename = "test_store" & DbExt
    dbPathAbs = basePathAbs / filename
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes

  var
    dsDb: SQLiteKVStore
    readOnlyDb: SQLiteKVStore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    dsDb = SQLiteKVStore.new(path = dbPathAbs).tryGet()
    readOnlyDb = SQLiteKVStore.new(path = dbPathAbs, readOnly = true).tryGet()

  teardownAll:
    (await dsDb.close()).tryGet()
    (await readOnlyDb.close()).tryGet()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "put":
    check:
      (await readOnlyDb.put(key, bytes)).isErr

    (await dsDb.put(key, bytes)).tryGet()

  var record: RawRecord
  test "get":
    record = (await readOnlyDb.get(key)).tryGet()

    check:
      record.val == bytes
      (await dsDb.get(key)).tryGet().val == bytes

  test "delete":
    check:
      (await readOnlyDb.delete(record)).isErr

    (await dsDb.delete(record)).tryGet()

  test "contains":
    check:
      not (await readOnlyDb.has(key)).tryGet()
      not (await dsDb.has(key)).tryGet()

suite "Test Query":
  var ds: SQLiteKVStore

  setup:
    ds = SQLiteKVStore.new(Memory).tryGet()

  teardown:
    (await ds.close()).tryGet

  queryTests(ds, testLimitsAndOffsets = true, testSortOrder = true)

  test "Query should return records with tokens":
    let key = Key.init("/test/query/token").tryGet()
    (await ds.put(key, @[1'u8, 2, 3])).tryGet()

    let q = Query.init(Key.init("/test/query").tryGet())
    let iter = (await ds.query(q)).tryGet()

    defer:
      iter.dispose()

    for item in iter:
      let maybeRecord = (await item).tryGet()
      if record =? maybeRecord:
        check record.token > 0  # Token should be non-zero after put

  test "Query with value=true should return records with tokens":
    let key = Key.init("/test/query/token/value").tryGet()
    let data = @[4'u8, 5, 6]
    (await ds.put(key, data)).tryGet()

    let q = Query.init(Key.init("/test/query/token").tryGet(), value = true)
    let iter = (await ds.query(q)).tryGet()

    defer:
      iter.dispose()

    for item in iter:
      let maybeRecord = (await item).tryGet()
      if record =? maybeRecord:
        check:
          record.token > 0  # Token should be non-zero after put
          record.val == data  # Value should be returned

suite "Test Atomic Batch Operations":
  var ds: SQLiteKVStore

  let
    key1 = Key.init("/atomic/key1").tryGet()
    key2 = Key.init("/atomic/key2").tryGet()
    key3 = Key.init("/atomic/key3").tryGet()

  setup:
    ds = SQLiteKVStore.new(SqliteMemory).tryGet()

  teardown:
    (await ds.close()).tryGet

  test "supportsAtomicBatch returns true for SQLiteKVStore":
    check ds.supportsAtomicBatch() == true

  test "putAtomic succeeds when all tokens match (inserts)":
    # All new keys with token=0 (insert mode)
    let records = @[
      RawRecord.init(key1, "value1".toBytes, 0),
      RawRecord.init(key2, "value2".toBytes, 0),
      RawRecord.init(key3, "value3".toBytes, 0),
    ]

    let conflicts = (await ds.putAtomic(records)).tryGet()
    check conflicts.len == 0

    # Verify all records were inserted
    let r1 = (await ds.get(key1)).tryGet()
    let r2 = (await ds.get(key2)).tryGet()
    let r3 = (await ds.get(key3)).tryGet()
    check:
      r1.val == "value1".toBytes
      r2.val == "value2".toBytes
      r3.val == "value3".toBytes

  test "putAtomic rolls back on any conflict":
    # Insert k1 first
    (await ds.put(RawRecord.init(key1, "v1".toBytes, 0))).tryGet()

    # Atomic batch: k1 (wrong token), k2 (new)
    let records = @[
      RawRecord.init(key1, "v1-new".toBytes, 999),  # Wrong token
      RawRecord.init(key2, "v2".toBytes, 0),         # Would succeed alone
    ]

    let conflicts = (await ds.putAtomic(records)).tryGet()

    # Should report k1 as conflict
    check conflicts.len == 1
    check conflicts[0] == key1

    # k2 was NOT committed (atomic rollback)
    let k2result = await ds.get(key2)
    check k2result.isErr  # Key doesn't exist

  test "putAtomic succeeds when all tokens match (updates)":
    # Insert both keys first
    discard (await ds.put(@[
      RawRecord.init(key1, "v1".toBytes, 0),
      RawRecord.init(key2, "v2".toBytes, 0),
    ])).tryGet()

    # Get current tokens
    let r1 = (await ds.get(key1)).tryGet()
    let r2 = (await ds.get(key2)).tryGet()

    # Update both atomically
    let conflicts = (await ds.putAtomic(@[
      RawRecord.init(key1, "v1-new".toBytes, r1.token),
      RawRecord.init(key2, "v2-new".toBytes, r2.token),
    ])).tryGet()

    check conflicts.len == 0

    # Verify both updated
    let u1 = (await ds.get(key1)).tryGet()
    let u2 = (await ds.get(key2)).tryGet()
    check:
      u1.val == "v1-new".toBytes
      u2.val == "v2-new".toBytes

  test "putAtomic fails insert when key exists":
    # Insert k1 first
    (await ds.put(RawRecord.init(key1, "v1".toBytes, 0))).tryGet()

    # Try to insert again with token=0 (insert-only mode)
    let conflicts = (await ds.putAtomic(@[
      RawRecord.init(key1, "v1-new".toBytes, 0),
    ])).tryGet()

    check conflicts.len == 1
    check conflicts[0] == key1

    # Original value unchanged
    let r1 = (await ds.get(key1)).tryGet()
    check r1.val == "v1".toBytes

  test "putAtomic fails update when key missing":
    # Try to update non-existent key with token != 0
    let conflicts = (await ds.putAtomic(@[
      RawRecord.init(key1, "v1".toBytes, 5),  # token != 0 means update-only
    ])).tryGet()

    check conflicts.len == 1
    check conflicts[0] == key1

    # Key still doesn't exist
    let result = await ds.get(key1)
    check result.isErr

  test "deleteAtomic succeeds when all tokens match":
    # Insert records first
    discard (await ds.put(@[
      RawRecord.init(key1, "v1".toBytes, 0),
      RawRecord.init(key2, "v2".toBytes, 0),
    ])).tryGet()

    # Get current tokens
    let r1 = (await ds.get(key1)).tryGet()
    let r2 = (await ds.get(key2)).tryGet()

    # Delete both atomically
    let conflicts = (await ds.deleteAtomic(@[
      KeyRecord.init(key1, r1.token),
      KeyRecord.init(key2, r2.token),
    ])).tryGet()

    check conflicts.len == 0

    # Verify both deleted
    check (await ds.get(key1)).isErr
    check (await ds.get(key2)).isErr

  test "deleteAtomic rolls back on any conflict":
    # Insert only k1
    (await ds.put(RawRecord.init(key1, "v1".toBytes, 0))).tryGet()
    let r1 = (await ds.get(key1)).tryGet()

    # Try to delete k1 (correct) and k2 (doesn't exist)
    let conflicts = (await ds.deleteAtomic(@[
      KeyRecord.init(key1, r1.token),
      KeyRecord.init(key2, 1),  # k2 doesn't exist
    ])).tryGet()

    check conflicts.len == 1
    check conflicts[0] == key2

    # k1 was NOT deleted (atomic rollback)
    let stillExists = (await ds.get(key1)).tryGet()
    check stillExists.val == "v1".toBytes

  test "empty batch returns success":
    let putConflicts = (await ds.putAtomic(newSeq[RawRecord]())).tryGet()
    check putConflicts.len == 0

    let delConflicts = (await ds.deleteAtomic(newSeq[KeyRecord]())).tryGet()
    check delConflicts.len == 0
