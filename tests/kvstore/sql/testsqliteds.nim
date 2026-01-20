import std/options
import std/os
import std/sequtils
import std/sets
from std/algorithm import sort, reversed
import std/tables

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable
import pkg/taskpools

import pkg/kvstore

import ../kvcommontests
import ../typedcommontests
import ../querycommontests
import ../closecommontests
import ../threadingcommontests

suite "Test Basic SQLiteKVStore":
  var
    tp: Taskpool
    ds: SQLiteKVStore

  let
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  setupAll:
    tp = Taskpool.new(num_threads = 4)
    ds = SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  teardownAll:
    (await ds.close()).tryGet()
    tp.shutdown()

  basicStoreTests(ds, key, bytes, otherBytes)
  typedHelperTests(ds, key)
  atomicBatchTests(ds, key, supportsAtomic = true)
  atomicRetryHelperTests(ds, key)

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
    tp: Taskpool
    dsDb: SQLiteKVStore
    readOnlyDb: SQLiteKVStore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    tp = Taskpool.new(num_threads = 4)
    dsDb = SQLiteKVStore.new(path = dbPathAbs, tp = tp).tryGet()
    readOnlyDb = SQLiteKVStore.new(path = dbPathAbs, tp = tp, readOnly = true).tryGet()

  teardownAll:
    (await dsDb.close()).tryGet()
    (await readOnlyDb.close()).tryGet()
    tp.shutdown()

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
  var
    tp: Taskpool
    ds: SQLiteKVStore

  setup:
    tp = Taskpool.new(num_threads = 4)
    ds = SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  teardown:
    (await ds.close()).tryGet()
    tp.shutdown()

  queryTests(ds, testLimitsAndOffsets = true, testSortOrder = true)

  test "Query should return records with tokens":
    let key = Key.init("/test/query/token").tryGet()
    (await ds.put(key, @[1'u8, 2, 3])).tryGet()

    let q = Query.init(Key.init("/test/query").tryGet())
    let iter = (await ds.query(q)).tryGet()

    defer:
      (await iter.dispose()).tryGet()

    for item in iter:
      let maybeRecord = (await item).tryGet()
      if record =? maybeRecord:
        check record.token > 0 # Token should be non-zero after put

  test "Query with value=true should return records with tokens":
    let key = Key.init("/test/query/token/value").tryGet()
    let data = @[4'u8, 5, 6]
    (await ds.put(key, data)).tryGet()

    let q = Query.init(Key.init("/test/query/token").tryGet(), value = true)
    let iter = (await ds.query(q)).tryGet()

    defer:
      (await iter.dispose()).tryGet()

    for item in iter:
      let maybeRecord = (await item).tryGet()
      if record =? maybeRecord:
        check:
          record.token > 0 # Token should be non-zero after put
          record.val == data # Value should be returned

suite "Test Atomic Batch Operations":
  var
    tp: Taskpool
    ds: SQLiteKVStore

  let
    key1 = Key.init("/atomic/key1").tryGet()
    key2 = Key.init("/atomic/key2").tryGet()
    key3 = Key.init("/atomic/key3").tryGet()

  setup:
    tp = Taskpool.new(num_threads = 4)
    ds = SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  teardown:
    (await ds.close()).tryGet()
    tp.shutdown()

  test "supportsAtomicBatch returns true for SQLiteKVStore":
    check ds.supportsAtomicBatch() == true

  test "putAtomic succeeds when all tokens match (inserts)":
    # All new keys with token=0 (insert mode)
    let records =
      @[
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
    let records =
      @[
        RawRecord.init(key1, "v1-new".toBytes, 999), # Wrong token
        RawRecord.init(key2, "v2".toBytes, 0), # Would succeed alone
      ]

    let conflicts = (await ds.putAtomic(records)).tryGet()

    # Should report k1 as conflict
    check conflicts.len == 1
    check conflicts[0] == key1

    # k2 was NOT committed (atomic rollback)
    let k2result = await ds.get(key2)
    check k2result.isErr # Key doesn't exist

  test "putAtomic succeeds when all tokens match (updates)":
    # Insert both keys first
    discard (
      await ds.put(
        @[RawRecord.init(key1, "v1".toBytes, 0), RawRecord.init(key2, "v2".toBytes, 0)]
      )
    ).tryGet()

    # Get current tokens
    let r1 = (await ds.get(key1)).tryGet()
    let r2 = (await ds.get(key2)).tryGet()

    # Update both atomically
    let conflicts = (
      await ds.putAtomic(
        @[
          RawRecord.init(key1, "v1-new".toBytes, r1.token),
          RawRecord.init(key2, "v2-new".toBytes, r2.token),
        ]
      )
    ).tryGet()

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
    let conflicts =
      (await ds.putAtomic(@[RawRecord.init(key1, "v1-new".toBytes, 0)])).tryGet()

    check conflicts.len == 1
    check conflicts[0] == key1

    # Original value unchanged
    let r1 = (await ds.get(key1)).tryGet()
    check r1.val == "v1".toBytes

  test "putAtomic fails update when key missing":
    # Try to update non-existent key with token != 0
    let conflicts = (
      await ds.putAtomic(
        @[
          RawRecord.init(key1, "v1".toBytes, 5) # token != 0 means update-only
        ]
      )
    ).tryGet()

    check conflicts.len == 1
    check conflicts[0] == key1

    # Key still doesn't exist
    let result = await ds.get(key1)
    check result.isErr

  test "deleteAtomic succeeds when all tokens match":
    # Insert records first
    discard (
      await ds.put(
        @[RawRecord.init(key1, "v1".toBytes, 0), RawRecord.init(key2, "v2".toBytes, 0)]
      )
    ).tryGet()

    # Get current tokens
    let r1 = (await ds.get(key1)).tryGet()
    let r2 = (await ds.get(key2)).tryGet()

    # Delete both atomically
    let conflicts = (
      await ds.deleteAtomic(
        @[KeyRecord.init(key1, r1.token), KeyRecord.init(key2, r2.token)]
      )
    ).tryGet()

    check conflicts.len == 0

    # Verify both deleted
    check (await ds.get(key1)).isErr
    check (await ds.get(key2)).isErr

  test "deleteAtomic rolls back on any conflict":
    # Insert only k1
    (await ds.put(RawRecord.init(key1, "v1".toBytes, 0))).tryGet()
    let r1 = (await ds.get(key1)).tryGet()

    # Try to delete k1 (correct) and k2 (doesn't exist)
    let conflicts = (
      await ds.deleteAtomic(
        @[
          KeyRecord.init(key1, r1.token), KeyRecord.init(key2, 1) # k2 doesn't exist
        ]
      )
    ).tryGet()

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

suite "Test Close and Dispose":
  var
    tp: Taskpool
    ds: SQLiteKVStore

  let key = Key.init("/close/test").tryGet()

  setupAll:
    tp = Taskpool.new(num_threads = 4)
    ds = SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  teardownAll:
    # Best effort close - may already be closed by test
    discard await ds.close()
    tp.shutdown()

  iteratorDisposeTests(ds, key)

suite "Test Iterator Tracking":
  var tp: Taskpool

  let key = Key.init("/tracking/test").tryGet()

  setupAll:
    tp = Taskpool.new(num_threads = 4)

  teardownAll:
    tp.shutdown()

  proc sqliteFactory(): Future[KVStore] {.
      async: (raises: [CancelledError, CatchableError])
  .} =
    SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  iteratorTrackingTests(sqliteFactory, key)
  closeAndDisposeTests(sqliteFactory, key)
  concurrentCloseTests(sqliteFactory, key)

suite "Test Error Aggregation Pattern":
  ## Unit tests for the catch(fut.read) pattern used in close()
  catchPatternTests()

suite "Test Threading":
  var tp: Taskpool

  let key = Key.init("/threading/test").tryGet()

  setupAll:
    tp = Taskpool.new(num_threads = 4)

  teardownAll:
    tp.shutdown()

  proc sqliteFactory(): Future[KVStore] {.
      async: (raises: [CancelledError, CatchableError])
  .} =
    SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  threadingTests(sqliteFactory, key)

suite "Test Large Batch Operations":
  ## Tests for automatic chunking of large batches (>999 SQLite parameters)
  var
    tp: Taskpool
    ds: SQLiteKVStore

  setupAll:
    tp = Taskpool.new(num_threads = 4)
    ds = SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  teardownAll:
    (await ds.close()).tryGet()
    tp.shutdown()

  test "getMany with >1000 keys chunks correctly":
    # Create 1100 records (exceeds 999 param limit)
    let count = 1100
    var keys: seq[Key]
    for i in 0 ..< count:
      let k = Key.init("/largebatch/get/" & $i).tryGet()
      keys.add(k)
      (await ds.put(k, ($i).toBytes)).tryGet()

    # Get all keys in one call - should be chunked internally
    let records = (await ds.get(keys)).tryGet()
    check records.len == count

    # Verify all records were retrieved
    var seen = initHashSet[string]()
    for r in records:
      seen.incl(r.key.id)
    check seen.len == count

  test "delete with >500 records chunks correctly":
    # Create 600 records (exceeds 495 param limit for delete - 2 params per record)
    let count = 600
    var records: seq[KeyRecord]
    for i in 0 ..< count:
      let k = Key.init("/largebatch/del/" & $i).tryGet()
      (await ds.put(k, ($i).toBytes)).tryGet()
      let stored = (await ds.get(k)).tryGet()
      records.add(KeyRecord.init(k, stored.token))

    # Delete all in one call - should be chunked internally
    let skipped = (await ds.delete(records)).tryGet()
    check skipped.len == 0

    # Verify all were deleted
    for i in 0 ..< count:
      let k = Key.init("/largebatch/del/" & $i).tryGet()
      check not (await ds.has(k)).tryGet()
