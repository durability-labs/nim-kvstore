import std/options
import std/sequtils
import std/sets

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable
import pkg/questionable/results
import pkg/taskpools

import pkg/kvstore
import pkg/kvstore/[key, types, taskutils]

# Import SQLite backend for atomic batch tests
import pkg/kvstore/sql/sqliteds

suite "KVStore (base)":
  let
    key = Key.init("a").get
    ds = KVStore()

  let record = KVRecord.init(key, @[1.byte])

  test "put":
    expect AssertionDefect:
      (await ds.put(record)).tryGet

  test "delete":
    expect AssertionDefect:
      (await ds.delete(KeyKVRecord.init(key, record.token))).tryGet

  test "contains":
    expect AssertionDefect:
      discard (await ds.has(key)).tryGet

  test "get":
    expect AssertionDefect:
      var rec = (await ds.get(key)).tryGet

  test "query":
    expect AssertionDefect:
      let iter = (await query(ds, Query.init(key))).tryGet
      for n in iter:
        discard

suite "KVStore duplicate key rejection (SQLite)":
  var
    tp: Taskpool
    ds: SQLiteKVStore

  let
    key1 = Key.init("dup/key1").tryGet()
    key2 = Key.init("dup/key2").tryGet()

  setup:
    tp = Taskpool.new(num_threads = 2)
    ds = SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  teardown:
    (await ds.close()).tryGet()
    tp.shutdown()

  test "should reject duplicate keys in atomic put batch":
    let records = @[
      RawKVRecord.init(key1, "value1".toBytes, 0),
      RawKVRecord.init(key1, "value2".toBytes, 0) # duplicate key
    ]
    let res = await ds.putAtomic(records)
    check res.isErr
    check res.error of KVStoreDuplicateKeyError

  test "should reject duplicate keys in atomic delete batch":
    # First insert a record to delete
    (await ds.put(RawKVRecord.init(key1, "value1".toBytes, 0))).tryGet()

    # Try to delete with duplicate key in batch
    let records = @[
      KeyKVRecord.init(key1, 1),
      KeyKVRecord.init(key1, 2) # duplicate key with different token
    ]
    let res = await ds.deleteAtomic(records)
    check res.isErr
    check res.error of KVStoreDuplicateKeyError

suite "KVStore token overflow protection":
  test "boundedToken should handle max int64":
    # Test that boundedToken correctly handles int64 high value
    let maxInt64: uint64 = uint64(high(int64))
    let res = boundedToken(maxInt64)
    check res.isOk
    check res.get == high(int64).int64

  test "boundedToken should reject overflow":
    # Test that boundedToken rejects values above int64 high
    let overflowValue: uint64 = uint64(high(int64)) + 1
    let res = boundedToken(overflowValue)
    check res.isErr
    check res.error of KVStoreCorruption

suite "KVStore middleware key-set validation":
  var
    tp: Taskpool
    ds: SQLiteKVStore

  let
    key1 = Key.init("middleware/key1").tryGet()
    key2 = Key.init("middleware/key2").tryGet()

  setup:
    tp = Taskpool.new(num_threads = 2)
    ds = SQLiteKVStore.new(SqliteMemory, tp).tryGet()

  teardown:
    (await ds.close()).tryGet()
    tp.shutdown()

  test "should reject middleware returning different keys for tryPutAtomic":
    # Insert initial record
    (await ds.put(RawKVRecord.init(key1, "v1".toBytes, 0))).tryGet()
    (await ds.put(RawKVRecord.init(key2, "v2".toBytes, 0))).tryGet()

    # TryPutAtomic with stale tokens to force conflict (middleware will be invoked)
    let middleware = proc(
      failed: seq[RawKVRecord],
      conflicts: seq[Key]
    ): Future[?!seq[RawKVRecord]] {.async: (raises: [CancelledError]).} =
      # Try to return different keys than original
      let newKey = Key.init("middleware/tampered").get()
      success(@[RawKVRecord.init(newKey, "newval".toBytes, 0)])

    let records = @[
      RawKVRecord.init(key1, "value1".toBytes, 0),  # stale token - will conflict
      RawKVRecord.init(key2, "value2".toBytes, 0)  # stale token - will conflict
    ]

    let res = await ds.tryPutAtomic(records, maxRetries = 1, middleware)
    check res.isErr

  test "should reject middleware returning different keys for tryDeleteAtomic":
    # Insert initial records first
    (await ds.put(RawKVRecord.init(key1, "v1".toBytes, 0))).tryGet()
    (await ds.put(RawKVRecord.init(key2, "v2".toBytes, 0))).tryGet()

    # Get the current tokens
    let rec1 = (await ds.get(key1)).tryGet()
    let rec2 = (await ds.get(key2)).tryGet()

    # TryDeleteAtomic with stale tokens to force conflict (middleware will be invoked)
    let middleware = proc(
      failed: seq[KeyKVRecord],
      conflicts: seq[Key]
    ): Future[?!seq[KeyKVRecord]] {.async: (raises: [CancelledError]).} =
      # Try to return different keys than original
      let newKey = Key.init("middleware/tampered").get()
      success(@[KeyKVRecord.init(newKey, 1)])

    let records = @[
      KeyKVRecord.init(key1, 0),  # stale token - will conflict
      KeyKVRecord.init(key2, 0)   # stale token - will conflict
    ]

    let res = await ds.tryDeleteAtomic(records, maxRetries = 1, middleware)
    check res.isErr
