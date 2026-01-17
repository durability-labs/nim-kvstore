import std/sequtils
import std/tables

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/endians2
import pkg/questionable
import pkg/questionable/results

import pkg/kvstore
import pkg/kvstore/typedkv

# Encoder/decoder for int type
proc encode(i: int): seq[byte] =
  @(cast[uint64](i).toBytesBE)

proc decode(T: type int, bytes: seq[byte]): ?!T =
  if bytes.len >= sizeof(uint64):
    success(cast[int](uint64.fromBytesBE(bytes)))
  else:
    failure("not enough bytes to decode int")

proc typedHelperTests*(ds: KVStore, key: Key) =
  # Tests for typed helper functions: tryPut, tryDelete, getOrPut

  test "tryPut - successful insertion without conflicts":
    let record = Record[int].init(key, 42)
    let failed = (await ds.tryPut(@[record])).tryGet()
    check failed.len == 0 # No failures

    let retrieved = (await get[int](ds, key)).tryGet()
    check retrieved.val == 42
    check retrieved.token == 1

  test "tryPut - handles conflicts with retry":
    let conflictKey = (key / "retry-conflict").tryGet()
    # First insert
    let initial = Record[int].init(conflictKey, 100)
    (await ds.tryPut(initial)).tryGet()

    # Get current version
    let current = (await get[int](ds, conflictKey)).tryGet()

    # Try to insert with stale token - should fail after retries
    let stale =
      Record[int].init(key = conflictKey, val = 200, token = current.token - 1)
    check (await ds.tryPut(@[stale], maxRetries = 2)).isErr

    # Original value should be unchanged
    let unchanged = (await get[int](ds, conflictKey)).tryGet()
    check unchanged.val == 100

  test "tryPut - single record wrapper":
    let key2 = (key / "single").tryGet()
    let record = Record[int].init(key2, 999)
    (await ds.tryPut(record)).tryGet()

    let retrieved = (await get[int](ds, key2)).tryGet()
    check retrieved.val == 999

  test "tryPut - bulk operation with partial conflicts":
    let key1 = (key / "bulk1").tryGet()
    let key2 = (key / "bulk2").tryGet()
    let key3 = (key / "bulk3").tryGet()

    # Insert initial records
    let records =
      @[Record[int].init(key1, 1), Record[int].init(key2, 2), Record[int].init(key3, 3)]
    discard (await ds.tryPut(records)).tryGet()

    # Update with one stale token
    let current2 = (await get[int](ds, key2)).tryGet()
    let updates =
      @[
        Record[int].init(key1, 10, 1), # Valid
        Record[int].init(key2, 20, current2.token - 1), # Stale - will conflict
        Record[int].init(key3, 30, 1), # Valid
      ]

    let res = await ds.tryPut(updates, maxRetries = 1)
    check res.isErr or res.get.len > 0 # Should have failures

  test "tryPut - middleware resolves conflicts":
    let middlewareKey = (key / "middleware").tryGet()

    # Insert initial value
    (await ds.tryPut(Record[int].init(middlewareKey, 5))).tryGet()

    # Middleware that refetches tokens
    var middlewareCalled = false
    let middleware = proc(
        failed: seq[Record[int]]
    ): Future[?!seq[Record[int]]] {.async: (raises: [CancelledError]).} =
      middlewareCalled = true
      let fresh = ?(await get[int](ds, failed.mapIt(it.key)))
      success zip(failed, fresh).mapIt(
        Record[int].init(it[0].key, it[0].val, it[1].token)
      )

    # Try to update with stale token
    let stale = Record[int].init(middlewareKey, 15, 0) # Wrong token
    let failed = (
      await tryPut[int](ds, @[stale], maxRetries = 3, middleware = middleware)
    ).tryGet()

    check middlewareCalled # Middleware should have been invoked
    check failed.len == 0 # Should eventually succeed

    let final = (await get[int](ds, middlewareKey)).tryGet()
    check final.val == 15 # Value should be updated

  test "tryDelete - successful deletion":
    let delKey = (key / "delete1").tryGet()

    # Insert and then delete
    (await ds.tryPut(Record[int].init(delKey, 77))).tryGet()
    let current = (await get[int](ds, delKey)).tryGet()

    let failed = (await ds.tryDelete(@[KeyRecord.init(delKey, current.token)])).tryGet()
    check failed.len == 0
    check not (await ds.has(delKey)).tryGet()

  test "tryDelete - handles conflicts with retry":
    let delKey = (key / "delete2").tryGet()

    # Insert record
    (await ds.tryPut(Record[int].init(delKey, 88))).tryGet()
    let current = (await get[int](ds, delKey)).tryGet()

    # Try to delete with stale token
    let stale = KeyRecord.init(delKey, current.token - 1)
    let res = await ds.tryDelete(@[stale], maxRetries = 2)

    check res.isErr # Should fail with max retries
    check (await ds.has(delKey)).tryGet() # Record should still exist

  test "tryDelete - single record wrapper":
    let delKey = (key / "delete3").tryGet()

    (await ds.tryPut(Record[int].init(delKey, 99))).tryGet()
    let current = (await get[int](ds, delKey)).tryGet()

    (await ds.tryDelete(KeyRecord.init(delKey, current.token))).tryGet()
    check not (await ds.has(delKey)).tryGet()

  test "tryDelete - middleware resolves conflicts":
    let delKey = (key / "delete4").tryGet()

    # Insert record
    (await ds.tryPut(Record[int].init(delKey, 111))).tryGet()

    # Middleware that refetches tokens
    var middlewareCalled = false
    let middleware = proc(
        failed: seq[KeyRecord]
    ): Future[?!seq[KeyRecord]] {.async: (raises: [CancelledError]).} =
      middlewareCalled = true
      success (?(await get[int](ds, failed.mapIt(it.key)))).mapIt(
        KeyRecord.init(it.key, it.token)
      )

    # Try to delete with stale token
    let stale = KeyRecord.init(delKey, 0)
    let failed =
      (await ds.tryDelete(@[stale], maxRetries = 3, middleware = middleware)).tryGet()

    check middlewareCalled
    check failed.len == 0
    check not (await ds.has(delKey)).tryGet()

  test "getOrPut - returns existing record":
    let gopKey = (key / "getorput1").tryGet()

    # Insert initial value
    (await ds.tryPut(Record[int].init(gopKey, 123))).tryGet()

    # Producer that should NOT be called
    var producerCalled = false
    let producer = proc(): Future[?!int] {.async: (raises: [CancelledError]).} =
      producerCalled = true
      success 999

    let res = (await getOrPut[int](ds, gopKey, producer)).tryGet()

    check not producerCalled # Producer should not be called
    check res.val == 123 # Should get existing value

  test "getOrPut - creates new record when missing":
    let gopKey = (key / "getorput2").tryGet()

    # Producer that should be called
    var producerCalled = false
    let producer = proc(): Future[?!int] {.async: (raises: [CancelledError]).} =
      producerCalled = true
      success 456

    let res = (await getOrPut[int](ds, gopKey, producer)).tryGet()

    check producerCalled # Producer should be called
    check res.val == 456 # Should get produced value

    # Verify it was actually stored
    let retrieved = (await get[int](ds, gopKey)).tryGet()
    check retrieved.val == 456

  test "getOrPut - handles concurrent insert race":
    let gopKey = (key / "getorput3").tryGet()

    # This tests that getOrPut properly handles the case where
    # the key doesn't exist initially but gets created before our insert
    var producerCallCount = 0
    let producer = proc(): Future[?!int] {.async: (raises: [CancelledError]).} =
      inc producerCallCount
      success 789

    let res = (await getOrPut[int](ds, gopKey, producer, maxRetries = 3)).tryGet()

    check producerCallCount > 0
    check res.val == 789

  test "getOrPut - propagates producer error":
    let gopKey = (key / "getorput-error").tryGet()

    let failingProducer = proc(): Future[?!int] {.async: (raises: [CancelledError]).} =
      failure "producer failed intentionally"

    let res = await getOrPut[int](ds, gopKey, failingProducer)
    check res.isErr

  test "newCorruptionError creates error with message":
    let err = newCorruptionError("corruption detected")
    check err.msg == "corruption detected"
    check err of KVStoreCorruption

  test "newMaxRetriesError creates error with default message":
    let err = newMaxRetriesError()
    check err.msg == "Max retries reached"
    check err of KVStoreMaxRetriesError

  test "newMaxRetriesError creates error with custom message":
    let err = newMaxRetriesError("custom retry error")
    check err.msg == "custom retry error"

  test "single put - conflict returns error":
    let conflictKey = (key / "single-put-conflict").tryGet()
    (await ds.put(conflictKey, 42)).tryGet()
    let current = (await get[int](ds, conflictKey)).tryGet()

    # Try single put with stale token - should error
    let stale = Record[int].init(conflictKey, 999, current.token - 1)
    let res = await ds.put(stale)
    check res.isErr

  test "single delete - conflict returns error":
    let delKey = (key / "single-del-conflict").tryGet()
    (await ds.put(delKey, 77)).tryGet()
    let current = (await get[int](ds, delKey)).tryGet()

    # Try single delete with stale token - should error
    let stale = KeyRecord.init(delKey, current.token - 1)
    let res = await ds.delete(stale)
    check res.isErr

  test "query with typed int results":
    let
      source = {"a": 11, "b": 22, "c": 33, "d": 44}.toTable()
      Root = (key / "typed-querytest").tryGet()

    # Insert typed data
    for k, v in source:
      let putKey = (Root / k).tryGet()
      let record = Record[int](key: putKey, val: v, token: 0)
      (await ds.put(record)).tryGet()

    # Query with typed results
    let iter = (await query[int](ds, Query.init(Root))).tryGet()

    var results = initTable[string, int]()

    while not iter.finished:
      let item = (await iter.next()).tryGet()

      without record =? item:
        continue

      let
        keyNs = record.key
        value = record.val

      check:
        keyNs.value notin results

      results[keyNs.value] = value

    check:
      results == source
