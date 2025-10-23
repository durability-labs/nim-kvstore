import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/stew/endians2
import pkg/questionable/results

import pkg/datastore

# Encoder/decoder for int type (needed for typed helper tests)
proc encode(i: int): seq[byte] =
  @(cast[uint64](i).toBytesBE)

proc decode(T: type int, bytes: seq[byte]): ?!T =
  if bytes.len >= sizeof(uint64):
    success(cast[int](uint64.fromBytesBE(bytes)))
  else:
    failure("not enough bytes to decode int")

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

proc helperTests*(ds: Datastore, key: Key) =
  # Tests for helper functions: tryPut, tryDelete, getOrPut

  test "tryPut - successful insertion without conflicts":
    let record = Record[int].init(key, 42)
    let failed = (await ds.tryPut(@[record])).tryGet()
    check failed.len == 0  # No failures

    let retrieved = (await ds.get[:int](key)).tryGet()
    check retrieved.val == 42
    check retrieved.token == 1

  test "tryPut - handles conflicts with retry":
    let conflictKey = (key / "retry-conflict").tryGet()
    # First insert
    let initial = Record[int].init(conflictKey, 100)
    (await ds.tryPut(initial)).tryGet()

    # Get current version
    let current = (await ds.get[:int](conflictKey)).tryGet()

    # Try to insert with stale token - should fail after retries
    let stale = Record[int].init(key = conflictKey, val = 200, token = current.token - 1)
    check (await ds.tryPut(@[stale], maxRetries = 2)).isErr

    # Original value should be unchanged
    let unchanged = (await ds.get[:int](conflictKey)).tryGet()
    check unchanged.val == 100

  test "tryPut - single record wrapper":
    let key2 = (key / "single").tryGet()
    let record = Record[int].init(key2, 999)
    (await ds.tryPut(record)).tryGet()

    let retrieved = (await ds.get[:int](key2)).tryGet()
    check retrieved.val == 999

  test "tryPut - bulk operation with partial conflicts":
    let key1 = (key / "bulk1").tryGet()
    let key2 = (key / "bulk2").tryGet()
    let key3 = (key / "bulk3").tryGet()

    # Insert initial records
    let records = @[
      Record[int].init(key1, 1),
      Record[int].init(key2, 2),
      Record[int].init(key3, 3)
    ]
    discard (await ds.tryPut(records)).tryGet()

    # Update with one stale token
    let current2 = (await ds.get[:int](key2)).tryGet()
    let updates = @[
      Record[int].init(key1, 10, 1),  # Valid
      Record[int].init(key2, 20, current2.token - 1),  # Stale - will conflict
      Record[int].init(key3, 30, 1)   # Valid
    ]

    let res = await ds.tryPut(updates, maxRetries = 1)
    check res.isErr or res.get.len > 0  # Should have failures

  test "tryPut - middleware resolves conflicts":
    let middlewareKey = (key / "middleware").tryGet()

    # Insert initial value
    (await ds.tryPut(Record[int].init(middlewareKey, 5))).tryGet()

    # Middleware that refetches tokens
    var middlewareCalled = false
    let middleware = proc(failed: seq[Record[int]]): Future[?!seq[Record[int]]] {.async: (raises: [CancelledError]).} =
      middlewareCalled = true
      let fresh = ?(await ds.get[:int](failed.mapIt( it.key )))
      success zip(failed, fresh).mapIt(
        Record[int].init(it[0].key, it[0].val, it[1].token)
      )

    # Try to update with stale token
    let stale = Record[int].init(middlewareKey, 15, 0)  # Wrong token
    let failed = (await ds.tryPut[:int](@[stale], maxRetries = 3, middleware = middleware)).tryGet()

    check middlewareCalled  # Middleware should have been invoked
    check failed.len == 0   # Should eventually succeed

    let final = (await ds.get[:int](middlewareKey)).tryGet()
    check final.val == 15  # Value should be updated

  test "tryDelete - successful deletion":
    let delKey = (key / "delete1").tryGet()

    # Insert and then delete
    (await ds.tryPut(Record[int].init(delKey, 77))).tryGet()
    let current = (await ds.get[:int](delKey)).tryGet()

    let failed = (await ds.tryDelete(@[current])).tryGet()
    check failed.len == 0
    check not (await ds.has(delKey)).tryGet()

  test "tryDelete - handles conflicts with retry":
    let delKey = (key / "delete2").tryGet()

    # Insert record
    (await ds.tryPut(Record[int].init(delKey, 88))).tryGet()
    let current = (await ds.get[:int](delKey)).tryGet()

    # Try to delete with stale token
    let stale = Record[int].init(delKey, current.val, current.token - 1)
    let res = await ds.tryDelete(@[stale], maxRetries = 2)

    check res.isErr  # Should fail with max retries
    check (await ds.has(delKey)).tryGet()  # Record should still exist

  test "tryDelete - single record wrapper":
    let delKey = (key / "delete3").tryGet()

    (await ds.tryPut(Record[int].init(delKey, 99))).tryGet()
    let current = (await ds.get[:int](delKey)).tryGet()

    (await ds.tryDelete(current)).tryGet()
    check not (await ds.has(delKey)).tryGet()

  test "tryDelete - middleware resolves conflicts":
    let delKey = (key / "delete4").tryGet()

    # Insert record
    (await ds.tryPut(Record[int].init(delKey, 111))).tryGet()

    # Middleware that refetches tokens
    var middlewareCalled = false
    let middleware = proc(failed: seq[Record[int]]): Future[?!seq[Record[int]]] {.async: (raw: true, raises: [CancelledError]).} =
      middlewareCalled = true
      ds.get[:int](failed.mapIt( it.key ))

    # Try to delete with stale token
    let stale = Record[int].init(delKey, 111, 0)
    let failed = (await ds.tryDelete(@[stale], maxRetries = 3, middleware = middleware)).tryGet()

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

    let res = (await ds.getOrPut[:int](gopKey, producer)).tryGet()

    check not producerCalled  # Producer should not be called
    check res.val == 123   # Should get existing value

  test "getOrPut - creates new record when missing":
    let gopKey = (key / "getorput2").tryGet()

    # Producer that should be called
    var producerCalled = false
    let producer = proc(): Future[?!int] {.async: (raises: [CancelledError]).} =
      producerCalled = true
      success 456

    let res = (await ds.getOrPut[:int](gopKey, producer)).tryGet()

    check producerCalled      # Producer should be called
    check res.val == 456   # Should get produced value

    # Verify it was actually stored
    let retrieved = (await ds.get[:int](gopKey)).tryGet()
    check retrieved.val == 456

  test "getOrPut - handles concurrent insert race":
    let gopKey = (key / "getorput3").tryGet()

    # This tests that getOrPut properly handles the case where
    # the key doesn't exist initially but gets created before our insert
    var producerCallCount = 0
    let producer = proc(): Future[?!int] {.async: (raises: [CancelledError]).} =
      inc producerCallCount
      success 789

    let res = (await ds.getOrPut[:int](gopKey, producer, maxRetries = 3)).tryGet()

    check producerCallCount > 0
    check res.val == 789
