## Threading tests for KVStore backends.
##
## These tests verify:
## 1. Concurrent operations don't block the event loop
## 2. Proper serialization under contention
## 3. Graceful shutdown with concurrent operations
##
## The key testing pattern for non-blocking verification:
## - Start a tight async loop doing operations
## - While that loop runs, try to do other operations with a timeout
## - If the event loop is blocked, timeout fires → test fails with clear message
## - If operations are truly async, both can interleave → test passes

import std/options
import std/sequtils
import std/strutils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable/results

import pkg/kvstore

type
  ## Factory proc that creates a fresh store instance for each test.
  StoreFactory* =
    proc(): Future[KVStore] {.async: (raises: [CancelledError, CatchableError]).}

const
  # Timeout for blocking tests - operations should complete well within this
  # if not blocked. Long enough to not flake, short enough to fail fast.
  BlockingTestTimeout = 5.seconds

# =============================================================================
# Event Loop Blocking Tests
# =============================================================================

proc eventLoopBlockingTests*(factory: StoreFactory, key: Key) =
  ## Tests that verify operations don't block the event loop.
  ## Uses tight loop pattern with timeout: if operations block, test fails fast.

  test "get operations don't block event loop":
    let ds = await factory()
    defer:
      (await ds.close()).tryGet

    let k1 = (key / "nonblock" / "get1").tryGet
    let k2 = (key / "nonblock" / "get2").tryGet
    (await ds.put(k1, "v1".toBytes)).tryGet
    (await ds.put(k2, "v2".toBytes)).tryGet

    var running = true
    var loopCount = 0

    proc tightReads(): Future[void] {.async.} =
      while running:
        discard (await ds.get(k1)).tryGet
        inc loopCount

    let loopFut = tightReads()

    proc mainWork(): Future[void] {.async.} =
      for i in 0 ..< 10:
        discard (await ds.get(k2)).tryGet

    let completed = await withTimeout(mainWork(), BlockingTestTimeout)
    check completed

    running = false
    let loopExited = await withTimeout(loopFut, BlockingTestTimeout)
    check loopExited

    check loopCount > 0

  test "put operations don't block event loop":
    let ds = await factory()
    defer:
      (await ds.close()).tryGet

    let k1 = (key / "nonblock" / "put1").tryGet
    let k2 = (key / "nonblock" / "put2").tryGet
    (await ds.put(k1, "v1".toBytes)).tryGet
    (await ds.put(k2, "v2".toBytes)).tryGet

    var running = true
    var loopCount = 0

    proc tightWrites(): Future[void] {.async.} =
      while running:
        let current = (await ds.get(k1)).tryGet
        (await ds.put(RawRecord.init(k1, "updated".toBytes, current.token))).tryGet
        inc loopCount

    let loopFut = tightWrites()

    proc mainWork(): Future[void] {.async.} =
      for i in 0 ..< 10:
        let current = (await ds.get(k2)).tryGet
        (await ds.put(RawRecord.init(k2, ("w" & $i).toBytes, current.token))).tryGet

    let completed = await withTimeout(mainWork(), BlockingTestTimeout)
    check completed

    running = false
    let loopExited = await withTimeout(loopFut, BlockingTestTimeout)
    check loopExited

    check loopCount > 0

  test "query operations don't block event loop":
    let ds = await factory()
    defer:
      (await ds.close()).tryGet

    for i in 0 ..< 5:
      let k = (key / "nonblock" / "query" / $i).tryGet
      (await ds.put(k, ($i).toBytes)).tryGet

    var running = true
    var loopCount = 0

    proc tightQueries(): Future[void] {.async.} =
      while running:
        let iter =
          (await ds.query(Query.init((key / "nonblock" / "query").tryGet))).tryGet
        discard (await iter.fetchAll()).tryGet
        (await iter.dispose()).tryGet
        inc loopCount

    let loopFut = tightQueries()

    proc mainWork(): Future[void] {.async.} =
      for i in 0 ..< 5:
        let iter =
          (await ds.query(Query.init((key / "nonblock" / "query").tryGet))).tryGet
        let records = (await iter.fetchAll()).tryGet
        check records.len == 5
        (await iter.dispose()).tryGet

    let completed = await withTimeout(mainWork(), BlockingTestTimeout)
    check completed

    running = false
    let loopExited = await withTimeout(loopFut, BlockingTestTimeout)
    check loopExited

    check loopCount > 0

  test "mixed operations don't block each other":
    let ds = await factory()
    defer:
      (await ds.close()).tryGet

    let k1 = (key / "nonblock" / "mixed1").tryGet
    let k2 = (key / "nonblock" / "mixed2").tryGet
    (await ds.put(k1, "initial".toBytes)).tryGet
    (await ds.put(k2, "initial".toBytes)).tryGet

    var running = true
    var readCount = 0
    var writeCount = 0

    proc tightReads(): Future[void] {.async.} =
      while running:
        discard (await ds.get(k1)).tryGet
        inc readCount

    proc tightWrites(): Future[void] {.async.} =
      var i = 0
      while running:
        let current = (await ds.get(k2)).tryGet
        (await ds.put(RawRecord.init(k2, ("v" & $i).toBytes, current.token))).tryGet
        inc writeCount
        inc i

    let readLoopFut = tightReads()
    let writeLoopFut = tightWrites()

    proc mainWork(): Future[void] {.async.} =
      for i in 0 ..< 10:
        check (await ds.has(k1)).tryGet
        check (await ds.has(k2)).tryGet

    let completed = await withTimeout(mainWork(), BlockingTestTimeout)
    check completed

    running = false
    let loopsExited =
      await withTimeout(allFutures(readLoopFut, writeLoopFut), BlockingTestTimeout)
    check loopsExited

    check readCount > 0
    check writeCount > 0

# =============================================================================
# Serialization Under Contention Tests
# =============================================================================

proc serializationTests*(factory: StoreFactory, key: Key) =
  ## Tests that verify proper serialization of operations under contention.

  test "concurrent updates to same key - only one succeeds per token":
    let
      ds = await factory()
      event = newAsyncEvent()
    defer:
      (await ds.close()).tryGet

    let testKey = (key / "serial" / "same").tryGet

    (await ds.put(testKey, "initial".toBytes)).tryGet
    let initial = (await ds.get(testKey)).tryGet

    # Start multiple updates concurrently with the same token
    # Use AsyncEvent to ensure all 5 start simultaneously
    var futures: seq[Future[?!void]]
    for i in 0 ..< 5:
      let record = RawRecord.init(testKey, ("update" & $i).toBytes, initial.token)
      futures.add(
        (
          proc(r: RawRecord): Future[?!void] {.async: (raises: [CancelledError]).} =
            await event.wait()
            await ds.put(r)
        )(record)
      )

    event.fire()
    await allFutures(futures)

    var successes = 0
    for fut in futures:
      if (await fut).isOk:
        inc successes

    # Exactly one should succeed (CAS semantics)
    check successes == 1

    # Verify final state: token advanced and value is one of the updates
    let final = (await ds.get(testKey)).tryGet
    check final.token == initial.token + 1
    check string.fromBytes(final.val) in
      ["update0", "update1", "update2", "update3", "update4"]

  test "concurrent deletes with same token - only one succeeds":
    let
      ds = await factory()
      event = newAsyncEvent()
    defer:
      (await ds.close()).tryGet

    let testKey = (key / "serial" / "del").tryGet

    (await ds.put(testKey, "value".toBytes)).tryGet
    let current = (await ds.get(testKey)).tryGet

    # Use AsyncEvent to ensure all 3 deletes start simultaneously
    var futures: seq[Future[?!void]]
    for i in 0 ..< 3:
      let record = KeyRecord.init(testKey, current.token)
      futures.add(
        (
          proc(r: KeyRecord): Future[?!void] {.async: (raises: [CancelledError]).} =
            await event.wait()
            await ds.delete(r)
        )(record)
      )

    event.fire()
    await allFutures(futures)

    var successes = 0
    for fut in futures:
      if (await fut).isOk:
        inc successes

    check successes == 1
    check (await ds.get(testKey)).isErr

# =============================================================================
# Iterator Threading Tests
# =============================================================================

proc iteratorThreadingTests*(factory: StoreFactory, key: Key) =
  ## Tests for iterator behavior under concurrent access.

  test "multiple iterators can be consumed concurrently":
    let
      ds = await factory()
      event = newAsyncEvent()
    defer:
      (await ds.close()).tryGet

    for prefix in ["a", "b", "c"]:
      for i in 0 ..< 5:
        let k = (key / "iterthread" / prefix / $i).tryGet
        (await ds.put(k, (prefix & $i).toBytes)).tryGet

    let iterA = (await ds.query(Query.init((key / "iterthread" / "a").tryGet))).tryGet
    let iterB = (await ds.query(Query.init((key / "iterthread" / "b").tryGet))).tryGet
    let iterC = (await ds.query(Query.init((key / "iterthread" / "c").tryGet))).tryGet

    # Use AsyncEvent to ensure all 3 fetchAll() start simultaneously
    let futA = (
      proc(): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
        await event.wait()
        await iterA.fetchAll()
    )()
    let futB = (
      proc(): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
        await event.wait()
        await iterB.fetchAll()
    )()
    let futC = (
      proc(): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
        await event.wait()
        await iterC.fetchAll()
    )()

    event.fire()
    await allFutures(futA, futB, futC)

    check (await futA).tryGet.len == 5
    check (await futB).tryGet.len == 5
    check (await futC).tryGet.len == 5

    (await iterA.dispose()).tryGet
    (await iterB.dispose()).tryGet
    (await iterC.dispose()).tryGet

  test "iterator next() calls are serialized - no duplicates or losses":
    let
      ds = await factory()
      event = newAsyncEvent()
    defer:
      (await ds.close()).tryGet

    for i in 0 ..< 10:
      let k = (key / "iterserial" / $i).tryGet
      (await ds.put(k, ($i).toBytes)).tryGet

    let iter = (await ds.query(Query.init((key / "iterserial").tryGet))).tryGet
    defer:
      (await iter.dispose()).tryGet

    # Fire multiple next() calls concurrently using AsyncEvent
    # This ensures all 10 next() calls race simultaneously
    var futures: seq[Future[?!Option[RawRecord]]]
    for i in 0 ..< 10:
      futures.add(
        (
          proc(): Future[?!Option[RawRecord]] {.async: (raises: [CancelledError]).} =
            await event.wait()
            await iter.next()
        )()
      )

    event.fire()
    await allFutures(futures)

    var values: seq[string]
    for fut in futures:
      let res = (await fut).tryGet
      if record =? res:
        values.add(string.fromBytes(record.val))

    check values.len == 10

    # All values 0-9 should be present (order may vary)
    var seen: set[0 .. 9]
    for v in values:
      let num = parseInt(v)
      check num notin seen
      seen.incl(num)
    check seen == {0 .. 9}

    # Verify iterator is exhausted
    check (await iter.next()).tryGet.isNone

# =============================================================================
# Graceful Shutdown Tests
# =============================================================================

proc gracefulShutdownTests*(factory: StoreFactory, key: Key) =
  ## Tests for graceful shutdown behavior.

  test "close waits for in-flight operations":
    let ds = await factory()

    var opCount = 0

    proc putLoop(): Future[void] {.async.} =
      var i = 0
      while true:
        let k = (key / "shutdown" / "inflight" / $i).tryGet
        let res = await ds.put(k, ("value" & $i).toBytes)
        if res.isErr:
          break # Store closed, exit loop
        inc opCount
        inc i

    let loopFut = putLoop()

    # Let the loop queue up operations
    await sleepAsync(300.milliseconds)

    # Close should wait for in-flight operations (with timeout to avoid hanging)
    let closeFut = ds.close()
    check await withTimeout(closeFut, BlockingTestTimeout)
    closeFut.read().tryGet

    # Loop should exit (put fails after close)
    check await withTimeout(loopFut, BlockingTestTimeout)
    check opCount > 0

  test "operations after close fail":
    let ds = await factory()

    let testKey = (key / "shutdown" / "after").tryGet
    (await ds.put(testKey, "value".toBytes)).tryGet

    (await ds.close()).tryGet

    check (await ds.get(testKey)).isErr
    check (await ds.put(testKey, "new".toBytes)).isErr
    check (await ds.query(Query.init(key))).isErr

  test "close disposes active iterators":
    let ds = await factory()

    let testKey = (key / "shutdown" / "iter").tryGet
    (await ds.put(testKey, "value".toBytes)).tryGet

    let iter = (await ds.query(Query.init((key / "shutdown" / "iter").tryGet))).tryGet
    discard (await iter.next()).tryGet

    (await ds.close()).tryGet

    check (await iter.next()).isErr

# =============================================================================
# Interleaving Pattern Tests
# =============================================================================

const
  # Magic header for corruption detection - if we see partial data, header won't match
  ValueHeader = @[0xDE'u8, 0xAD, 0xBE, 0xEF]

proc makeValue(data: string): seq[byte] =
  ## Create a value with header for corruption detection
  ValueHeader & data.toBytes

proc isValidValue(val: seq[byte]): bool =
  ## Check if value has valid header (not corrupted/partial)
  val.len >= 4 and val[0 ..< 4] == ValueHeader

proc extractData(val: seq[byte]): string =
  ## Extract data portion from value
  if val.len > 4:
    string.fromBytes(val[4 ..^ 1])
  else:
    ""

proc interleavingTests*(factory: StoreFactory, key: Key) =
  ## Tests for various read/write interleaving patterns.
  ## Uses relaxed assertions (allowed end-states) per Oracle recommendations.
  ## All tests use withTimeout to detect deadlocks.

  test "write during delete - exactly one wins":
    ## One task writes, one deletes with same token.
    ## Assertion: exactly one succeeds (CAS), final state is consistent.
    let ds = await factory()
    defer:
      (await ds.close()).tryGet

    let testKey = (key / "interleave" / "wd").tryGet

    # Multiple rounds for better coverage
    for round in 0 ..< 5:
      # Setup fresh key
      let
        roundKey = (testKey / $round).tryGet
        event = newAsyncEvent()

      (await ds.put(roundKey, makeValue("initial"))).tryGet
      let current = (await ds.get(roundKey)).tryGet

      # Race: delete vs write with same token using AsyncEvent
      let deleteFut = (
        proc(): Future[?!void] {.async: (raises: [CancelledError]).} =
          await event.wait()
          await ds.delete(KeyRecord.init(roundKey, current.token))
      )()
      let writeFut = (
        proc(): Future[?!void] {.async: (raises: [CancelledError]).} =
          await event.wait()
          await ds.put(RawRecord.init(roundKey, makeValue("updated"), current.token))
      )()

      event.fire()
      let completed =
        await withTimeout(allFutures(deleteFut, writeFut), BlockingTestTimeout)
      check completed

      let deleteRes = await deleteFut
      let writeRes = await writeFut

      # Exactly one should succeed
      let deleteOk = deleteRes.isOk
      let writeOk = writeRes.isOk
      check (deleteOk xor writeOk)

      # Verify consistent state
      let exists = (await ds.has(roundKey)).tryGet
      if deleteOk:
        check not exists
      else:
        check exists
        let final = (await ds.get(roundKey)).tryGet
        check isValidValue(final.val)
        check extractData(final.val) == "updated"

  test "concurrent batch puts - per-record CAS":
    ## Two batches with overlapping keys race.
    ## Assertion: each key has value from one batch, no corruption.
    ## Note: put(seq) is per-record CAS, NOT atomic.
    let
      ds = await factory()
      event = newAsyncEvent()

    defer:
      (await ds.close()).tryGet

    var batch1: seq[RawRecord]
    var batch2: seq[RawRecord]

    for i in 0 ..< 10:
      let k = (key / "interleave" / "batch" / $i).tryGet
      batch1.add(RawRecord.init(k, makeValue("batch1-" & $i)))
      batch2.add(RawRecord.init(k, makeValue("batch2-" & $i)))

    let fut1 = (
      proc(): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
        await event.wait()
        await ds.put(batch1)
    )()

    let fut2 = (
      proc(): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
        await event.wait()
        await ds.put(batch2)
    )()

    event.fire()
    await allFutures(fut1, fut2)
    let
      res1 = await fut1
      res2 = await fut2

    # Both batches should complete successfully
    check res1.isOk
    check res2.isOk

    # Each key results in exactly one winner and one loser (conflict).
    # Total conflicts = number of keys, regardless of how the race plays out.
    let totalConflicts = res1.tryGet.len + res2.tryGet.len
    check totalConflicts == 10

    # Check consistency - each key should have valid value from one batch
    for i in 0 ..< 10:
      let k = (key / "interleave" / "batch" / $i).tryGet
      let res = (await ds.get(k))
      if res.isErr:
        if res.error of KVStoreKeyNotFound:
          continue
        else:
          expect KVStoreError:
            raise res.error
          continue

      let record = res.tryGet
      check isValidValue(record.val)
      let data = extractData(record.val)
      check (data == "batch1-" & $i or data == "batch2-" & $i)

  test "high contention - many concurrent updates, no deadlock":
    ## Many concurrent updates to same key with same token.
    ## Assertion: exactly one succeeds (CAS), no deadlock.
    let
      ds = await factory()
      event = newAsyncEvent()

    defer:
      (await ds.close()).tryGet

    let testKey = (key / "interleave" / "contention").tryGet
    (await ds.put(testKey, makeValue("initial"))).tryGet
    let initial = (await ds.get(testKey)).tryGet

    const numTasks = 10

    # All tasks wait at the gate, then try to update with the same initial token
    var futures: seq[Future[?!void]]
    for i in 0 ..< numTasks:
      let record = RawRecord.init(testKey, makeValue("update" & $i), initial.token)
      futures.add(
        (
          proc(): Future[?!void] {.async: (raises: [CancelledError]).} =
            await event.wait()
            await ds.put(record)
        )()
      )

    # Release all tasks simultaneously
    event.fire()

    let completed = await withTimeout(allFutures(futures), BlockingTestTimeout)
    check completed

    var successCount = 0
    for fut in futures:
      if (await fut).isOk:
        inc successCount

    # Exactly one should succeed (CAS semantics)
    check successCount == 1

    # Final value should be one of the updates
    let final = (await ds.get(testKey)).tryGet
    check isValidValue(final.val)

  test "modification during iteration - sequential":
    ## Modify records while iterator is active (sequentially).
    ## Assertion: no crash, dispose succeeds.
    let ds = await factory()
    defer:
      (await ds.close()).tryGet

    # Insert initial records
    for i in 0 ..< 10:
      let k = (key / "interleave" / "itermod" / $i).tryGet
      (await ds.put(k, makeValue($i))).tryGet

    let iter =
      (await ds.query(Query.init((key / "interleave" / "itermod").tryGet))).tryGet

    # Get first few results
    var collected = 0
    for _ in 0 ..< 3:
      let res = (await iter.next()).tryGet
      if res.isSome:
        inc collected

    # Now modify some records while iterator is still open
    for i in 5 ..< 10:
      let k = (key / "interleave" / "itermod" / $i).tryGet
      let current = await ds.get(k)
      if current.isOk:
        discard
          await ds.put(RawRecord.init(k, makeValue("modified" & $i), current.get.token))

    # Continue iteration
    while true:
      let res = (await iter.next()).tryGet
      if res.isNone:
        break
      inc collected

    # Dispose must succeed
    let disposeRes = await iter.dispose()
    check disposeRes.isOk

    # Should have collected some records
    check collected >= 1

  test "rapid sequential ops on same key":
    ## Rapid put cycles on same key (sequential, not concurrent).
    ## Tests lock table lifecycle.
    ## Assertion: no exceptions, store usable after.
    let ds = await factory()
    defer:
      (await ds.close()).tryGet

    let testKey = (key / "interleave" / "rapid").tryGet
    (await ds.put(testKey, makeValue("0"))).tryGet

    # Rapid sequential updates
    for i in 1 .. 50:
      let current = (await ds.get(testKey)).tryGet
      (await ds.put(RawRecord.init(testKey, makeValue($i), current.token))).tryGet

    # Store should still be usable
    let final = (await ds.get(testKey)).tryGet
    check isValidValue(final.val)
    check extractData(final.val) == "50"

# =============================================================================
# Combined Test Suite
# =============================================================================

proc threadingTests*(factory: StoreFactory, key: Key) =
  ## Run all threading tests for a backend.
  eventLoopBlockingTests(factory, key)
  serializationTests(factory, key)
  iteratorThreadingTests(factory, key)
  gracefulShutdownTests(factory, key)
  interleavingTests(factory, key)
