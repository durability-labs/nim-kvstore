{.push raises: [].}

## Backend liveness tests for cancellation handling.
##
## These tests verify POST-CONDITIONS only, not cancellation timing:
## 1. Store remains usable after cancellation attempt
## 2. Data remains consistent (no corruption)
## 3. No resource leaks (no crashes on subsequent operations)
## 4. Close works correctly after cancellations
##
## IMPORTANT: These tests do NOT assert whether cancellation actually happened.
## Operations may complete before cancellation occurs - that's fine.
## The deterministic cancellation tests are in testtaskutils.nim (awaitSignal tests).
##
## Uses cancelAfterSteps() to attempt cancellation after N poll cycles.

import std/options
import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable
import pkg/questionable/results

import pkg/kvstore

type
  ## Factory proc that creates a fresh store instance for each test.
  StoreFactory* =
    proc(): Future[KVStore] {.async: (raises: [CancelledError, CatchableError]).}

# =============================================================================
# Helper Utilities
# =============================================================================

proc cancelAfterSteps*[T](
    fut: Future[T], steps: int
): Future[Option[T]] {.async: (raises: [CancelledError, CatchableError]).} =
  ## Run future for `steps` poll cycles, then cancel if not done.
  ## Returns Some(result) if completed within steps, None if cancelled.
  let stepsFut = stepsAsync(steps)

  try:
    discard await race(fut, stepsFut)

    if fut.finished():
      if not stepsFut.finished():
        await stepsFut.cancelAndWait()
      return some(fut.read())
    else:
      # Steps exhausted - cancel the operation
      await fut.cancelAndWait()
      return none(T)
  except CancelledError as exc:
    if not fut.finished():
      await fut.cancelAndWait()
    if not stepsFut.finished():
      await stepsFut.cancelAndWait()
    raise exc

# =============================================================================
# Basic Operation Cancellation Tests
# =============================================================================

proc basicCancellationTests*(factory: StoreFactory, key: Key) =
  ## Tests basic cancellation of individual operations.

  test "cancel get - no crash, store still usable":
    let ds = await factory()
    defer:
      discard await ds.close()

    let k = (key / "cancel-get").tryGet
    (await ds.put(k, "value".toBytes)).tryGet

    # Start a get and cancel it quickly
    let getFut = ds.get(k)
    discard await cancelAfterSteps(getFut, 1)

    # Whether cancelled or completed, store should still be usable
    let record = (await ds.get(k)).tryGet
    check record.val == "value".toBytes

  test "cancel put - store remains consistent":
    let ds = await factory()
    defer:
      discard await ds.close()

    let k = (key / "cancel-put").tryGet

    # Put initial value
    (await ds.put(k, "initial".toBytes)).tryGet

    # Get token for update
    let existing = (await ds.get(k)).tryGet

    # Start an update and try to cancel it
    let updateRecord = RawKVRecord.init(k, "updated".toBytes, existing.token)
    let putFut = ds.put(updateRecord)
    discard await cancelAfterSteps(putFut, 1)

    # Value should be either initial or updated, never corrupted
    let final = (await ds.get(k)).tryGet
    check (final.val == "initial".toBytes or final.val == "updated".toBytes)

  test "cancel delete - key state is consistent":
    let ds = await factory()
    defer:
      discard await ds.close()

    let k = (key / "cancel-delete").tryGet
    (await ds.put(k, "value".toBytes)).tryGet
    let existing = (await ds.get(k)).tryGet

    # Start delete and try to cancel
    let delFut = ds.delete(KeyKVRecord.init(k, existing.token))
    discard await cancelAfterSteps(delFut, 1)

    # Key should either exist or not, never in corrupted state
    let exists = (await ds.has(k)).tryGet
    if exists:
      let record = (await ds.get(k)).tryGet
      check record.val == "value".toBytes
    # else: successfully deleted, that's fine too

  test "cancel has - no crash":
    let ds = await factory()
    defer:
      discard await ds.close()

    let k = (key / "cancel-has").tryGet
    (await ds.put(k, "value".toBytes)).tryGet

    let hasFut = ds.has(k)
    discard await cancelAfterSteps(hasFut, 1)

    # Store should still work
    check (await ds.has(k)).tryGet == true

# =============================================================================
# Batch Operation Cancellation Tests
# =============================================================================

proc batchCancellationTests*(factory: StoreFactory, key: Key) =
  ## Tests cancellation of batch operations.

  test "cancel batch put - partial results handled":
    let ds = await factory()
    defer:
      discard await ds.close()

    # Create batch of records
    var records: seq[RawKVRecord]
    for i in 0 ..< 20:
      let k = (key / "batch-put" / $i).tryGet
      records.add(RawKVRecord.init(k, ("value" & $i).toBytes))

    # Start batch put and cancel
    let putFut = ds.put(records)
    discard await cancelAfterSteps(putFut, 1)

    # Store should still be usable - verify we can read whatever was written
    var foundCount = 0
    for r in records:
      let exists = (await ds.has(r.key)).tryGet
      if exists:
        inc foundCount
        let got = (await ds.get(r.key)).tryGet
        # Value should match what we tried to write
        check got.val == r.val

    # Some records may have been written before cancellation
    # The exact count depends on timing, just verify store is consistent

  test "cancel batch delete - partial results handled":
    let ds = await factory()
    defer:
      discard await ds.close()

    # First, insert records
    var records: seq[RawKVRecord]
    for i in 0 ..< 20:
      let k = (key / "batch-del" / $i).tryGet
      records.add(RawKVRecord.init(k, ("value" & $i).toBytes))

    discard (await ds.put(records)).tryGet

    # Get tokens for delete
    let fetched = (await ds.get(records.mapIt(it.key))).tryGet
    let deleteRecords = fetched.mapIt(KeyKVRecord.init(it.key, it.token))

    # Start batch delete and cancel
    let delFut = ds.delete(deleteRecords)
    discard await cancelAfterSteps(delFut, 1)

    # Verify store is consistent - each key either exists or doesn't
    for r in records:
      let exists = (await ds.has(r.key)).tryGet
      if exists:
        let got = (await ds.get(r.key)).tryGet
        check got.val == r.val

# =============================================================================
# Iterator Cancellation Tests
# =============================================================================

proc iteratorCancellationTests*(factory: StoreFactory, key: Key) =
  ## Tests cancellation during query iteration.

  test "cancel iterator next - iterator can be disposed":
    let ds = await factory()
    defer:
      discard await ds.close()

    # Insert some data to iterate
    for i in 0 ..< 10:
      let k = (key / "iter-cancel" / $i).tryGet
      (await ds.put(k, ("v" & $i).toBytes)).tryGet

    let query = Query.init((key / "iter-cancel").tryGet)
    let iter = (await ds.query(query)).tryGet

    # Get first result normally
    let first = (await iter.next()).tryGet
    check first.isSome

    # Start next() and cancel it
    let nextFut = iter.next()
    discard await cancelAfterSteps(nextFut, 1)

    # Iterator should still be disposable
    discard await iter.dispose()

  test "cancel multiple next calls - cleanup works":
    let ds = await factory()
    defer:
      discard await ds.close()

    for i in 0 ..< 10:
      let k = (key / "iter-multi" / $i).tryGet
      (await ds.put(k, ("v" & $i).toBytes)).tryGet

    let query = Query.init((key / "iter-multi").tryGet)
    let iter = (await ds.query(query)).tryGet

    # Cancel several next() calls
    for _ in 0 ..< 3:
      let nextFut = iter.next()
      discard await cancelAfterSteps(nextFut, 1)

    discard await iter.dispose()

    # Store should still work
    check (await ds.has((key / "iter-multi" / "0").tryGet)).tryGet

# =============================================================================
# Rapid Cancel/Retry Tests
# =============================================================================

proc rapidCancelRetryTests*(factory: StoreFactory, key: Key) =
  ## Tests rapid cancellation and retry patterns.

  test "cancel and immediately retry same key - no deadlock":
    let ds = await factory()
    defer:
      discard await ds.close()

    let k = (key / "rapid-retry").tryGet
    (await ds.put(k, "initial".toBytes)).tryGet

    # Cancel a get, then immediately do another
    for i in 0 ..< 5:
      let getFut = ds.get(k)
      discard await cancelAfterSteps(getFut, 1)

      # Immediately retry - should not deadlock
      let record = (await ds.get(k)).tryGet
      check record.val == "initial".toBytes

  test "multiple rapid cancellations on same key":
    let ds = await factory()
    defer:
      discard await ds.close()

    let k = (key / "rapid-cancel").tryGet
    (await ds.put(k, "value".toBytes)).tryGet

    # Fire off many operations and cancel them all rapidly
    var futures: seq[Future[?!RawKVRecord]]
    for _ in 0 ..< 10:
      futures.add(ds.get(k))

    # Cancel them all
    for fut in futures:
      if not fut.finished():
        await fut.cancelAndWait()

    # Store should still work
    let record = (await ds.get(k)).tryGet
    check record.val == "value".toBytes

  test "interleaved put/cancel cycles":
    let ds = await factory()
    defer:
      discard await ds.close()

    let k = (key / "interleaved").tryGet
    (await ds.put(k, "v0".toBytes)).tryGet

    for i in 1 .. 5:
      let existing = (await ds.get(k)).tryGet
      let updateRecord = RawKVRecord.init(k, ("v" & $i).toBytes, existing.token)

      let putFut = ds.put(updateRecord)
      discard await cancelAfterSteps(putFut, 1)

    # Final value should be some valid state
    let final = (await ds.get(k)).tryGet
    check final.val.len > 0

# =============================================================================
# Close During Cancellation Tests
# =============================================================================

proc closeDuringCancellationTests*(factory: StoreFactory, key: Key) =
  ## Tests closing store while operations are being cancelled.

  test "close store with cancelled operations pending":
    let ds = await factory()

    let k = (key / "close-cancel").tryGet
    (await ds.put(k, "value".toBytes)).tryGet

    # Start operations and cancel them
    var futures: seq[Future[?!RawKVRecord]]
    for _ in 0 ..< 5:
      futures.add(ds.get(k))

    # Cancel them
    for fut in futures:
      if not fut.finished():
        await fut.cancelAndWait()

    # Close should work
    (await ds.close()).tryGet

  test "cancel operation then immediately close":
    let ds = await factory()

    let k = (key / "cancel-close").tryGet
    (await ds.put(k, "value".toBytes)).tryGet

    let getFut = ds.get(k)
    discard await cancelAfterSteps(getFut, 1)

    # Immediately close
    (await ds.close()).tryGet

# =============================================================================
# Aggregate Test Runner
# =============================================================================

proc cancellationTests*(factory: StoreFactory, key: Key) =
  ## Run all cancellation tests.
  basicCancellationTests(factory, key)
  batchCancellationTests(factory, key)
  iteratorCancellationTests(factory, key)
  rapidCancelRetryTests(factory, key)
  closeDuringCancellationTests(factory, key)
