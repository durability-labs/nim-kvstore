{.push raises: [].}

## Common tests for close() and dispose() behavior across all backends.
## Tests iterator tracking, error collection, and cleanup semantics.
##
## Tests are split into two categories:
## - Tests that can share a store instance (iteratorDisposeTests)
## - Tests that need fresh stores per test (use factory pattern)

import std/options
import std/sequtils
import std/strutils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable/results

import pkg/kvstore

# =============================================================================
# Tests for the catch(it.read) error aggregation pattern
# =============================================================================

proc catchPatternTests*() =
  ## Verifies that catch(fut.read) correctly captures errors from both:
  ## 1. Future that failed with exception
  ## 2. Future that succeeded but contains failure(err) Result

  test "catch + flatten captures Result failure from successful Future":
    # Simulate a Future[?!void] that completes successfully with failure Result
    proc failingOp(): Future[?!void] {.async: (raises: [CancelledError]).} =
      return failure(newException(CatchableError, "result failure msg"))

    let fut = failingOp()
    discard await fut  # Future completes successfully, discard the Result

    # catch alone does NOT capture Result failures - it only catches exceptions
    # We need flatten to collapse Result[Result[T,E],E] -> Result[T,E]
    let caught = catch(fut.read).flatten()
    check caught.isErr
    check "result failure msg" in caught.error.msg

  test "catch + flatten captures exception from failed Future":
    # Simulate a Future that fails with an exception
    proc throwingOp(): Future[?!void] {.async: (raises: [CancelledError, CatchableError]).} =
      raise newException(CatchableError, "future exception msg")

    let fut = throwingOp()
    try:
      discard await fut
    except CatchableError:
      discard  # Expected

    # catch captures the exception, flatten collapses the nested Result
    let caught = catch(fut.read).flatten()
    check caught.isErr
    check "future exception msg" in caught.error.msg

  test "catch + flatten returns success for successful Future with success Result":
    proc successOp(): Future[?!void] {.async: (raises: [CancelledError]).} =
      return success()

    let fut = successOp()
    discard await fut

    let caught = catch(fut.read).flatten()
    check caught.isOk

  test "error aggregation pattern with flatten collects multiple failures":
    # Simulate the pattern used in close()
    proc op1(): Future[?!void] {.async: (raises: [CancelledError]).} =
      return failure(newException(CatchableError, "error1"))

    proc op2(): Future[?!void] {.async: (raises: [CancelledError]).} =
      return failure(newException(CatchableError, "error2"))

    proc op3(): Future[?!void] {.async: (raises: [CancelledError]).} =
      return success()

    let futures = @[op1(), op2(), op3()]
    await allFutures(futures)

    # Use catch + flatten to handle both Future exceptions AND Result failures
    let errors = futures
      .filterIt(catch(it.read).flatten().isErr)
      .mapIt(catch(it.read).flatten().error)
      .mapIt(it.msg)

    check errors.len == 2
    check "error1" in errors[0]
    check "error2" in errors[1]

type
  ## Factory proc that creates a fresh store instance for each test.
  ## May raise CatchableError if store creation fails (e.g., tryGet on Result).
  StoreFactory* = proc(): Future[KVStore] {.async: (raises: [CancelledError, CatchableError]).}

# =============================================================================
# Tests that can share a store instance
# =============================================================================

proc iteratorDisposeTests*(ds: KVStore, key: Key) =
  ## Tests for iterator dispose() behavior.
  ## These tests do NOT close the store, so they can share a store instance.

  test "dispose succeeds on fresh iterator":
    let testKey = (key / "dispose" / "fresh").tryGet()
    (await ds.put(testKey, "value".toBytes)).tryGet()

    let iter = (await ds.query(Query.init((key / "dispose").tryGet()))).tryGet()
    # Dispose without consuming any items
    (await iter.dispose()).tryGet()

  test "dispose succeeds on partially consumed iterator":
    let k1 = (key / "dispose" / "partial1").tryGet()
    let k2 = (key / "dispose" / "partial2").tryGet()
    (await ds.put(k1, "v1".toBytes)).tryGet()
    (await ds.put(k2, "v2".toBytes)).tryGet()

    let iter = (await ds.query(Query.init((key / "dispose").tryGet()))).tryGet()

    # Consume one item
    let item = (await iter.next()).tryGet()
    check item.isSome

    # Dispose with remaining items
    (await iter.dispose()).tryGet()

  test "dispose succeeds on fully consumed iterator":
    let testKey = (key / "dispose" / "full").tryGet()
    (await ds.put(testKey, "value".toBytes)).tryGet()

    let iter = (await ds.query(Query.init((key / "dispose" / "full").tryGet()))).tryGet()

    # Consume all items
    while not iter.finished:
      discard (await iter.next()).tryGet()

    # Dispose after exhaustion
    (await iter.dispose()).tryGet()

  test "dispose is idempotent":
    let testKey = (key / "dispose" / "idempotent").tryGet()
    (await ds.put(testKey, "value".toBytes)).tryGet()

    let iter = (await ds.query(Query.init((key / "dispose" / "idempotent").tryGet()))).tryGet()

    # Dispose twice should both succeed
    (await iter.dispose()).tryGet()
    (await iter.dispose()).tryGet()

  test "next fails after dispose":
    let testKey = (key / "dispose" / "nextfail").tryGet()
    (await ds.put(testKey, "value".toBytes)).tryGet()

    let iter = (await ds.query(Query.init((key / "dispose" / "nextfail").tryGet()))).tryGet()
    (await iter.dispose()).tryGet()

    # next() should fail after dispose
    let result = await iter.next()
    check result.isErr

# =============================================================================
# Tests that need fresh stores (use factory pattern)
# =============================================================================

proc iteratorTrackingTests*(factory: StoreFactory, key: Key) =
  ## Tests for iterator tracking in the store.
  ## These tests close the store, so they need fresh stores per test.

  test "close disposes active iterators automatically":
    let ds = await factory()
    let k1 = (key / "tracking" / "auto1").tryGet()
    let k2 = (key / "tracking" / "auto2").tryGet()
    (await ds.put(k1, "v1".toBytes)).tryGet()
    (await ds.put(k2, "v2".toBytes)).tryGet()

    # Create iterators but don't dispose them
    let iter1 = (await ds.query(Query.init((key / "tracking").tryGet()))).tryGet()
    let iter2 = (await ds.query(Query.init((key / "tracking").tryGet()))).tryGet()

    # Consume some items from iter1
    discard (await iter1.next()).tryGet()

    # Close should dispose both iterators
    (await ds.close()).tryGet()

    # Iterators should be disposed (next fails)
    check (await iter1.next()).isErr
    check (await iter2.next()).isErr

  test "multiple iterators can be active simultaneously":
    let ds = await factory()
    let k1 = (key / "tracking" / "multi1").tryGet()
    let k2 = (key / "tracking" / "multi2").tryGet()
    (await ds.put(k1, "v1".toBytes)).tryGet()
    (await ds.put(k2, "v2".toBytes)).tryGet()

    # Create multiple iterators
    let iter1 = (await ds.query(Query.init((key / "tracking" / "multi1").tryGet()))).tryGet()
    let iter2 = (await ds.query(Query.init((key / "tracking" / "multi2").tryGet()))).tryGet()

    # Both should work independently
    let r1 = (await iter1.next()).tryGet()
    let r2 = (await iter2.next()).tryGet()

    check r1.isSome
    check r2.isSome

    # Dispose both
    (await iter1.dispose()).tryGet()
    (await iter2.dispose()).tryGet()
    (await ds.close()).tryGet()

  test "disposed iterator is unregistered from store":
    let ds = await factory()
    let testKey = (key / "tracking" / "unreg").tryGet()
    (await ds.put(testKey, "value".toBytes)).tryGet()

    let iter = (await ds.query(Query.init((key / "tracking" / "unreg").tryGet()))).tryGet()
    (await iter.dispose()).tryGet()

    # Close should succeed without errors (no iterators to dispose)
    (await ds.close()).tryGet()

proc closeAndDisposeTests*(factory: StoreFactory, key: Key) =
  ## Tests for close() behavior.
  ## These tests close the store, so they need fresh stores per test.

  test "operations fail after close":
    let ds = await factory()
    let testKey = (key / "close" / "ops").tryGet()
    (await ds.put(testKey, "value".toBytes)).tryGet()

    # Close the store
    (await ds.close()).tryGet()

    # All operations should fail
    check (await ds.has(testKey)).isErr
    check (await ds.get(testKey)).isErr
    check (await ds.put(testKey, "new".toBytes)).isErr
    check (await ds.delete(KeyRecord.init(testKey, 1))).isErr
    check (await ds.query(Query.init(key))).isErr

  test "close is idempotent":
    let ds = await factory()
    # Closing twice should succeed
    (await ds.close()).tryGet()
    (await ds.close()).tryGet()

proc concurrentCloseTests*(factory: StoreFactory, key: Key) =
  ## Tests for concurrent close and iterator behavior.
  ## These tests close the store, so they need fresh stores per test.

  test "close waits for in-flight iterator operations":
    let ds = await factory()
    let testKey = (key / "concurrent" / "inflight").tryGet()
    (await ds.put(testKey, "value".toBytes)).tryGet()

    let iter = (await ds.query(Query.init((key / "concurrent" / "inflight").tryGet()))).tryGet()

    # Start a next() call
    let nextFut = iter.next()

    # Close while next() is in progress
    let closeFut = ds.close()

    # Both should complete (close waits for next)
    discard await nextFut
    (await closeFut).tryGet()
