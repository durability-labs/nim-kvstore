{.push raises: [].}

import std/strutils
import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/questionable/results

import pkg/kvstore/taskutils

suite "TaskUtils":
  test "toKVError preserves KVStoreError subtypes":
    # Create a Result containing a KVConflictError
    let conflictErr = newException(KVConflictError, "test conflict")
    let res = Result[int, ref CatchableError].err(conflictErr)

    # Apply toKVError - should preserve the subtype
    let converted = res.toKVError(context = "should be ignored")

    check converted.isErr
    check converted.error of KVConflictError
    check converted.error.msg == "test conflict" # Original message preserved

  test "toKVError wraps non-KVStoreError exceptions":
    # Create a Result containing a generic CatchableError
    let genericErr = newException(CatchableError, "generic error")
    let res = Result[int, ref CatchableError].err(genericErr)

    # Apply toKVError - should wrap with context
    let converted = res.toKVError(context = "operation failed")

    check converted.isErr
    check converted.error of KVStoreError
    check "operation failed" in converted.error.msg
    check "generic error" in converted.error.msg

  test "toKVError with string error type":
    let res = Result[int, string].err("string error message")

    let converted = res.toKVError(context = "operation failed")

    check converted.isErr
    check converted.error of KVStoreError
    check "operation failed" in converted.error.msg
    check "string error message" in converted.error.msg

  test "toKVError with custom error type":
    let res =
      Result[int, ref CatchableError].err(newException(CatchableError, "generic"))

    let converted =
      res.toKVError(context = "custom context", errType = KVStoreBackendError)

    check converted.isErr
    check converted.error of KVStoreBackendError
    check "custom context" in converted.error.msg

suite "batchChunks":
  test "empty sequence executes body zero times":
    let items: seq[int] = @[]
    var callCount = 0
    batchChunks(items, 100, chunk):
      check chunk.len >= 0
      callCount.inc
    check callCount == 0

  test "concatenated chunks reconstruct the original sequence":
    let items = @[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    var collected: seq[int]
    batchChunks(items, 3, chunk):
      for x in chunk:
        collected.add(x)
    check collected == items

  test "no chunk exceeds maxChunkSize":
    let items = @[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    batchChunks(items, 3, chunk):
      check chunk.len <= 3

  test "no empty chunks are produced":
    let items = @[1, 2, 3, 4, 5]
    var chunkLengths: seq[int]
    batchChunks(items, 10, chunk):
      chunkLengths.add(chunk.len)
    for length in chunkLengths:
      check length >= 1

  test "chunkIdx is correctly injected and sequential":
    let items = @[10, 20, 30, 40, 50, 60]
    var indices: seq[int]
    batchChunks(items, 2, chunk):
      discard chunk.len
      indices.add(chunkIdx)
    check indices == toSeq(0 ..< indices.len)

  test "single chunk when items fit within maxChunkSize":
    let items = @[1, 2, 3, 4, 5]
    var callCount = 0
    batchChunks(items, 100, chunk):
      callCount.inc
      check chunk == items
    check callCount == 1

  test "ceiling division: 10 items, maxChunkSize 3 creates 4 chunks":
    let items = @[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    var chunkCount = 0
    var totalItems = 0
    batchChunks(items, 3, chunk):
      chunkCount.inc
      totalItems.inc(chunk.len)
    check chunkCount == 4
    check totalItems == 10

  test "zero or negative maxChunkSize executes body zero times":
    let items = @[1, 2, 3]
    var callCount = 0
    batchChunks(items, 0, chunk):
      callCount.inc
    check callCount == 0

    callCount = 0
    batchChunks(items, -1, chunk):
      callCount.inc
    check callCount == 0

  test "simulates SQL parameter limit chunking":
    # Simulates SQLite batching with max 1000 params per query
    let items = toSeq(1 .. 2500)
    const MaxSqliteParams = 1000
    var chunkCount = 0
    var totalItems = 0
    batchChunks(items, MaxSqliteParams, chunk):
      chunkCount.inc
      totalItems.inc(chunk.len)
      check chunk.len <= MaxSqliteParams
    check chunkCount == 3
    check totalItems == 2500

  test "all chunk bounds are valid for various sizes":
    # Test invariants for various total/maxChunkSize combinations
    for total in 1 .. 20:
      for maxChunkSize in 1 .. 20:
        let items = toSeq(1 .. total)
        var chunkCount = 0
        batchChunks(items, maxChunkSize, chunk):
          chunkCount.inc
          # Every chunk must be non-empty and not exceed maxChunkSize
          check chunk.len >= 1
          check chunk.len <= maxChunkSize
        # Verify ceiling division: numChunks = ceil(total / maxChunkSize)
        let expectedChunks = (total + maxChunkSize - 1) div maxChunkSize
        check chunkCount == expectedChunks
