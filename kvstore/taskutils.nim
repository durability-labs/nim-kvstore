{.push raises: [].}

## Common utilities for threading support in kvstore backends.
##
## Bridges threadspawn results (ThreadSpawnRes[T], string errors) to ?!T
## at the async boundary via fromSpawn.

when not compileOption("threads"):
  {.error: "taskutils requires --threads:on".}

import std/locks
import std/hashes

import pkg/chronos
import pkg/questionable/results
import pkg/threadspawn

import ./types

export locks
export types
export threadspawn

const
  ## Duration of fairness time slot. If exceeded, operations yield.
  ## Set high enough to batch multiple operations before yielding.
  TimeSlotDuration = 1.milliseconds
  LastCalledInterval = 10.milliseconds

template fromSpawn*[T, E](res: Result[T, E]): ?!T =
  ## Convert a threadspawn bridge result to a KVStore error result.
  ## ThreadSpawnRes string errors become KVStoreError with the message
  ## preserved; ref CatchableError results keep KVStoreError subtypes.
  res.mapErr(
    proc(e: E): ref CatchableError =
      when E is string:
        newException(KVStoreError, e)
      elif E is ref CatchableError:
        if e of KVStoreError:
          e
        else:
          newException(KVStoreError, e.msg)
      else:
        newException(KVStoreError, $e)
  )

proc hash*(fut: FutureBase): Hash =
  ## Hash a chronos Future by its pointer address.
  hash(cast[pointer](fut))

proc boundedToken*(token: uint64): ?!int64 =
  if token > uint64(high(int64)):
    return failure(newException(KVStoreCorruption, "Token overflow"))

  success token.int64

template toKVError*[T, E](
    self: Result[T, E], context: string = "Error", errType: typedesc = KVStoreError
): ?!T =
  ## Convert any error to a KVStore error.
  ##
  ## If the error is already a KVStoreError subtype at runtime, it is preserved
  ## as-is to avoid flattening the error hierarchy (context is not added).
  ##
  ## Usage:
  ##   let signal = ?ThreadSignalPtr.new().toKVError()
  ##   let signal = ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal")
  ##   let handle = ?openFile(path, flags).toKVError(errType = KVStoreBackendError)
  ##
  when E is ref CatchableError:
    self.mapErr(
      proc(e: E): ref CatchableError =
        # Preserve existing KVStoreError subtypes at runtime
        if e of KVStoreError:
          e
        else:
          newException(errType, context & ": " & e.msg, parentException = e)
    )
  elif E is string:
    self.mapErr(
      proc(e: string): ref CatchableError =
        newException(errType, context & ": " & e)
    )
  else:
    self.mapErr(
      proc(e: E): ref CatchableError =
        newException(errType, context & ": " & $e)
    )

template batchChunks*(items: typed, maxChunkSize: int, chunkName, body: untyped) =
  ## Iterate over `items` in size-limited chunks (max `maxChunkSize` items per chunk).
  ##
  ## Injects `chunkName` as the current chunk slice and `chunkIdx` as
  ## the zero-based chunk index. The body is executed once per chunk.
  ##
  ## This is useful when operations have a hard limit on items per batch
  ## (e.g., SQLite parameter limits, API batch size limits).
  ##
  ## Usage:
  ##   batchChunks(mySeq, 1000, chunk):
  ##     # `chunk` has at most 1000 items, `chunkIdx` is 0-based
  ##     processBatch(chunk)

  let
    batchLen = items.len
    batchNumChunks =
      if batchLen == 0 or maxChunkSize <= 0:
        0
      else:
        (batchLen + maxChunkSize - 1) div maxChunkSize # Ceiling division
    batchChunkSize = maxChunkSize

  for chunkIdx {.inject.} in 0 ..< batchNumChunks:
    let
      batchStart = chunkIdx * batchChunkSize
      batchEnd = min(batchStart + batchChunkSize, batchLen)
    var chunkName = items[batchStart ..< batchEnd]
    body
