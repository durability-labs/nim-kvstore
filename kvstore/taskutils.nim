{.push raises: [].}

## Common utilities for threading support in kvstore backends.
##
## Bridges threadspawn results (ThreadSpawnRes[T, KVSpawnError], typed
## errors) to ?!T at the async boundary via fromSpawn; spawnJoin's errMap
## (spawnErrToException) reconstructs the typed KVStoreError at the
## caller side.

when not compileOption("threads"):
  {.error: "taskutils requires --threads:on".}

import std/locks
import std/hashes

import pkg/chronos
import pkg/questionable/results
import pkg/taskpools
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

type KVStoreErrorKind* = enum
  Generic ## Unclassified backend failure.
  Conflict ## Destination already exists (move conflict).
  KeyNotFound ## Key absent from the store.

type KVSpawnError* = object
  kind*: KVStoreErrorKind
  msg*: string

proc `$`*(e: KVSpawnError): string =
  e.msg

proc toKVSpawnError*(err: ref CatchableError): KVSpawnError {.gcsafe, raises: [].} =
  ## Map a backend exception to the typed spawn channel error.  The kind
  ## survives the thread boundary so the public API can reconstruct the
  ## typed exception on unpack.
  KVSpawnError(
    kind:
      if err of KVConflictError:
        Conflict
      elif err of KVStoreKeyNotFound:
        KeyNotFound
      else:
        Generic,
    msg: err.msg,
  )

proc spawnErrToException*(e: KVSpawnError): ref CatchableError {.gcsafe, raises: [].} =
  ## Reconstruct the typed KVStore exception from a spawn channel error.
  case e.kind
  of Conflict:
    newException(KVConflictError, e.msg)
  of KeyNotFound:
    newException(KVStoreKeyNotFound, e.msg)
  of Generic:
    newException(KVStoreError, e.msg)

proc spawnJoinKVS*[T](
    spawnFn: SpawnFn[T, KVSpawnError]
): Future[Result[T, SpawnUserError[KVSpawnError]]] {.async: (raises: [CancelledError]).} =
  ## Join a spawned worker, converting spawnJoin's raised contract failures
  ## to SpawnUserError[KVSpawnError] with the generic kind, so store methods
  ## keep surfacing typed failures instead of exceptions.
  try:
    await spawnJoin[T, KVSpawnError](spawnFn)
  except SpawnContractError as e:
    return err(toSpawnError(KVSpawnError(kind: Generic, msg: e.msg)))

template spawnJoinOn*[T](
    tp: Taskpool, worker: untyped, args: varargs[untyped]
): untyped =
  ## Spawn `worker(ctx, args...)` on `tp` and join via `spawnJoin`,
  ## mapping contract failures to generic KVSpawnErrors.
  ## Collapses the repetitive SpawnFn wrapper at every call site.
  ## Returns `untyped` so the expansion's typed-raises future flows through.
  let spawnFn = proc(ctx: SharedPtr[TaskCtx[T, KVSpawnError]]) {.gcsafe, raises: [].} =
    tp.spawn worker(ctx, args)
  spawnJoinKVS[T](spawnFn)

template toSpawnRes*[T](
    exp: Result[T, ref CatchableError]
): ThreadSpawnRes[T, KVSpawnError] =
  ## Convert a worker result to the threadspawn channel, mapping the error
  ## kind so the caller can reconstruct the typed exception.  The result is
  ## isolated here, so callers must not wrap it again.
  isolate exp.mapErr(
    proc(e: ref CatchableError): KVSpawnError =
      toKVSpawnError(e)
  )

template fromSpawn*[T](res: Result[T, SpawnUserError[KVSpawnError]]): ?!T =
  ## Convert a spawnJoin result to a KVStore error result.  The worker's
  ## typed error (inside the SpawnUserError envelope) is mapped back to the
  ## KVStore exception; the message is preserved.
  res.mapErr(
    proc(e: SpawnUserError[KVSpawnError]): ref CatchableError =
      spawnErrToException(e.error)
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
