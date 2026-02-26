{.push raises: [].}

## Common utilities for threading support in kvstore backends.
##
## Provides TaskCtx pattern for bridging taskpools to Chronos async,
## with cancellation-safe waiting.

when not compileOption("threads"):
  {.error: "taskutils requires --threads:on".}

import std/isolation
import std/locks
import std/hashes

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable/results
import pkg/threading/smartptrs

import ./types

export isolation
export types
export locks
export threadsync
export smartptrs

const
  ## Duration of fairness time slot. If exceeded, operations yield.
  ## Set high enough to batch multiple operations before yielding.
  TimeSlotDuration = 1.milliseconds
  LastCalledInterval = 10.milliseconds

type
  TaskCtx*[T] = object
    ## Bundles per-task state for cross-thread communication.
    ##
    ## - signal: Completion notification (per-task)
    ## - result: Output value wrapped in Isolated for thread-safe transfer
    ##
    ## Workers must manually unpack results and recreate exceptions with their
    ## original types to satisfy Isolated's compile-time isolation check.
    ## This preserves both error messages and exception types (e.g., KVConflictError).
    ##
    ## Memory management: Use SharedPtr[TaskCtx[T]] for automatic cleanup via
    ## atomic reference counting. SharedPtr handles cross-thread ownership safely.
    signal*: ThreadSignalPtr
    result*: Isolated[?!T]

  TaskFut* = Future[void].Raising([AsyncError, CancelledError])
  OnError* = proc() {.async: (raises: [CancelledError]).}

proc awaitSignal*(
    taskFut: TaskFut, onError: OnError = nil
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Cancellation-safe wait for task completion.
  ##
  ## Uses join() + noCancel pattern to ensure worker completes before exit:
  ## - join() creates wrapper future - cancelling it does NOT cancel original
  ## - On error/cancel: noCancel waits for worker to complete
  ## - Ensures worker completes before we free heap-allocated ctx

  let joinFut = taskFut.join()

  if err =? catch(await joinFut).errorOption:
    if not onError.isNil:
      await noCancel onError()

    # Must wait for worker to finish - without this we'd write to freed memory
    ?catch(await noCancel taskFut)
    if err of CancelledError:
      raise (ref CancelledError)(err)
    return failure(err)

  success()

proc hash*[T](fut: Future[T]): Hash =
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

var
  fairnessTimeSlot {.global.}: Moment
  fairnessLastCalled {.global.}: Moment
  fairnessInitialized {.global.}: bool = false
