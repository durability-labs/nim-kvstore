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

export isolation
export locks
export threadsync

type TaskCtx*[T] = object
  ## Bundles per-task state for cross-thread communication.
  ##
  ## - signal: Completion notification (per-task)
  ## - result: Output value wrapped in Isolated for thread-safe transfer
  ##
  ## Error messages are preserved but exception types become generic CatchableError.
  signal*: ThreadSignalPtr
  result*: Isolated[?!T]

# NOTE: No =destroy hook - we manage memory manually with allocShared/deallocShared
# Adding a destructor interferes with ORC when TaskCtx is used with raw pointers.

type TaskCtxPtr*[T] = ptr TaskCtx[T]

proc new*[T](_: type TaskCtxPtr[T], signal: ThreadSignalPtr): TaskCtxPtr[T] =
  ## Allocate a TaskCtx on the shared heap.
  ## MUST be freed with freeTaskCtx after use.
  let res = cast[TaskCtxPtr[T]](allocShared0(sizeof(TaskCtx[T])))
  res.signal = signal

  res

proc freeTaskCtx*[T](ctx: TaskCtxPtr[T]) =
  ## Free a heap-allocated TaskCtx.
  if ctx != nil:
    deallocShared(ctx)

proc awaitSignal*(
    taskFut: Future[void].Raising([AsyncError, CancelledError])
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Cancellation-safe wait for task completion.
  ##
  ## Uses join() + noCancel pattern to ensure worker completes before exit:
  ## - join() creates wrapper future - cancelling it does NOT cancel original
  ## - On error/cancel: noCancel waits for worker to complete
  ## - Ensures worker completes before we free heap-allocated ctx

  let joinFut = taskFut.join()

  if err =? catch(await joinFut).errorOption:
    # Must wait for worker to finish - without this we'd write to freed memory
    ?catch(await noCancel taskFut)
    if err of CancelledError:
      raise (ref CancelledError)(err)
    return failure(err)

  success()

proc hash*[T](fut: Future[T]): Hash =
  ## Hash a chronos Future by its pointer address.
  hash(cast[pointer](fut))
