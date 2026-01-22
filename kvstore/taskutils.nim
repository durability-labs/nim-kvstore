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

export isolation
export locks
export threadsync
export smartptrs

type TaskCtx*[T] = object
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

proc `=destroy`*[T](ctx: var TaskCtx[T]) =
  ## Destructor cleans up Isolated result field.
  ## Called automatically when SharedPtr's refcount drops to zero.
  `=destroy`(ctx.result)

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
