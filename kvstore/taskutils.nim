{.push raises: [].}

## Common utilities for threading support in kvstore backends.
##
## Provides TaskCtx pattern for bridging taskpools to Chronos async,
## with cancellation-safe waiting.

when not compileOption("threads"):
  {.error: "taskutils requires --threads:on".}

import std/locks
import std/hashes

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable/results

export locks
export threadsync

type
  TaskCtx*[T] = object
    ## Bundles per-task state for cross-thread communication.
    ##
    ## - lock: Optional store-level lock (nil if not needed)
    ## - signal: Completion notification (per-task)
    ## - result: Output value (per-task)
    lock*: ptr Lock
    signal*: ThreadSignalPtr
    result*: ?!T

proc awaitSignal*(signal: ThreadSignalPtr): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Cancellation-safe wait for task completion.
  ##
  ## Uses join() + noCancel pattern to ensure worker completes before exit:
  ## - join() creates wrapper future - cancelling it does NOT cancel original
  ## - On error/cancel: noCancel waits for worker to complete
  ## - Ensures ctx (stack-allocated) is never a dangling pointer
  let taskFut = signal.wait()
  if err =? catch(await taskFut.join()).errorOption:
    # Must wait for worker to finish - without this we'd write to freed memory
    ?catch(await noCancel taskFut)
    if err of CancelledError:
      raise (ref CancelledError)(err)
    return failure(err)
  success()

proc hash*[T](fut: Future[T]): Hash =
  ## Hash a chronos Future by its pointer address.
  hash(cast[pointer](fut))
