{.push raises: [].}

import std/strutils

import pkg/unittest2
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
