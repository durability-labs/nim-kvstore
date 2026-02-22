{.push raises: [].}

## Synchronous filesystem I/O operations and threadpool task workers.
##
## These are pure blocking operations with no async concerns.
## They are used by the async layer in fsds.nim via threadpool spawning.

import std/os
import std/sequtils
import std/sysrand

import pkg/chronicles
import pkg/questionable
import pkg/questionable/results
import pkg/stew/endians2
import pkg/stew/io2
import pkg/stew/byteutils

import ../key
import ../kvstore
import ../taskutils
import ./metrics

const
  TokenBytes* = sizeof(uint64)
  FileExt* = "dsobj"
  EmptyBytes* = newSeq[byte](0)

# =============================================================================
# File Utilities
# =============================================================================

proc generateTempSuffix(): ?!string {.gcsafe.} =
  ## Generate cryptographically secure random suffix for temp files.
  ## Returns 16-character hex string (8 random bytes).
  let bytes =
    ?catch(urandom(8)).toKVError("Failed to generate random bytes", KVStoreBackendError)
  success bytes.toHex

proc moveFile(src, dst: string): ?!void {.gcsafe.} =
  try:
    os.moveFile(src, dst)
    success()
  except Exception as exc:
    failure newException(
      KVStoreBackendError, "unable to move '" & src & "' to '" & dst & "': " & exc.msg
    )

proc syncParentDirectory(path: string) {.gcsafe.} =
  ## Sync parent directory to ensure rename/delete operations are durable.
  ## On POSIX: opens directory, fsyncs it, closes it.
  ## On Windows: no-op (directory sync not supported/undefined behavior).
  when defined(posix):
    let dir = parentDir(path)
    let handle = openFile(dir, {OpenFlags.Read}).valueOr:
      return # Best effort - don't fail if we can't sync
    discard fsync(handle)
    discard closeFile(handle)

# =============================================================================
# Sync I/O Operations (blocking, called from threadpool workers)
# =============================================================================

proc readVersioned*(
    path: string, key: Key, data = true
): ?!RawKVRecord {.gcsafe, raises: [].} =
  if not isFile(path):
    return failure newException(KVStoreKeyNotFound, "file does not exist: " & path)

  let handle =
    ?openFile(path, {OpenFlags.Read}).toKVError(
      context = "Unable to open file: " & path, errType = KVStoreBackendError
    )

  defer:
    discard closeFile(handle)

  let size =
    ?getFileSize(handle).toKVError(
      context = "Unable to get file size: " & path, errType = KVStoreBackendError
    )

  if size < TokenBytes:
    return failure newException(KVStoreCorruption, "File too small for record: " & path)

  var header: array[TokenBytes, byte]
  let headerRead =
    ?readFile(handle, header).toKVError(
      context = "Unable to read token header", errType = KVStoreCorruption
    )

  if headerRead != TokenBytes.uint:
    return failure newException(KVStoreCorruption, "Unable to read token header")

  let token = uint64.fromBytesLE(header)
  let payloadLen = size - TokenBytes

  let value =
    if data:
      var value = newSeq[byte](payloadLen)
      if payloadLen > 0:
        let payloadRead =
          ?readFile(handle, value).toKVError(
            context = "Unable to read payload", errType = KVStoreBackendError
          )
        if payloadRead != payloadLen.uint:
          return failure newException(KVStoreBackendError, "unable to read payload")
      value
    else:
      EmptyBytes

  return success RawKVRecord.init(key, value, token)

proc writeVersioned*(
    path: string, token: uint64, value: seq[byte]
): ?!void {.gcsafe, raises: [].} =
  let dir = parentDir(path)
  if not dirExists(dir):
    if createPath(dir).isErr:
      return
        failure newException(KVStoreBackendError, "unable to create parent directory")

  let handle =
    ?openFile(path, {OpenFlags.Write, OpenFlags.Create, OpenFlags.Truncate}).toKVError(
      context = "Unable to open file '" & path & "'", errType = KVStoreBackendError
    )

  defer:
    discard closeFile(handle)

  let
    header = token.toBytesLE()
    headerWritten =
      ?writeFile(handle, header).toKVError(
        context = "Failed writing token", errType = KVStoreBackendError
      )

  if headerWritten != TokenBytes.uint:
    return failure newException(KVStoreBackendError, "Failed writing token")

  if value.len > 0:
    let valueWritten =
      ?writeFile(handle, value).toKVError(
        context = "Failed writing data", errType = KVStoreBackendError
      )

    if valueWritten != value.len.uint:
      return failure newException(KVStoreBackendError, "Failed writing data")

  # TODO: fsync disabled for benchmarking - restore with configurable sync mode
  # if fsync(handle).isErr:
  #   return failure newException(KVStoreBackendError, "Failed to sync file")
  # syncParentDirectory(path)

  return success()

proc deleteFile(path: string): ?!void {.gcsafe, raises: [].} =
  if io2.removeFile(path).isErr:
    return failure newException(KVStoreBackendError, "unable to delete file: " & path)
  # syncParentDirectory(path)
  success()

proc getSync*(path: string, key: Key): ?!RawKVRecord =
  return readVersioned(path, key)

proc getSyncMany*(keys: seq[(string, Key)]): ?!seq[RawKVRecord] =
  var records: seq[RawKVRecord]
  for (path, key) in keys:
    without record =? readVersioned(path, key), err:
      if err of KVStoreKeyNotFound:
        trace "Key not found", key = $key, err = err.msg
        continue
      else:
        return failure(err)

    records.add(record)

  success records

proc putSync*(path: string, record: RawKVRecord): ?!void =
  if not isFile(path):
    if record.token != 0:
      return failure newException(
        KVConflictError, "Token not 0 for new record " & $record.key
      )
    else:
      ?writeVersioned(path, 1'u64, record.val)
  else:
    let current = ?readVersioned(path, record.key)
    if current.token != record.token:
      return failure newException(
        KVConflictError,
        "Token mismatch for record " & $record.key & ", expected " & $record.token &
          ", got " & $current.token,
      )
    else:
      ?writeVersioned(path, current.token + 1, record.val)

  return success()

proc putSyncMany*(records: seq[(string, RawKVRecord)]): ?!seq[Key] =
  var skipped: seq[Key]
  for (path, record) in records:
    if err =? putSync(path, record).errorOption:
      if err of KVConflictError:
        kvstore_fs_put_conflict_total.inc()
        trace "Unable to put record due to conflict", key = record.key, err = err.msg
        skipped.add(record.key)
      else:
        error "Error putting record", key = record.key, err = err.msg
        return failure(err)

  success skipped

proc deleteSync*(path: string, record: KeyKVRecord): ?!void =
  if not isFile(path):
    return
      failure newException(KVConflictError, "KVRecord does not exist: " & $record.key)

  let current = ?readVersioned(path, record.key)
  if current.token != record.token:
    return failure newException(
      KVConflictError,
      "Token mismatch for record " & $record.key & ", expected " & $record.token &
        ", got " & $current.token,
    )

  discard ?catch(deleteFile(path))
  return success()

proc deleteSyncMany*(records: seq[(string, KeyKVRecord)]): ?!seq[Key] =
  var skipped: seq[Key]
  for (path, record) in records:
    if err =? deleteSync(path, record).errorOption:
      if err of KVConflictError:
        trace "Unable to delete record due to conflict", key = record.key, err = err.msg
        skipped.add(record.key)
      else:
        error "Error deleting records", err = err.msg
        return failure err

  success skipped

# =============================================================================
# Task Workers (top-level procs for threadpool)
# =============================================================================

proc runHasTask*(ctx: SharedPtr[TaskCtx[bool]], path: string) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runHasTask", error = res.error

  ctx[].result = isolate(success(isFile(path)))

proc runHasTaskMany*(
    ctx: SharedPtr[TaskCtx[seq[string]]], paths: seq[string]
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runHasTask", error = res.error

  ctx[].result = isolate(success(paths.filterIt(isFile(it))))

proc runGetTask*(
    ctx: SharedPtr[TaskCtx[RawKVRecord]], path: string, key: Key
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runGetTask", error = res.error

  ctx[].result = unsafeIsolate(getSync(path, key))

proc runGetTaskMany*(
    ctx: SharedPtr[TaskCtx[seq[RawKVRecord]]], keys: seq[(string, Key)]
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runGetTask", error = res.error

  ctx[].result = unsafeIsolate(getSyncMany(keys))

proc runPutTask*(
    ctx: SharedPtr[TaskCtx[void]], path: string, record: RawKVRecord
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runPutTask", error = res.error

  ctx[].result = unsafeIsolate(putSync(path, record))

proc runPutTaskMany*(
    ctx: SharedPtr[TaskCtx[seq[Key]]], records: seq[(string, RawKVRecord)]
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runPutTask", error = res.error

  ctx[].result = unsafeIsolate(putSyncMany(records))

proc runDeleteTask*(
    ctx: SharedPtr[TaskCtx[void]], path: string, record: KeyKVRecord
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDeleteTask", error = res.error

  ctx[].result = unsafeIsolate(deleteSync(path, record))

proc runDeleteTaskMany*(
    ctx: SharedPtr[TaskCtx[seq[Key]]], records: seq[(string, KeyKVRecord)]
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDeleteTask", error = res.error

  ctx[].result = unsafeIsolate(deleteSyncMany(records))

proc runReadRecordTask*(
    ctx: SharedPtr[TaskCtx[RawKVRecord]], path: string, key: Key, includeValue: bool
) {.gcsafe.} =
  ## Task worker for reading a single record from disk.
  ## Walker stepping happens on the async thread; this only does file I/O.
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runReadRecordTask", error = res.error

  ctx[].result = unsafeIsolate(readVersioned(path, key, includeValue))
