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

type FsWriteConfig* = object
  ## Write durability configuration for the filesystem backend.
  ## directIO: bypass page cache (O_DIRECT). Default false -- opt-in only,
  ##   may cause EINVAL on some platforms/filesystems with alignment issues.
  ## fsyncFile: fsync the file before closing. Default true.
  ## fsyncDir: fsync the parent directory after rename/delete. Default true.
  directIO*: bool
  fsyncFile*: bool
  fsyncDir*: bool

const DefaultFsWriteConfig* =
  FsWriteConfig(directIO: false, fsyncFile: true, fsyncDir: true)

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

proc syncParentDirectory(path: string, config: FsWriteConfig) {.gcsafe.} =
  ## Sync parent directory to ensure rename/delete operations are durable.
  ## On POSIX: opens directory, fsyncs it, closes it.
  ## On Windows: no-op (directory sync not supported/undefined behavior).
  if not config.fsyncDir:
    return
  when defined(posix):
    let dir = parentDir(path)
    let handle = openFile(dir, {OpenFlags.Read}).valueOr:
      return # Best effort - don't fail if we can't sync
    discard fsync(handle)
    discard closeFile(handle)

# =============================================================================
# Sync I/O Operations (blocking, called from threadpool workers)
# =============================================================================

proc openFileOrKeyNotFound(path: string): ?!IoHandle =
  ## Open file, mapping ENOENT to KVStoreKeyNotFound
  let res = openFile(path, {OpenFlags.Read})
  if res.isErr:
    let errCode = res.error
    # ENOENT = 2 on most systems
    if errCode == 2.IoErrorCode:
      return failure newException(KVStoreKeyNotFound, "file does not exist: " & path)
    return failure newException(
      KVStoreBackendError, "Unable to open file: " & path & " (error: " & $errCode & ")"
    )
  success res.value

proc readVersioned*(
    path: string, key: Key, data = true
): ?!RawKVRecord {.gcsafe, raises: [].} =
  # Optimization: Removed isFile check - saves 1 syscall
  # openFileOrKeyNotFound maps ENOENT to KeyNotFound
  let handle = ?openFileOrKeyNotFound(path)

  defer:
    discard closeFile(handle)

  let size =
    ?getFileSize(handle).toKVError(
      context = "Unable to get file size: " & path, errType = KVStoreBackendError
    )

  if size < TokenBytes:
    return failure newException(KVStoreCorruption, "File too small for record: " & path)

  # Optimization: Read entire file in one operation instead of separate header + payload reads
  var entireFile = newSeq[byte](size.int)
  let bytesRead =
    ?readFile(handle, entireFile).toKVError(
      context = "Unable to read file: " & path, errType = KVStoreBackendError
    )

  if bytesRead != size.uint:
    return failure newException(KVStoreBackendError, "unable to read complete file")

  let token = uint64.fromBytesLE(entireFile[0 ..< TokenBytes])

  let value =
    if data:
      entireFile[TokenBytes ..< entireFile.len]
    else:
      EmptyBytes

  return success RawKVRecord.init(key, value, token)

proc writeVersioned*(
    path: string, token: int64, value: seq[byte], config = DefaultFsWriteConfig
): ?!void {.gcsafe, raises: [].} =
  let dir = parentDir(path)
  if not dirExists(dir):
    if createPath(dir).isErr:
      return
        failure newException(KVStoreBackendError, "unable to create parent directory")
  # Write to a temp file first, then rename. This prevents partial writes
  # or truncation-incorrect file on Windows, where writing shorter content over
  # a longer file without rename can leave trailing bytes from the old content.
  let
    suffix = ?generateTempSuffix()
    tmp = path & "." & suffix & ".tmp"

  var completed = false
  defer:
    if not completed:
      if err =? io2.removeFile(tmp).errorOption:
        warn "Failed to remove temp file", tmp, error = $err

  var openFlags = {OpenFlags.Write, OpenFlags.Create, OpenFlags.Truncate}
  if config.directIO:
    openFlags.incl(OpenFlags.Direct)

  block writeBlock:
    let handle =
      ?openFile(tmp, openFlags).toKVError(
        context = "Unable to open temp file '" & tmp & "'",
        errType = KVStoreBackendError,
      )

    defer:
      discard closeFile(handle)

    let
      header = token.uint64.toBytesLE()
      headerWritten =
        ?writeFile(handle, header).toKVError(
          context = "Failed writing token", errType = KVStoreBackendError
        )

    if headerWritten != TokenBytes.uint:
      return failure newException(KVStoreBackendError, "Failed writing token")

    let valueWritten =
      ?writeFile(handle, value).toKVError(
        context = "Failed writing data", errType = KVStoreBackendError
      )

    if valueWritten != value.len.uint:
      return failure newException(KVStoreBackendError, "Failed writing data")

    if config.fsyncFile:
      if fsync(handle).isErr:
        return failure newException(KVStoreBackendError, "Failed to sync file")

  # On Windows the file must be closed before it can be renamed.
  if err =? moveFile(tmp, path).errorOption:
    return failure(err)

  completed = true
  syncParentDirectory(path, config)
  return success()

proc deleteFile(path: string, config: FsWriteConfig): ?!void {.gcsafe, raises: [].} =
  if io2.removeFile(path).isErr:
    return failure newException(KVStoreBackendError, "unable to delete file: " & path)
  syncParentDirectory(path, config)
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

proc putSync*(
    path: string, record: RawKVRecord, config = DefaultFsWriteConfig
): ?!void =
  if not isFile(path):
    if record.token != 0:
      return failure newException(
        KVConflictError, "Token not 0 for new record " & $record.key
      )
    else:
      ?writeVersioned(path, 1, record.val, config)
  else:
    let current = ?readVersioned(path, record.key)
    if current.token != record.token:
      return failure newException(
        KVConflictError,
        "Token mismatch for record " & $record.key & ", expected " & $record.token &
          ", got " & $current.token,
      )
    else:
      ?writeVersioned(
        path, ?boundedToken((current.token + 1).uint64), record.val, config
      )

  return success()

proc putSyncMany*(
    records: seq[(string, RawKVRecord)], config = DefaultFsWriteConfig
): ?!seq[Key] =
  var skipped: seq[Key]
  for (path, record) in records:
    if err =? putSync(path, record, config).errorOption:
      if err of KVConflictError:
        kvstore_fs_put_conflict_total.inc()
        trace "Unable to put record due to conflict", key = record.key, err = err.msg
        skipped.add(record.key)
      else:
        error "Error putting record", key = record.key, err = err.msg
        return failure(err)

  success skipped

proc deleteSync*(
    path: string, record: KeyKVRecord, config = DefaultFsWriteConfig
): ?!void =
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

  discard ?catch(deleteFile(path, config))
  return success()

proc deleteSyncMany*(
    records: seq[(string, KeyKVRecord)], config = DefaultFsWriteConfig
): ?!seq[Key] =
  var skipped: seq[Key]
  for (path, record) in records:
    if err =? deleteSync(path, record, config).errorOption:
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
    ctx: SharedPtr[TaskCtx[void]],
    path: string,
    record: RawKVRecord,
    config: FsWriteConfig,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runPutTask", error = res.error
  ctx[].result = unsafeIsolate(putSync(path, record, config))

proc runPutTaskMany*(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    records: seq[(string, RawKVRecord)],
    config: FsWriteConfig,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runPutTask", error = res.error
  ctx[].result = unsafeIsolate(putSyncMany(records, config))

proc runDeleteTask*(
    ctx: SharedPtr[TaskCtx[void]],
    path: string,
    record: KeyKVRecord,
    config: FsWriteConfig,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDeleteTask", error = res.error
  ctx[].result = unsafeIsolate(deleteSync(path, record, config))

proc runDeleteTaskMany*(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    records: seq[(string, KeyKVRecord)],
    config: FsWriteConfig,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDeleteTask", error = res.error
  ctx[].result = unsafeIsolate(deleteSyncMany(records, config))

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
