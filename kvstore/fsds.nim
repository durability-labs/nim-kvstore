{.push raises: [].}

when not compileOption("threads"):
  {.error: "FSKVStore requires --threads:on".}

import std/os
import std/tables
import std/strutils
import std/options
import std/random
import std/times
import std/atomics

when defined(posix):
  from std/posix import open, fsync, close, O_RDONLY

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/stew/endians2
import pkg/taskpools

import ./key
import ./query
import ./kvstore
import ./taskutils

const
  TokenBytes = sizeof(uint64)
  FileExt* = "dsobj"
  EmptyBytes* = newSeq[byte](0)

export kvstore

type
  FSKVStore* = ref object of KVStore
    ## Filesystem-backed kvstore that stores records as files.
    ##
    ## Tokens are stored as uint64 in little-endian format. Unlike SQLiteKVStore
    ## which is limited to int64 range due to SQLite INTEGER type, FSKVStore
    ## supports the full uint64 range.
    root*: string
    ignoreProtected: bool
    depth: int
    locks: TableRef[Key, AsyncLock]
    tp: Taskpool

  # Per-iterator state for query operations
  FsQueryIterState = ref object
    basePath: string
    walker: iterator(): string {.raises: [Defect], gcsafe.}
    root: string
    queryKey: Key
    queryValue: bool
    finished: Atomic[bool]
    isDisposed: Atomic[bool]
    tp: Taskpool

randomize() # TODO: We should probably use a stronger rng here?

proc moveFile(src, dst: string): ?!void {.gcsafe.} =
  try:
    os.moveFile(src, dst)
    success()
  except Exception as exc:
    failure newBackendError("unable to move '" & src & "' to '" & dst & "': " & exc.msg)

proc validDepth*(self: FSKVStore, key: Key): bool =
  key.len <= self.depth

proc isRootSubdir*(self: FSKVStore, path: string): bool =
  path.startsWith(self.root)

proc path*(self: FSKVStore, key: Key): ?!string =
  ## Return filename corresponding to the key
  ## or failure if the key doesn't correspond to a valid filename
  ##

  if not self.validDepth(key):
    return failure newBackendError("Path has invalid depth!")

  var segments: seq[string]
  for ns in key:
    let basename = ns.value.extractFilename
    if basename == "" or not basename.isValidFilename:
      return failure newBackendError("Filename contains invalid chars!")

    if ns.field == "":
      segments.add(ns.value)
    else:
      let basename = ns.field.extractFilename
      if basename == "" or not basename.isValidFilename:
        return failure newBackendError("Filename contains invalid chars!")

      # `:` are replaced with `/`
      segments.add(ns.field / ns.value)

  let absolute = ?((self.root / segments.joinPath()).absolutePath().catch())
  let fullname = absolute.addFileExt(FileExt)

  if not self.isRootSubdir(fullname):
    return failure newBackendError("Path is outside of `root` directory!")

  return success fullname

# =============================================================================
# Sync I/O Operations (blocking, called from threadpool workers)
# =============================================================================

proc readVersioned*(
    path: string, key: Key, data = true
): ?!RawRecord {.gcsafe.} =
  var file: File

  defer:
    file.close

  if not file.open(path):
    return failure newBackendError("unable to open file: " & path)

  let size = ?catch (file.getFileSize)
  if size < TokenBytes:
    return failure newCorruptionError("File too small for record: " & path)

  var header: array[TokenBytes, byte]
  if ?catch (file.readBuffer(addr header[0], TokenBytes)) != TokenBytes:
    return failure newBackendError("unable to read token header")

  let token = uint64.fromBytesLE(header)
  let payloadLen = size - TokenBytes

  let value =
    if data:
      var value = newSeq[byte](payloadLen)
      if payloadLen > 0 and ?catch (file.readBytes(value, 0, payloadLen)) != payloadLen:
        return failure newBackendError("unable to read payload")
      value
    else:
      EmptyBytes

  return success RawRecord.init(key, value, token)

proc writeVersioned*(
    path: string, token: uint64, value: seq[byte]
): ?!void {.gcsafe.} =
  ?catch(createDir(parentDir(path)))

  let tmp = path & ".tmp-" & $epochTime() & "-" & $rand(1_000_000)

  var file: File
  defer:
    ?catch(removeFile(tmp))

  try:
    if not file.open(tmp, fmWrite):
      return failure newBackendError("unable to open temporary file '" & tmp & "'")

    let header = token.toBytesLE()
    if ?catch(file.writeBuffer(addr header[0], TokenBytes)) != TokenBytes:
      return failure newBackendError("Failed writing token")

    if value.len > 0:
      if ?catch (file.writeBytes(value, 0, value.len)) != value.len:
        return failure newBackendError("Failed writing data")

    file.flushFile()
  finally:
    file.close

  ?moveFile(tmp, path)

  # Sync parent directory to ensure rename is durable
  when defined(posix):
    let dir = parentDir(path)
    let fd = posix.open(dir.cstring, O_RDONLY)
    if fd >= 0:
      discard posix.fsync(fd)
      discard posix.close(fd)

  return success()

proc deleteFile(path: string): ?!void {.gcsafe.} =
  ?catch(removeFile(path))

  # Sync parent directory to ensure delete is durable
  when defined(posix):
    let dir = parentDir(path)
    let fd = posix.open(dir.cstring, O_RDONLY)
    if fd >= 0:
      discard posix.fsync(fd)
      discard posix.close(fd)

  success()

# =============================================================================
# Task Workers (top-level procs for threadpool)
# =============================================================================

proc runHasTask(ctx: ptr TaskCtx[bool], path: string) {.gcsafe.} =
  defer: discard ctx[].signal.fireSync()
  ctx[].result = success(path.fileExists())

proc runGetTask(ctx: ptr TaskCtx[RawRecord], path: string, key: Key) {.gcsafe.} =
  defer: discard ctx[].signal.fireSync()
  if not path.fileExists():
    ctx[].result = Result[RawRecord, ref CatchableError].err(
      newException(KVStoreKeyNotFound, "Key doesn't exist"))
  else:
    ctx[].result = readVersioned(path, key)

proc runPutTask(ctx: ptr TaskCtx[bool], path: string, record: RawRecord) {.gcsafe.} =
  ## Returns true if successful, false if conflict
  defer: discard ctx[].signal.fireSync()
  
  if not path.fileExists():
    if record.token != 0:
      ctx[].result = success(false)  # Conflict: insert expected but key exists check failed... wait no
      return
    ctx[].result = writeVersioned(path, 1'u64, record.val).map(proc(_: void): bool = true)
  else:
    let currentRes = readVersioned(path, record.key)
    if currentRes.isErr:
      ctx[].result = Result[bool, ref CatchableError].err(currentRes.error)
      return
    let current = currentRes.get
    if current.token != record.token:
      ctx[].result = success(false)  # Conflict: token mismatch
    else:
      ctx[].result = writeVersioned(path, current.token + 1, record.val).map(proc(_: void): bool = true)

proc runDeleteTask(ctx: ptr TaskCtx[bool], path: string, record: KeyRecord) {.gcsafe.} =
  ## Returns true if successful, false if conflict/skip
  defer: discard ctx[].signal.fireSync()
  
  if not path.fileExists():
    ctx[].result = success(false)
    return
    
  let currentRes = readVersioned(path, record.key)
  if currentRes.isErr:
    ctx[].result = Result[bool, ref CatchableError].err(currentRes.error)
    return
  let current = currentRes.get
  if current.token != record.token:
    ctx[].result = success(false)  # Conflict
  else:
    ctx[].result = deleteFile(path).map(proc(_: void): bool = true)

proc runNextTask(ctx: ptr TaskCtx[?RawRecord], state: ptr FsQueryIterState) {.gcsafe.} =
  defer: discard ctx[].signal.fireSync()

  type R = ?!(?RawRecord)

  if state[].finished.load():
    ctx[].result = R.ok(RawRecord.none)
    return

  while true:
    let relPath = state[].walker()

    if finished(state[].walker):
      state[].finished.store(true)
      ctx[].result = R.ok(RawRecord.none)
      return

    var keyPath = state[].basePath
    keyPath.removePrefix(state[].root)
    keyPath = keyPath / relPath.changeFileExt("")
    keyPath = keyPath.replace("\\", "/")

    let keyRes = Key.init(keyPath)
    if keyRes.isErr:
      continue

    let key = keyRes.get
    if key != state[].queryKey and not state[].queryKey.ancestor(key):
      continue

    let absPathRes = catch((state[].basePath / relPath).absolutePath())
    if absPathRes.isErr:
      ctx[].result = R.err(absPathRes.error)
      return

    let recordRes = readVersioned(absPathRes.get, key, state[].queryValue)
    if recordRes.isErr:
      ctx[].result = R.err(recordRes.error)
      return

    ctx[].result = R.ok(recordRes.get.some)
    return

# =============================================================================
# Per-Key Locking (unchanged from original)
# =============================================================================

proc acquire*(
    self: FSKVStore, key: Key
): Future[AsyncLock] {.async: (raises: [CancelledError]).} =
  var lock = self.locks.mgetOrPut(key, newAsyncLock())
  await lock.acquire()
  lock

proc release*(self: FSKVStore, key: Key, lock: AsyncLock) {.raises: [].} =
  if lock.locked:
    try:
      lock.release()
    except CatchableError as err:
      raiseAssert(err.msg) # shouldn't happen
    finally:
      if not lock.locked:
        self.locks.del(key)

# =============================================================================
# Async Methods (public API)
# =============================================================================

method has*(
    self: FSKVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  let p = ?self.path(key)

  let signal = ThreadSignalPtr.new().valueOr:
    return failure(newException(KVStoreError, error))
  var ctx = TaskCtx[bool](signal: signal)
  defer: discard ctx.signal.close()

  self.tp.spawn runHasTask(addr ctx, p)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method get*(
    self: FSKVStore, key: Key
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
  let p = ?self.path(key)

  let signal = ThreadSignalPtr.new().valueOr:
    return failure(newException(KVStoreError, error))
  var ctx = TaskCtx[RawRecord](signal: signal)
  defer: discard ctx.signal.close()

  self.tp.spawn runGetTask(addr ctx, p, key)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method get*(
    self: FSKVStore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  var records: seq[RawRecord]

  for key in keys:
    let p = ?self.path(key)

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[RawRecord](signal: signal)
    defer: discard ctx.signal.close()

    self.tp.spawn runGetTask(addr ctx, p, key)
    ?await awaitSignal(ctx.signal)

    if ctx.result.isOk:
      records.add(ctx.result.get)
    # Skip keys that don't exist (KVStoreKeyNotFound)

  return success records

method put*(
    self: FSKVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  var conflicts: seq[Key]

  for record in records:
    let p = ?self.path(record.key)

    # Acquire per-key lock BEFORE spawning task
    let lock = await self.acquire(record.key)
    defer: self.release(record.key, lock)

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[bool](signal: signal)
    defer: discard ctx.signal.close()

    self.tp.spawn runPutTask(addr ctx, p, record)
    ?await awaitSignal(ctx.signal)

    let ok = ?ctx.result
    if not ok:
      conflicts.add(record.key)

  return success conflicts

method delete*(
    self: FSKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  var skipped: seq[Key]

  for record in records:
    let p = ?self.path(record.key)

    # Acquire per-key lock BEFORE spawning task
    let lock = await self.acquire(record.key)
    defer: self.release(record.key, lock)

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[bool](signal: signal)
    defer: discard ctx.signal.close()

    self.tp.spawn runDeleteTask(addr ctx, p, record)
    ?await awaitSignal(ctx.signal)

    let ok = ?ctx.result
    if not ok:
      skipped.add(record.key)

  return success skipped

method close*(self: FSKVStore): Future[?!void] {.async: (raises: [CancelledError]).} =
  return success()

# =============================================================================
# Query Iterator
# =============================================================================

proc dirWalker(path: string): (iterator (): string {.raises: [Defect], gcsafe.}) =
  return
    iterator (): string =
      try:
        for p in path.walkDirRec(yieldFilter = {pcFile}, relative = true):
          yield p
      except CatchableError as exc:
        raise newException(Defect, exc.msg)

method query*(
    self: FSKVStore, query: Query
): Future[?!QueryIterRaw] {.async: (raises: [CancelledError]).} =
  let p = ?self.path(query.key)

  let basePath =
    # if there is a file in the directory
    # with the same name then list the contents
    # of the directory, otherwise recurse
    # into subdirectories
    if p.fileExists:
      p.parentDir
    else:
      p.changeFileExt("")

  var state = FsQueryIterState(
    basePath: basePath,
    walker: dirWalker(basePath),
    root: self.root,
    queryKey: query.key,
    queryValue: query.value,
    tp: self.tp
  )
  state.finished.store(false)
  state.isDisposed.store(false)

  proc next(): Future[?!(?RawRecord)] {.async: (raises: [CancelledError]).} =
    if state.finished.load():
      return success(RawRecord.none)

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[?RawRecord](signal: signal)
    defer: discard ctx.signal.close()

    state.tp.spawn runNextTask(addr ctx, addr state)

    let taskFut = ctx.signal.wait()
    if err =? catch(await taskFut.join()).errorOption:
      state.finished.store(true)
      ?catch(await noCancel taskFut)
      if err of CancelledError:
        raise (ref CancelledError)(err)
      return failure(err)

    return ctx.result

  proc isFinished(): bool =
    state.finished.load()

  proc dispose() =
    if state.isDisposed.exchange(true):
      return
    state.finished.store(true)

  return success QueryIter.new(next, isFinished, dispose)

# =============================================================================
# Constructor
# =============================================================================

proc new*(
    T: type FSKVStore,
    root: string,
    tp: Taskpool,
    depth = 2,
    caseSensitive = true,
    ignoreProtected = false,
): ?!T =
  let root =
    ?(
      block:
        if root.isAbsolute: root
        else:
          getCurrentDir() / root
    ).catch

  if not dirExists(root):
    return failure "directory does not exist: " & root

  success T(
    root: root,
    ignoreProtected: ignoreProtected,
    depth: depth,
    locks: newTable[Key, AsyncLock](),
    tp: tp,
  )
