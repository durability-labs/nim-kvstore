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
import std/sets
import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/stew/endians2
import pkg/stew/io2
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
  RefCountedLock* = ref object
    ## AsyncLock with reference counting to safely track waiters.
    ## Only removed from table when refcount reaches zero.
    lock*: AsyncLock
    refCount*: int

  FSKVStore* = ref object of KVStore
    ## Filesystem-backed kvstore that stores records as files.
    ##
    ## Tokens are stored as uint64 in little-endian format. Unlike SQLiteKVStore
    ## which is limited to int64 range due to SQLite INTEGER type, FSKVStore
    ## supports the full uint64 range.
    root*: string
    ignoreProtected: bool
    depth: int
    locks: Table[Key, RefCountedLock]
    tasks: HashSet[Future[?!void]]
    iteratorDisposers: HashSet[IterDispose] # Track active iterators for close()
    tp: Taskpool
    closed: bool

  # Per-iterator state for query operations.
  # The walker is stepped on the async thread (single-threaded, no races).
  # Only file I/O is offloaded to the threadpool.
  FsQueryIterState = ref object
    basePath: string
    walker: iterator (): string {.raises: [Defect], gcsafe.}
    root: string
    queryKey: Key
    queryValue: bool
    finished: bool
    isDisposed: bool
    iterTasks: HashSet[Future[void].Raising([AsyncError, CancelledError])]
    lock: AsyncLock
    tp: Taskpool

randomize() # TODO: We should probably use a stronger rng here?

proc moveFile(src, dst: string): ?!void {.gcsafe.} =
  try:
    os.moveFile(src, dst)
    success()
  except Exception as exc:
    failure newBackendError("unable to move '" & src & "' to '" & dst & "': " & exc.msg)

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

proc validDepth*(self: FSKVStore, key: Key): bool =
  key.len <= self.depth

proc isRootSubdir*(self: FSKVStore, path: string): bool =
  path.startsWith(self.root)

proc path*(self: FSKVStore, key: Key): ?!string {.raises: [].} =
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
): ?!RawRecord {.gcsafe, raises: [].} =
  if not isFile(path):
    return failure newException(KVStoreKeyNotFound, "file does not exist: " & path)

  let handle = openFile(path, {OpenFlags.Read}).valueOr:
    return failure newException(KVStoreBackendError, "unable to open file: " & path)

  defer:
    discard closeFile(handle)

  let size = getFileSize(handle).valueOr:
    return failure newBackendError("unable to get file size: " & path)

  if size < TokenBytes:
    return failure newCorruptionError("File too small for record: " & path)

  var header: array[TokenBytes, byte]
  let headerRead = readFile(handle, header).valueOr:
    return failure newBackendError("unable to read token header")

  if headerRead != TokenBytes.uint:
    return failure newBackendError("unable to read token header")

  let token = uint64.fromBytesLE(header)
  let payloadLen = size - TokenBytes

  let value =
    if data:
      var value = newSeq[byte](payloadLen)
      if payloadLen > 0:
        let payloadRead = readFile(handle, value).valueOr:
          return failure newBackendError("unable to read payload")
        if payloadRead != payloadLen.uint:
          return failure newBackendError("unable to read payload")
      value
    else:
      EmptyBytes

  return success RawRecord.init(key, value, token)

proc writeVersioned*(
    path: string, token: uint64, value: seq[byte]
): ?!void {.gcsafe, raises: [].} =
  if createPath(parentDir(path)).isErr:
    return failure newBackendError("unable to create parent directory")

  let tmp = path & ".tmp-" & $epochTime() & "-" & $rand(1_000_000)

  let handle = openFile(tmp, {OpenFlags.Write, OpenFlags.Create, OpenFlags.Truncate}).valueOr:
    return failure newBackendError("unable to open temporary file '" & tmp & "'")

  defer:
    discard closeFile(handle)
    discard io2.removeFile(tmp)

  let header = token.toBytesLE()
  let headerWritten = writeFile(handle, header).valueOr:
    return failure newBackendError("Failed writing token")

  if headerWritten != TokenBytes.uint:
    return failure newBackendError("Failed writing token")

  if value.len > 0:
    let valueWritten = writeFile(handle, value).valueOr:
      return failure newBackendError("Failed writing data")
    if valueWritten != value.len.uint:
      return failure newBackendError("Failed writing data")

  if fsync(handle).isErr:
    return failure newBackendError("Failed to sync file")

  discard closeFile(handle)
  ?moveFile(tmp, path)
  syncParentDirectory(path)
  return success()

proc deleteFile(path: string): ?!void {.gcsafe, raises: [].} =
  if io2.removeFile(path).isErr:
    return failure newBackendError("unable to delete file: " & path)
  syncParentDirectory(path)
  success()

proc getSync*(path: string, key: Key): ?!RawRecord =
  return readVersioned(path, key)

proc putSync*(path: string, record: RawRecord): ?!void =
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

proc deleteSync*(path: string, record: KeyRecord): ?!void =
  if not isFile(path):
    return
      failure newException(KVConflictError, "Record does not exist: " & $record.key)

  let current = ?readVersioned(path, record.key)
  if current.token != record.token:
    return failure newException(
      KVConflictError,
      "Token mismatch for record " & $record.key & ", expected " & $record.token &
        ", got " & $current.token,
    )

  discard ?catch(deleteFile(path))
  return success()

# =============================================================================
# Task Workers (top-level procs for threadpool)
# =============================================================================

proc runHasTask(ctx: ptr TaskCtx[bool], path: string) {.gcsafe.} =
  defer:
    discard ctx[].signal.fireSync()
  ctx[].result = success(isFile(path))

proc runGetTask(ctx: ptr TaskCtx[RawRecord], path: string, key: Key) {.gcsafe.} =
  defer:
    discard ctx[].signal.fireSync()
  ctx[].result = getSync(path, key)

proc runPutTask(ctx: ptr TaskCtx[void], path: string, record: RawRecord) {.gcsafe.} =
  ## Returns true if successful, false if conflict
  defer:
    discard ctx[].signal.fireSync()
  ctx[].result = putSync(path, record)

proc runDeleteTask(ctx: ptr TaskCtx[void], path: string, record: KeyRecord) {.gcsafe.} =
  ## Returns true if successful, false if conflict/skip
  defer:
    discard ctx[].signal.fireSync()
  ctx[].result = deleteSync(path, record)

proc runReadRecordTask(
    ctx: ptr TaskCtx[RawRecord], path: string, key: Key, includeValue: bool
) {.gcsafe.} =
  ## Task worker for reading a single record from disk.
  ## Walker stepping happens on the async thread; this only does file I/O.
  defer:
    discard ctx[].signal.fireSync()
  ctx[].result = readVersioned(path, key, includeValue)

# =============================================================================
# Per-Key Locking with Reference Counting
# =============================================================================

proc acquire*(
    self: FSKVStore, key: Key
): Future[RefCountedLock] {.async: (raises: [CancelledError]).} =
  ## Acquire a per-key lock. Refcount is incremented BEFORE await to track waiters.
  ## This prevents the race where a lock is deleted while tasks are waiting on it.
  var rcLock =
    self.locks.mgetOrPut(key, RefCountedLock(lock: newAsyncLock(), refCount: 0))
  rcLock.refCount += 1 # Increment BEFORE await to count waiters
  try:
    await rcLock.lock.acquire()
  except CancelledError as exc:
    # If cancelled while waiting, decrement refcount and maybe cleanup
    rcLock.refCount -= 1
    if rcLock.refCount == 0:
      self.locks.del(key)
    raise exc
  rcLock

proc release*(self: FSKVStore, key: Key, rcLock: RefCountedLock) {.raises: [].} =
  ## Release a per-key lock. Only removes from table when refcount reaches zero.
  if rcLock.lock.locked:
    try:
      rcLock.lock.release()
    except CatchableError as err:
      raiseAssert(err.msg) # shouldn't happen

  rcLock.refCount -= 1
  if rcLock.refCount == 0:
    self.locks.del(key)

# =============================================================================
# Async Methods (public API)
# =============================================================================

method has*(
    self: FSKVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure newException(KVStoreError, "FSKVStore is closed")

  let p = ?self.path(key)

  let signal = ThreadSignalPtr.new().valueOr:
    return failure(newException(KVStoreError, error))
  var ctx = TaskCtx[bool](signal: signal)
  defer:
    discard ctx.signal.close()

  self.tp.spawn runHasTask(addr ctx, p)
  let fut = awaitSignal(ctx.signal)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  return ctx.result

method get*(
    self: FSKVStore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure newException(KVStoreError, "FSKVStore is closed")

  var records: seq[RawRecord]

  for key in keys:
    let p = ?self.path(key)

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[RawRecord](signal: signal)
    defer:
      discard ctx.signal.close()

    self.tp.spawn runGetTask(addr ctx, p, key)
    let fut = awaitSignal(ctx.signal)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    if ctx.result.isOk:
      records.add(ctx.result.get)
    # Skip keys that don't exist (KVStoreKeyNotFound)

  return success records

method put*(
    self: FSKVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure newException(KVStoreError, "FSKVStore is closed")

  var conflicts: seq[Key]

  for record in records:
    let p = ?self.path(record.key)

    # Acquire per-key lock BEFORE spawning task
    let lock = await self.acquire(record.key)
    defer:
      self.release(record.key, lock)

    # Re-check after await - close() may have started during acquire
    if self.closed:
      return failure(newException(KVStoreError, "FSKVStore is closed"))

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[void](signal: signal)
    defer:
      discard ctx.signal.close()

    self.tp.spawn runPutTask(addr ctx, p, record)
    let fut = awaitSignal(ctx.signal)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    if ctx.result.isErr and ctx.result.error of KVConflictError:
      conflicts.add(record.key)
    else:
      ?ctx.result

  return success conflicts

method delete*(
    self: FSKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure newException(KVStoreError, "FSKVStore is closed")

  var skipped: seq[Key]

  for record in records:
    let p = ?self.path(record.key)

    # Acquire per-key lock BEFORE spawning task
    let lock = await self.acquire(record.key)
    defer:
      self.release(record.key, lock)

    # Re-check after await - close() may have started during acquire
    if self.closed:
      return failure(newException(KVStoreError, "FSKVStore is closed"))

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[void](signal: signal)
    defer:
      discard ctx.signal.close()

    self.tp.spawn runDeleteTask(addr ctx, p, record)
    let fut = awaitSignal(ctx.signal)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    if ctx.result.isErr and ctx.result.error of KVConflictError:
      skipped.add(record.key)
    else:
      ?ctx.result

  return success skipped

method close*(self: FSKVStore): Future[?!void] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return success()

  self.closed = true

  let
    disposers = (self.iteratorDisposers).toSeq().mapIt(it())
    tasks = self.tasks.toSeq()

  await noCancel allFutures(
    @[
      # Dispose all active iterators first (copy set since dispose modifies it)
      noCancel allFutures(disposers),
      # Dispose all active tasks
      noCancel allFutures(tasks),
    ]
  )

  let
    dispErrors = disposers.filterIt(catch(it.read).flatten().isErr).mapIt(
        catch(it.read).flatten().error.msg
      )

    taskErrors = tasks.filterIt(catch(it.read).flatten().isErr).mapIt(
        catch(it.read).flatten().error.msg
      )

  if dispErrors.len > 0 or taskErrors.len > 0:
    var msg = "Errors occurred during FSKVStore close()"
    if dispErrors.len > 0:
      msg &= "\nDisposer errors:\n  - " & dispErrors.join("\n  - ")
    if taskErrors.len > 0:
      msg &= "\nTask errors:\n  - " & taskErrors.join("\n  - ")
    return failure(newException(KVStoreBackendError, msg))

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
  if self.closed:
    return failure newException(KVStoreError, "FSKVStore is closed")

  let p = ?self.path(query.key)

  let basePath =
    # if there is a file in the directory
    # with the same name then list the contents
    # of the directory, otherwise recurse
    # into subdirectories
    if isFile(p):
      p.parentDir
    else:
      p.changeFileExt("")

  var state = FsQueryIterState(
    basePath: basePath,
    walker: dirWalker(basePath),
    root: self.root,
    queryKey: query.key,
    queryValue: query.value,
    tp: self.tp,
    lock: newAsyncLock(),
  )
  state.finished = false
  state.isDisposed = false

  proc next(): Future[?!(?RawRecord)] {.async: (raises: [CancelledError]).} =
    if self.closed or state.isDisposed or state.finished:
      return failure newException(
        KVStoreError, "FSKVStore is closed or iterator disposed/finished"
      )

    await state.lock.acquire()
    defer:
      if state.lock.locked:
        if err =? catch(state.lock.release()).errorOption:
          state.finished = true
          return failure(err)

    # Re-check after await - close/dispose may have run
    if self.closed or state.isDisposed:
      return
        failure newException(KVStoreError, "FSKVStore is closed or iterator disposed")

    if state.finished:
      return success(RawRecord.none)

    # Step the walker on the async thread (single-threaded, no races).
    # This loop finds the next valid path before spawning a worker.
    while not state.finished:
      let relPath = state.walker()

      if finished(state.walker):
        state.finished = true
        return success(RawRecord.none)

      var keyPath = state.basePath
      keyPath.removePrefix(state.root)
      keyPath = keyPath / relPath.changeFileExt("")
      keyPath = keyPath.replace("\\", "/")

      let keyRes = Key.init(keyPath)
      if keyRes.isErr:
        continue

      let key = keyRes.get
      if key != state.queryKey and not state.queryKey.ancestor(key):
        continue

      let absPathRes = catch((state.basePath / relPath).absolutePath())
      if absPathRes.isErr:
        return failure(absPathRes.error)

      let absPath = absPathRes.get

      # Found a valid path - spawn worker to read the file
      let signal = ThreadSignalPtr.new().valueOr:
        return failure(newException(KVStoreError, error))
      var ctx = TaskCtx[RawRecord](signal: signal)
      defer:
        discard ctx.signal.close()

      state.tp.spawn runReadRecordTask(addr ctx, absPath, key, state.queryValue)

      # Wait for result - on cancellation, mark finished first then wait for worker
      let taskFut = ctx.signal.wait()

      state.iterTasks.incl(taskFut)
      defer:
        state.iterTasks.excl(taskFut)

      if err =? catch(await taskFut.join()).errorOption:
        # Mark finished to stop further iteration, then wait for worker to complete
        state.finished = true
        ?catch(await noCancel taskFut)
        if err of CancelledError:
          raise (ref CancelledError)(err)
        return failure(err)

      let record = ?ctx.result
      return success(record.some)

  proc isFinished(): bool =
    state.finished

  proc isDisposed(): bool =
    state.isDisposed

  let handle = newFuture[?!void]()
  proc dispose(): Future[?!void] {.async: (raises: [CancelledError]), gcsafe.} =
    state.finished = true

    await state.lock.acquire()
    defer:
      if state.lock.locked:
        if err =? catch(state.lock.release()).errorOption:
          state.finished = true
          return failure(err)

    # Lock serializes dispose calls - if already disposed, first dispose completed
    if state.isDisposed:
      return ?catch(await handle)

    state.isDisposed = true

    defer:
      # Unregister from store
      self.iteratorDisposers.excl(dispose)

    # wait for all iter tasks to finish
    await noCancel allFutures(state.iterTasks.toSeq())
    if not handle.finished:
      handle.complete(success())

    return ?catch(await handle)

  # Register dispose proc with store for tracking
  self.iteratorDisposers.incl(dispose)

  return success QueryIter.new(next, isFinished, isDisposed, dispose)

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
          os.getCurrentDir() / root
    ).catch

  if not isDir(root):
    return failure "directory does not exist: " & root

  success T(
    root: root,
    ignoreProtected: ignoreProtected,
    depth: depth,
    locks: initTable[Key, RefCountedLock](),
    tp: tp,
  )
