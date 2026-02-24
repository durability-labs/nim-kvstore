{.push raises: [].}

when not compileOption("threads"):
  {.error: "FSKVStore requires --threads:on".}

import std/os
import std/strutils

import std/algorithm
import std/sets
import std/sequtils

import pkg/chronicles
import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/stew/io2
import pkg/taskpools

import ../key
import ../query
import ../kvstore
import ../taskutils
import ./locks
import ./metrics
import ./operations

export locks, operations

type
  FSKVStore* = ref object of KVStore
    ## Filesystem-backed kvstore that stores records as files.
    ##
    ## Tokens are stored as uint64 in little-endian format. Unlike SQLiteKVStore
    ## which is limited to int64 range due to SQLite INTEGER type, FSKVStore
    ## supports the full uint64 range.
    root*: string
    depth: int
    locks: LockTable
    tasks: HashSet[Future[?!void]]
    disposeHandles: HashSet[Future[?!void]] # Track dispose calls (wait, don't cancel)
    tp: Taskpool
    writeConfig*: FsWriteConfig
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
    iterTaskHandle: Future[?!void]
    lock: AsyncLock
    tp: Taskpool
    signal: ThreadSignalPtr

proc validDepth*(self: FSKVStore, key: Key): bool =
  key.len <= self.depth

proc isRootSubdir*(self: FSKVStore, path: string): bool =
  path.startsWith(self.root)

proc path*(self: FSKVStore, key: Key): ?!string {.raises: [].} =
  ## Return filename corresponding to the key
  ## or failure if the key doesn't correspond to a valid filename
  ##

  if not self.validDepth(key):
    return failure newException(KVStoreBackendError, "Path has invalid depth!")

  var segments: seq[string]
  for ns in key:
    let basename = ns.value.extractFilename
    if basename == "" or not basename.isValidFilename:
      return
        failure newException(KVStoreBackendError, "Filename contains invalid chars!")

    if ns.field == "":
      segments.add(ns.value)
    else:
      let basename = ns.field.extractFilename
      if basename == "" or not basename.isValidFilename:
        return
          failure newException(KVStoreBackendError, "Filename contains invalid chars!")

      # `:` are replaced with `/`
      segments.add(ns.field / ns.value)

  let fullname = (self.root / segments.joinPath()).addFileExt(FileExt)

  if not self.isRootSubdir(fullname):
    return
      failure newException(KVStoreBackendError, "Path is outside of `root` directory!")

  return success fullname

# =============================================================================
# Async Methods (public API)
# =============================================================================

method hasImpl*(
    self: FSKVStore, keys: seq[Key]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## Check existence of multiple keys.
  ## Returns the subset of input keys that exist in the store, in input order.
  ##

  # XXX: don't move after closed, every await introduces concurrency
  await checkFairness()

  if self.closed:
    return failure newException(KVStoreError, "FSKVStore is closed")

  writeHasMetrics(keys)

  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for has")

  let ctx = newSharedPtr(TaskCtx[seq[string]](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in has", error = err
    # SharedPtr handles TaskCtx cleanup automatically

  let
    keys = keys.deduplicate()
    taskFut = signal.wait()
  self.tp.spawn runHasTaskMany(ctx, keys.mapIt(?self.path(it)))

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut
  let hasPaths = (?extract(ctx[].result)).toHashSet

  success keys.filterIt(?self.path(it) in hasPaths)

method getImpl*(
    self: FSKVStore, keys: seq[Key]
): Future[?!seq[RawKVRecord]] {.async: (raises: [CancelledError]).} =
  # XXX: don't move after closed, every await introduces concurrency
  await checkFairness()

  if self.closed:
    return failure newException(KVStoreError, "FSKVStore is closed")

  writeGetMetrics(keys)

  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for get")

  let ctx = newSharedPtr(TaskCtx[seq[RawKVRecord]](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in get", error = err
    # SharedPtr handles TaskCtx cleanup automatically

  let taskFut = signal.wait()
  self.tp.spawn runGetTaskMany(ctx, keys.deduplicate().mapIt((?self.path(it), it)))

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  let records = ?extract(ctx[].result)

  when defined(kvstore_expensive_metrics):
    kvstore_fs_get_value_bytes.observe(records.mapIt(it.val.len.float64))

  return success records

method putImpl*(
    self: FSKVStore, records: seq[RawKVRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure newException(KVStoreError, "FSKVStore is closed")

  writePutMetrics(records)

  await checkFairness()

  if self.closed:
    return failure(newException(KVStoreError, "FSKVStore is closed"))

  # Single pass: deduplicate by key and build (path, record) pairs.
  # This avoids records.deduplicate (which deep-copies all payloads and
  # compares seq[byte] with ==) and the separate mapIt (another deep copy).
  var
    seen: HashSet[Key]
    sortedKeys: seq[Key]
    pairs = newSeqOfCap[(string, RawKVRecord)](records.len)

  for i in 0 ..< records.len:
    if records[i].key notin seen:
      seen.incl(records[i].key)
      sortedKeys.add(records[i].key)
      pairs.add((?self.path(records[i].key), records[i]))

  sortedKeys.sort(
    proc(a, b: Key): int =
      cmp(a.id, b.id)
  )

  # Acquire per-key locks in sorted order to prevent deadlocks
  var heldLocks: seq[(Key, RefCountedLock)]
  defer:
    for (key, rcLock) in heldLocks:
      self.locks.release(key, rcLock)

  for key in sortedKeys:
    heldLocks.add((key, await self.locks.acquire(key)))

  # Re-check after lock acquisition awaits
  if self.closed:
    return failure(newException(KVStoreError, "FSKVStore is closed"))

  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for put")

  let ctx = newSharedPtr(TaskCtx[seq[Key]](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in put", error = err

  let taskFut = signal.wait()
  self.tp.spawn runPutTaskMany(ctx, pairs, self.writeConfig)

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  return success ?extract(ctx[].result)

method deleteImpl*(
    self: FSKVStore, records: seq[KeyKVRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  writeDeleteMetrics(records)

  await checkFairness()

  if self.closed:
    return failure(newException(KVStoreError, "FSKVStore is closed"))

  # Acquire per-key locks in sorted order to prevent deadlocks
  let sortedKeys = records.mapIt(it.key).deduplicate().sortedByIt(it.id)
  var heldLocks: seq[(Key, RefCountedLock)]
  defer:
    for (key, rcLock) in heldLocks:
      self.locks.release(key, rcLock)

  for key in sortedKeys:
    heldLocks.add((key, await self.locks.acquire(key)))

  # Re-check after lock acquisition awaits
  if self.closed:
    return failure(newException(KVStoreError, "FSKVStore is closed"))

  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for delete")

  let ctx = newSharedPtr(TaskCtx[seq[Key]](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in delete", error = err

  let taskFut = signal.wait()
  self.tp.spawn runDeleteTaskMany(
    ctx, records.mapIt((?self.path(it.key), it)), self.writeConfig
  )

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  return success ?extract(ctx[].result)

method closeImpl*(self: FSKVStore): Future[?!void] {.async: (raises: []).} =
  if self.closed:
    return success()

  self.closed = true
  let tasks = (self.tasks).toSeq().mapIt(it.cancelAndWait())

  # Cancel all active tasks
  await noCancel allFutures(tasks)

  # Wait for dispose calls to finish (don't cancel them)
  await noCancel allFutures(self.disposeHandles.toSeq())

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

method queryImpl*(
    self: FSKVStore, query: Query
): Future[?!QueryIterRaw] {.async: (raises: [CancelledError]).} =
  kvstore_fs_query_total.inc()
  let startTime = Moment.now()

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

  # Found a valid path - spawn worker to read the file
  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for query")

  var state = FsQueryIterState(
    basePath: basePath,
    walker: dirWalker(basePath),
    root: self.root,
    queryKey: query.key,
    queryValue: query.value,
    tp: self.tp,
    signal: signal,
    lock: newAsyncLock(),
  )
  state.finished = false
  state.isDisposed = false

  proc next(): Future[?!(?RawKVRecord)] {.async: (raises: [CancelledError]).} =
    # don't move to after close/disposed/finished checks due to concurrency
    await checkFairness()

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
      return success(RawKVRecord.none)

    # Step the walker on the async thread (single-threaded, no races).
    # This loop finds the next valid path before spawning a worker.
    while not state.finished:
      let relPath = state.walker()

      if finished(state.walker):
        state.finished = true
        return success(RawKVRecord.none)

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

      let absPath = state.basePath / relPath

      let ctx = newSharedPtr(TaskCtx[RawKVRecord](signal: state.signal))

      let taskFut = signal.wait()
      state.tp.spawn runReadRecordTask(ctx, absPath, key, state.queryValue)
      let fut = awaitSignal(
        taskFut,
        onError = proc() {.async: (raises: [CancelledError]).} =
          state.finished = true,
      )

      # disposer task handle for graceful dispose
      state.iterTaskHandle = fut
      self.tasks.incl(fut)
      defer:
        self.tasks.excl(fut)
        state.iterTaskHandle = nil

      ?await fut

      let record = ?extract(ctx[].result)
      return success(record.some)

  proc isFinished(): bool =
    state.finished

  proc isDisposed(): bool =
    state.isDisposed

  proc disposeImpl(): Future[?!void] {.async: (raises: []).} =
    try:
      # Cancel iter task before acquiring lock (so next() can release it)
      if not state.iterTaskHandle.isNil:
        await noCancel state.iterTaskHandle.cancelAndWait()
    finally:
      if err =? state.signal.close().errorOption:
        warn "signal.close failed in query next", error = err
        # SharedPtr handles TaskCtx cleanup automatically

      if state.lock.locked:
        if err =? catch(state.lock.release()).errorOption:
          state.finished = true
          warn "Unable to release state lock", error = err

      kvstore_fs_active_iterators.dec()

    return success()

  var handle: Future[?!void].Raising([])
  proc dispose(): Future[?!void] {.async: (raises: []), gcsafe.} =
    state.finished = true

    await noCancel state.lock.acquire()
    defer:
      if state.lock.locked:
        if err =? catch(state.lock.release()).errorOption:
          return failure(err)

    # Lock serializes dispose calls - if already disposed, first dispose completed
    if state.isDisposed:
      return ?catch(await noCancel handle)

    state.isDisposed = true
    handle = disposeImpl()

    # Register with store so close() waits for us
    self.disposeHandles.incl(handle)
    defer:
      self.disposeHandles.excl(handle)

    return ?catch(await noCancel handle)

  kvstore_fs_query_duration_seconds.observe(
    (Moment.now() - startTime).nanos.float64 / 1_000_000_000.0
  )
  kvstore_fs_active_iterators.inc()
  return success QueryIter.new(next, isFinished, isDisposed, dispose)

# =============================================================================
# Constructor
# =============================================================================

proc new*(
    _: type FSKVStore,
    root: string,
    tp: Taskpool,
    depth = 2,
    directIO = false,
    fsyncFile = true,
    fsyncDir = true,
): ?!FSKVStore =
  let root =
    ?(
      block:
        if root.isAbsolute: root
        else:
          os.getCurrentDir() / root
    ).catch

  if not isDir(root):
    return failure "directory does not exist: " & root

  success FSKVStore(
    root: root,
    depth: depth,
    locks: newLockTable(),
    tp: tp,
    writeConfig:
      FsWriteConfig(directIO: directIO, fsyncFile: fsyncFile, fsyncDir: fsyncDir),
  )
