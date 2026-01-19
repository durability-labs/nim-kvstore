{.push raises: [].}

when not compileOption("threads"):
  {.error: "SQLiteKVStore requires --threads:on".}

import std/os
import std/sets
import std/options
import std/atomics
import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/taskpools

import ../key
import ../query
import ../kvstore
import ../taskutils
import ./sqlitedsdb
import ./sqliteutils
import ./operations

export sqlitedsdb
export operations
export taskutils

type
  SQLiteKVStore* = ref object of KVStore
    readOnly: bool
    db: SQLiteDsDb
    lock: Lock                      # Serializes access to shared prepared statements
    tp: Taskpool                    # Injected threadpool for async operations
    tasks: HashSet[Future[?!void]]  # Track outstanding tasks for close()
    closed: bool

  # Per-iterator state for query operations
  # Each iterator has its own lock (not the store-wide lock) since each
  # has a private prepared statement. The lock only protects against
  # concurrent next() calls on the same iterator instance.
  QueryIterState* = ref object
    stmt*: RawStmtPtr
    lock*: Lock
    finished*: Atomic[bool]
    isDisposed*: bool
    tp*: Taskpool                        # For spawning next() workers
    iterTasks*: HashSet[Future[void].Raising([AsyncError, CancelledError])]  # Track outstanding iterator tasks
    queryValue*: bool                    # Whether to include value in results

proc path*(self: SQLiteKVStore): string =
  self.db.dbPath

proc `readOnly=`*(
  self: SQLiteKVStore
): bool {.error: "readOnly should not be assigned".}

# =============================================================================
# Task Workers (for threadpool - top-level procs)
# =============================================================================

proc runHasTask(ctx: ptr TaskCtx[bool], db: ptr SQLiteDsDb,
                lock: ptr Lock, keyId: string) {.gcsafe.} =
  ## Task worker for has() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = hasSync(db[], keyId)

proc runGetTask(ctx: ptr TaskCtx[RawRecord], db: ptr SQLiteDsDb,
                lock: ptr Lock, key: Key) {.gcsafe.} =
  ## Task worker for get() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = getSync(db[], key)

proc runGetManyTask(ctx: ptr TaskCtx[seq[RawRecord]], db: ptr SQLiteDsDb,
                    lock: ptr Lock, keys: seq[Key]) {.gcsafe.} =
  ## Task worker for get(keys) operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = getManySync(db[], keys)

proc runPutTask(ctx: ptr TaskCtx[seq[Key]], db: ptr SQLiteDsDb,
                lock: ptr Lock, records: seq[RawRecord], readOnly: bool) {.gcsafe.} =
  ## Task worker for put() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = putSync(db[], records, readOnly)

proc runDeleteTask(ctx: ptr TaskCtx[seq[Key]], db: ptr SQLiteDsDb,
                   lock: ptr Lock, records: seq[KeyRecord], readOnly: bool) {.gcsafe.} =
  ## Task worker for delete() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = deleteSync(db[], records, readOnly)

proc runPutAtomicTask(ctx: ptr TaskCtx[seq[Key]], db: ptr SQLiteDsDb,
                      lock: ptr Lock, records: seq[RawRecord], readOnly: bool) {.gcsafe.} =
  ## Task worker for putAtomic() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = putAtomicSync(db[], records, readOnly)

proc runDeleteAtomicTask(ctx: ptr TaskCtx[seq[Key]], db: ptr SQLiteDsDb,
                         lock: ptr Lock, records: seq[KeyRecord], readOnly: bool) {.gcsafe.} =
  ## Task worker for deleteAtomic() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = deleteAtomicSync(db[], records, readOnly)

proc runNextTask(ctx: ptr TaskCtx[?RawRecord],
                 stmt: ptr RawStmtPtr,
                 lock: ptr Lock,
                 finished: ptr Atomic[bool],
                 queryValue: bool) {.gcsafe.} =
  ## Task worker for query iterator next() operation.
  ## Uses per-iterator lock, not store-wide lock.
  defer:
    discard ctx[].signal.fireSync()

  # Type alias for explicit Result construction (needed inside withLock)
  type R = ?!(?RawRecord)

  # Check finished atomically before acquiring lock
  # Return none instead of error - it's valid to call next() after finishing
  if finished[].load():
    ctx[].result = R.ok(RawRecord.none)
    return

  withLock(lock[]):
    # Double-check after acquiring lock
    if finished[].load():
      ctx[].result = R.ok(RawRecord.none)
      return

    let res = nextSync(stmt[], queryValue)
    if res.isErr:
      finished[].store(true)
      ctx[].result = R.err(res.error)
      return

    let recordOpt = res.get
    if recordOpt.isNone:
      finished[].store(true)
    ctx[].result = R.ok(recordOpt)

# =============================================================================
# Async Methods (public API)
# =============================================================================

method has*(
    self: SQLiteKVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let signal = ThreadSignalPtr.new().valueOr:
    return failure(newException(KVStoreError, error))
  var ctx = TaskCtx[bool](
    lock: addr self.lock,
    signal: signal
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runHasTask(addr ctx, addr self.db, addr self.lock, key.id)
  let fut = awaitSignal(ctx.signal)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  return ctx.result

method get*(
    self: SQLiteKVStore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if keys.len == 0:
    return success(newSeq[RawRecord]())

  if keys.len == 1:
    # Optimize single-key get to avoid extra allocation
    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[RawRecord](
      lock: addr self.lock,
      signal: signal
    )
    defer: discard ctx.signal.close()

    self.tp.spawn runGetTask(addr ctx, addr self.db, addr self.lock, keys[0])
    let fut = awaitSignal(ctx.signal)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    return success @[?ctx.result]
  else:
    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[seq[RawRecord]](
      lock: addr self.lock,
      signal: signal
    )
    defer: discard ctx.signal.close()

    self.tp.spawn runGetManyTask(addr ctx, addr self.db, addr self.lock, keys)
    let fut = awaitSignal(ctx.signal)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    return ctx.result

method put*(
    self: SQLiteKVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let signal = ThreadSignalPtr.new().valueOr:
    return failure(newException(KVStoreError, error))
  var ctx = TaskCtx[seq[Key]](
    lock: addr self.lock,
    signal: signal
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runPutTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  let fut = awaitSignal(ctx.signal)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  return ctx.result

method delete*(
    self: SQLiteKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if records.len == 0:
    return success(newSeq[Key]())

  let signal = ThreadSignalPtr.new().valueOr:
    return failure(newException(KVStoreError, error))
  var ctx = TaskCtx[seq[Key]](
    lock: addr self.lock,
    signal: signal
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runDeleteTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  let fut = awaitSignal(ctx.signal)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  return ctx.result

# =============================================================================
# Atomic Batch API Implementation
# =============================================================================

method supportsAtomicBatch*(self: SQLiteKVStore): bool =
  true

method putAtomic*(
    self: SQLiteKVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## All-or-nothing batch put with CAS.
  ##
  ## If ANY record has a CAS conflict, NO records are committed.
  ## Returns conflict keys on rollback, empty seq on success.

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if records.len == 0:
    return success(newSeq[Key]())

  let signal = ThreadSignalPtr.new().valueOr:
    return failure(newException(KVStoreError, error))
  var ctx = TaskCtx[seq[Key]](
    lock: addr self.lock,
    signal: signal
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runPutAtomicTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  let fut = awaitSignal(ctx.signal)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  return ctx.result

method deleteAtomic*(
    self: SQLiteKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## All-or-nothing batch delete with CAS.
  ## Same semantics as putAtomic().

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if records.len == 0:
    return success(newSeq[Key]())

  let signal = ThreadSignalPtr.new().valueOr:
    return failure(newException(KVStoreError, error))
  var ctx = TaskCtx[seq[Key]](
    lock: addr self.lock,
    signal: signal
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runDeleteAtomicTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  let fut = awaitSignal(ctx.signal)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  return ctx.result

method close*(
    self: SQLiteKVStore
): Future[?!void] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return success()

  self.closed = true

  defer:
    deinitLock(self.lock) # don't leak resources

  # wait for all outstanding tasks to finish
  await noCancel allFutures(self.tasks.toSeq())

  return self.db.close()

method query*(
    self: SQLiteKVStore, query: Query
): Future[?!QueryIterRaw] {.async: (raises: [CancelledError]).} =
  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  # Prepare private statement (no store lock needed - SQLite FULLMUTEX handles it)
  let s = ?prepareQueryStmt(self.db.env, query)

  # Create per-iterator state using module-level type
  var state = QueryIterState(
    stmt: s,
    tp: self.tp,
    queryValue: query.value
  )
  state.finished.store(false)
  state.isDisposed = false
  initLock(state.lock)

  let asyncLock = newAsyncLock()
  proc next(): Future[?!(?RawRecord)] {.async: (raises: [CancelledError]).} =
    if self.closed or state.isDisposed or state.finished.load():
      return failure newException(KVStoreError, "SQLiteKVStore is closed or iterator disposed/finished")

    # AsyncLock serializes next() calls to ensure results are returned in order.
    # This is critical for sort order queries - without serialization, workers
    # race for the cursor lock and results come back in arbitrary order.
    await asyncLock.acquire()
    defer:
      if asyncLock.locked:
        if err =? catch(asyncLock.release()).errorOption:
          state.finished.store(true)
          return failure(err)

    # Early check with atomic load (fast path for finished iterator)
    if state.finished.load():
      return success(RawRecord.none)

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[?RawRecord](
      lock: addr state.lock,
      signal: signal
    )
    defer: discard ctx.signal.close()

    state.tp.spawn runNextTask(addr ctx, addr state.stmt, addr state.lock,
                               addr state.finished, state.queryValue)

    # Wait for result - on cancellation, mark finished first then wait for worker
    let taskFut = ctx.signal.wait()

    state.iterTasks.incl(taskFut)
    defer:
      state.iterTasks.excl(taskFut)

    if err =? catch(await taskFut.join()).errorOption:
      # Mark finished to stop further iteration, then wait for worker to complete
      state.finished.store(true)
      ?catch(await noCancel taskFut)
      if err of CancelledError:
        raise (ref CancelledError)(err)
      return failure(err)

    return ctx.result

  proc isFinished(): bool =
    state.finished.load()

  proc dispose(): Future[?!void] {.async: (raises: [CancelledError]).} =
    # Signal workers to stop accepting new work
    state.finished.store(true)

    await asyncLock.acquire()
    defer:
      if asyncLock.locked:
        if err =? catch(asyncLock.release()).errorOption:
          state.finished.store(true)
          return failure(err)

    # Atomic check-and-set to prevent double-dispose
    if state.isDisposed:
      return success()
    state.isDisposed = true

    defer:
      # don't leak resources
      discard disposeStmtSync(state.stmt)
      deinitLock(state.lock)

    # wait for iter tasks to finish
    await noCancel allFutures(state.iterTasks.toSeq())

    return success()

  return success QueryIter.new(next, isFinished, dispose)

proc new*(T: type SQLiteKVStore, path: string, tp: Taskpool, readOnly = false): ?!T =
  ## Create a new SQLiteKVStore.
  ##
  ## Parameters:
  ##   - path: Database file path, or SqliteMemory for in-memory
  ##   - tp: Taskpool for async operations (required)
  ##   - readOnly: Open in read-only mode
  let flags =
    if readOnly:
      SQLITE_OPEN_READONLY
    else:
      SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  var store = T(db: ?SQLiteDsDb.open(path, flags), readOnly: readOnly, tp: tp)
  initLock(store.lock)
  success store

proc new*(T: type SQLiteKVStore, db: SQLiteDsDb, tp: Taskpool): ?!T =
  ## Create a new SQLiteKVStore from an existing database handle.
  ##
  ## Parameters:
  ##   - db: Pre-opened SQLiteDsDb
  ##   - tp: Taskpool for async operations (required)
  var store = T(db: db, readOnly: db.readOnly, tp: tp)
  initLock(store.lock)
  success store
