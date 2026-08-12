{.push raises: [].}

when not compileOption("threads"):
  {.error: "SQLiteKVStore requires --threads:on".}

import std/os
import std/sets
import std/options
import std/atomics
import std/sequtils
import std/strutils

import pkg/chronicles
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
import ./metrics

export sqlitedsdb
export operations
export taskutils

type
  SQLiteKVStore* = ref object of KVStore
    readOnly: bool
    db: SQLiteDsDb
    lock: Lock # Serializes access to shared prepared statements
    tp: Taskpool # Injected threadpool for async operations
    tasks: HashSet[FutureBase]
      # Track outstanding tasks for close(); heterogeneous spawnJoin futures
    disposeHandles: HashSet[Future[?!void]] # Track dispose calls (wait, don't cancel)
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
    tp*: Taskpool # For spawning next() workers
    signal: ThreadSignalPtr
    iterTaskHandle*: Future[?!void] # Track outstanding iterator tasks
    queryValue*: bool # Whether to include value in results

proc path*(self: SQLiteKVStore): cstring =
  self.db.dbPath

proc `readOnly=`*(
  self: SQLiteKVStore
): bool {.error: "readOnly should not be assigned".}

# =============================================================================
# Task Workers (for threadpool - top-level procs)
# =============================================================================

proc runHasTask(
    ctx: SharedPtr[TaskCtx[bool]], db: ptr SQLiteDsDb, lock: ptr Lock, keyId: string
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runHasTask", error = res.error

  withLock(lock[]):
    var r = hasSync(db[], keyId)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runHasManyTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    keys: ptr seq[Key],
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runHasManyTask", error = res.error

  withLock(lock[]):
    var r = hasManySync(db[], keys[])
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runGetTask(
    ctx: SharedPtr[TaskCtx[?RawKVRecord]], db: ptr SQLiteDsDb, lock: ptr Lock, key: Key
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runGetTask", error = res.error

  withLock(lock[]):
    let r = getSync(db[], key)
    var res: ThreadSpawnRes[?RawKVRecord]
    if err =? r.errorOption:
      if err of KVStoreKeyNotFound:
        res = ThreadSpawnRes[?RawKVRecord].ok(RawKVRecord.none)
      else:
        res = ThreadSpawnRes[?RawKVRecord].err(err.msg)
    else:
      res = ThreadSpawnRes[?RawKVRecord].ok(some(r.value))
    ctx[].result = unsafeIsolate(move res)

proc runGetManyTask(
    ctx: SharedPtr[TaskCtx[seq[RawKVRecord]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    keys: ptr seq[Key],
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runGetManyTask", error = res.error

  withLock(lock[]):
    var r = getManySync(db[], keys[])
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runPutTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    records: ptr seq[RawKVRecord],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runPutTask", error = res.error

  withLock(lock[]):
    var r = putSync(db[], records[], readOnly)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runDeleteTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    records: ptr seq[KeyKVRecord],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDeleteTask", error = res.error

  withLock(lock[]):
    var r = deleteSync(db[], records[], readOnly)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runPutAtomicTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    records: ptr seq[RawKVRecord],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runPutAtomicTask", error = res.error

  withLock(lock[]):
    var r = putAtomicSync(db[], records[], readOnly)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runDeleteAtomicTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    records: ptr seq[KeyKVRecord],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDeleteAtomicTask", error = res.error

  withLock(lock[]):
    var r = deleteAtomicSync(db[], records[], readOnly)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runMoveTask(
    ctx: SharedPtr[TaskCtx[void]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    oldPrefix, newPrefix: Key,
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runMoveTask", error = res.error

  withLock(lock[]):
    var r = moveSync(db[], oldPrefix, newPrefix, readOnly)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runMoveMultiTask(
    ctx: SharedPtr[TaskCtx[void]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    moves: ptr seq[(Key, Key)],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runMoveMultiTask", error = res.error

  withLock(lock[]):
    var r = moveSyncMulti(db[], moves[], readOnly)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runDropPrefixTask(
    ctx: SharedPtr[TaskCtx[void]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    prefix: Key,
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDropPrefixTask", error = res.error

  withLock(lock[]):
    var r = dropPrefixSync(db[], prefix, readOnly)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runDropPrefixMultiTask(
    ctx: SharedPtr[TaskCtx[void]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    prefixes: ptr seq[Key],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDropPrefixMultiTask", error = res.error

  withLock(lock[]):
    var r = dropPrefixSyncMulti(db[], prefixes[], readOnly)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

proc runNextTask(
    ctx: SharedPtr[TaskCtx[?RawKVRecord]],
    stmt: ptr RawStmtPtr,
    lock: ptr Lock,
    finished: ptr Atomic[bool],
    queryValue: bool,
) {.gcsafe.} =
  ## Task worker for query iterator next() operation.
  ## Uses per-iterator lock, not store-wide lock.
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runNextTask", error = res.error

  # Check finished atomically before acquiring lock
  if finished[].load():
    var r = success(RawKVRecord.none)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))
    return

  withLock(lock[]):
    # Double-check after acquiring lock
    if finished[].load():
      var r = success(RawKVRecord.none)
      ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))
      return

    var r = nextSync(stmt[], queryValue)
    ctx[].result = unsafeIsolate(mapThreadSpawnErr(move r))

# =============================================================================
# Async Methods (public API)
# =============================================================================

method hasImpl*(
    self: SQLiteKVStore, keys: seq[Key]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## Check existence of multiple keys.
  ## Returns the subset of input keys that exist in the store.
  ## Result preserves input order; duplicates are deduplicated (first occurrence wins).

  writeSqlHasMetrics(keys)

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if keys.len == 0:
    return success(newSeq[Key]())

  if keys.len == 1:
    # Single-key optimization: use existing runHasTask
    let fut = spawnJoin[bool](
      proc(ctx: SharedPtr[TaskCtx[bool]]) {.gcsafe, raises: [].} =
        self.tp.spawn runHasTask(ctx, addr self.db, addr self.lock, keys[0].id)
    )
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    let exists = ?((await fut).toKVStoreError())
    return success(
      if exists:
        @[keys[0]]
      else:
        newSeq[Key]()
    )
  else:
    # Multi-key path
    let fut = spawnJoin[seq[Key]](
      proc(ctx: SharedPtr[TaskCtx[seq[Key]]]) {.gcsafe, raises: [].} =
        self.tp.spawn runHasManyTask(ctx, addr self.db, addr self.lock, addr keys)
    )
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    return success ?((await fut).toKVStoreError())

method getImpl*(
    self: SQLiteKVStore, keys: seq[Key]
): Future[?!seq[RawKVRecord]] {.async: (raises: [CancelledError]).} =
  writeSqlGetMetrics(keys)

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if keys.len == 0:
    return success(newSeq[RawKVRecord]())

  if keys.len == 1:
    let fut = spawnJoin[?RawKVRecord](
      proc(ctx: SharedPtr[TaskCtx[?RawKVRecord]]) {.gcsafe, raises: [].} =
        self.tp.spawn runGetTask(ctx, addr self.db, addr self.lock, keys[0])
    )
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    # Match batch get semantics: return empty seq for missing key, not error
    without rec =? (await fut).toKVStoreError(), err:
      return failure(err)
    if rec.isNone:
      return success(newSeq[RawKVRecord]())

    when defined(kvstore_expensive_metrics):
      kvstore_sql_get_value_bytes.observe(rec.get.val.len.float64)

    return success(@[rec.get])
  else:
    let fut = spawnJoin[seq[RawKVRecord]](
      proc(ctx: SharedPtr[TaskCtx[seq[RawKVRecord]]]) {.gcsafe, raises: [].} =
        self.tp.spawn runGetManyTask(ctx, addr self.db, addr self.lock, addr keys)
    )
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    let records = ?((await fut).toKVStoreError())
    when defined(kvstore_expensive_metrics):
      for record in records:
        kvstore_sql_get_value_bytes.observe(record.val.len.float64)

    return success(records)

method putImpl*(
    self: SQLiteKVStore, records: seq[RawKVRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  writeSqlPutMetrics(records)

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let fut = spawnJoin[seq[Key]](
    proc(ctx: SharedPtr[TaskCtx[seq[Key]]]) {.gcsafe, raises: [].} =
      self.tp.spawn runPutTask(
        ctx, addr self.db, addr self.lock, addr records, self.readOnly
      )
  )
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  let skipped = ?((await fut).toKVStoreError())
  if skipped.len > 0:
    kvstore_sql_put_conflict_total.inc(skipped.len.int64)
  return success(skipped)

method deleteImpl*(
    self: SQLiteKVStore, records: seq[KeyKVRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  writeSqlDeleteMetrics(records)

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if records.len == 0:
    return success(newSeq[Key]())

  let fut = spawnJoin[seq[Key]](
    proc(ctx: SharedPtr[TaskCtx[seq[Key]]]) {.gcsafe, raises: [].} =
      self.tp.spawn runDeleteTask(
        ctx, addr self.db, addr self.lock, addr records, self.readOnly
      )
  )
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  let skipped = ?((await fut).toKVStoreError())
  if skipped.len > 0:
    kvstore_sql_delete_conflict_total.inc(skipped.len.int64)
  return success(skipped)

# =============================================================================
# Atomic Batch API Implementation
# =============================================================================

method supportsAtomicBatch*(self: SQLiteKVStore): bool =
  true

method putAtomicImpl*(
    self: SQLiteKVStore, records: seq[RawKVRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## All-or-nothing batch put with CAS.
  ## If ANY record has a CAS conflict, NO records are committed.
  ## Returns conflict keys on rollback, empty seq on success.

  writeSqlPutAtomicMetrics(records)

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if records.len == 0:
    return success(newSeq[Key]())

  let fut = spawnJoin[seq[Key]](
    proc(ctx: SharedPtr[TaskCtx[seq[Key]]]) {.gcsafe, raises: [].} =
      self.tp.spawn runPutAtomicTask(
        ctx, addr self.db, addr self.lock, addr records, self.readOnly
      )
  )
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  let conflicts = ?((await fut).toKVStoreError())
  if conflicts.len > 0:
    kvstore_sql_putatomic_conflict_total.inc(conflicts.len.int64)
    kvstore_sql_putatomic_rollback_total.inc()
  return success(conflicts)

method deleteAtomicImpl*(
    self: SQLiteKVStore, records: seq[KeyKVRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## All-or-nothing batch delete with CAS.
  ## Same semantics as putAtomic().

  writeSqlDeleteAtomicMetrics(records)

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if records.len == 0:
    return success(newSeq[Key]())

  let fut = spawnJoin[seq[Key]](
    proc(ctx: SharedPtr[TaskCtx[seq[Key]]]) {.gcsafe, raises: [].} =
      self.tp.spawn runDeleteAtomicTask(
        ctx, addr self.db, addr self.lock, addr records, self.readOnly
      )
  )
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  let conflicts = ?((await fut).toKVStoreError())
  if conflicts.len > 0:
    kvstore_sql_deleteatomic_conflict_total.inc(conflicts.len.int64)
    kvstore_sql_deleteatomic_rollback_total.inc()
  return success(conflicts)

# =============================================================================
# Move (Key-Prefix Rename)
# =============================================================================

method moveKeysAtomicImpl*(
    self: SQLiteKVStore, oldPrefix, newPrefix: Key
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Move all keys from oldPrefix/* to newPrefix/* atomically.
  ## Single UPDATE statement in autocommit mode is already atomic.
  ## Returns KVConflictError if any destination key already exists.
  writeSqlMoveAtomicMetrics()

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let fut = spawnJoin[void](
    proc(ctx: SharedPtr[TaskCtx[void]]) {.gcsafe, raises: [].} =
      self.tp.spawn runMoveTask(
        ctx, addr self.db, addr self.lock, oldPrefix, newPrefix, self.readOnly
      )
  )
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  if err =? ((await fut).toKVStoreError()).errorOption:
    kvstore_sql_moveatomic_error_total.inc()
    return failure(err)

  success()

method moveKeysAtomicImpl*(
    self: SQLiteKVStore, moves: seq[(Key, Key)]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Move multiple prefix pairs atomically in a single transaction.
  ## All pairs succeed or all are rolled back.
  ## Returns KVConflictError if any destination key already exists.
  writeSqlMoveAtomicMetrics()

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let fut = spawnJoin[void](
    proc(ctx: SharedPtr[TaskCtx[void]]) {.gcsafe, raises: [].} =
      self.tp.spawn runMoveMultiTask(
        ctx, addr self.db, addr self.lock, addr moves, self.readOnly
      )
  )
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  if err =? ((await fut).toKVStoreError()).errorOption:
    kvstore_sql_moveatomic_error_total.inc()
    return failure(err)

  success()

method dropPrefixImpl*(
    self: SQLiteKVStore, prefix: Key
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Delete all records under prefix/* and the prefix key itself, atomically.
  ## Idempotent: no-op if no matching keys exist.
  writeSqlDropPrefixMetrics()

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let fut = spawnJoin[void](
    proc(ctx: SharedPtr[TaskCtx[void]]) {.gcsafe, raises: [].} =
      self.tp.spawn runDropPrefixTask(
        ctx, addr self.db, addr self.lock, prefix, self.readOnly
      )
  )
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  if err =? ((await fut).toKVStoreError()).errorOption:
    kvstore_sql_dropprefix_error_total.inc()
    return failure(err)

  success()

method dropPrefixImpl*(
    self: SQLiteKVStore, prefixes: seq[Key]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Drop multiple prefixes atomically in a single transaction.
  ## Idempotent: no-op if no matching keys exist.
  writeSqlDropPrefixMetrics()

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let fut = spawnJoin[void](
    proc(ctx: SharedPtr[TaskCtx[void]]) {.gcsafe, raises: [].} =
      self.tp.spawn runDropPrefixMultiTask(
        ctx, addr self.db, addr self.lock, addr prefixes, self.readOnly
      )
  )
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  if err =? ((await fut).toKVStoreError()).errorOption:
    kvstore_sql_dropprefix_error_total.inc()
    return failure(err)

  success()

method closeImpl*(self: SQLiteKVStore): Future[?!void] {.async: (raises: []).} =
  if self.closed:
    return success()

  self.closed = true

  try:
    let tasks = self.tasks.toSeq().mapIt(it.cancelAndWait())
    await noCancel allFutures(tasks)

    # Wait for dispose calls to finish (don't cancel them)
    await noCancel allFutures(self.disposeHandles.toSeq())

    ?self.db.close()
  finally:
    # don't change to deffer, otherwise you'll get a double close if
    # close is called twice
    deinitLock(self.lock) # don't leak resources

  return success()

method queryImpl*(
    self: SQLiteKVStore, query: Query
): Future[?!QueryIterRaw] {.async: (raises: [CancelledError]).} =
  kvstore_sql_query_total.inc()
  let startTime = Moment.now()

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  # Prepare private statement (no store lock needed - SQLite FULLMUTEX handles it)
  let
    s = ?prepareQueryStmt(self.db.env, query)
    signal =
      ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for query")

  # Create per-iterator state using module-level type
  var state =
    QueryIterState(stmt: s, tp: self.tp, queryValue: query.value, signal: signal)
  state.finished.store(false)
  state.isDisposed = false
  initLock(state.lock)

  let asyncLock = newAsyncLock()
  proc next(): Future[?!(?RawKVRecord)] {.async: (raises: [CancelledError]).} =
    if self.closed or state.isDisposed or state.finished.load():
      return failure newException(
        KVStoreError, "SQLiteKVStore is closed or iterator disposed/finished"
      )

    # AsyncLock serializes next() calls to ensure results are returned in order.
    # This is critical for sort order queries - without serialization, workers
    # race for the cursor lock and results come back in arbitrary order.
    await asyncLock.acquire()
    defer:
      if asyncLock.locked:
        if err =? catch(asyncLock.release()).errorOption:
          state.finished.store(true)
          return failure(err)

    # Re-check after await - close/dispose may have run
    if self.closed or state.isDisposed:
      return failure newException(
        KVStoreError, "SQLiteKVStore is closed or iterator disposed"
      )

    if state.finished.load():
      return success(RawKVRecord.none)

    let ctx = newSharedPtr(TaskCtx[?RawKVRecord](signal: state.signal))

    let taskFut = signal.wait()
    if taskFut.failed():
      state.finished.store(true)
      return failure(taskFut.error())

    state.tp.spawn runNextTask(
      ctx, addr state.stmt, addr state.lock, addr state.finished, state.queryValue
    )

    let fut = awaitSpawn(
      taskFut,
      onError = proc() {.async: (raises: []).} =
        state.finished.store(true),
    )

    # disposer task handle for graceful dispose
    state.iterTaskHandle = fut
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)
      state.iterTaskHandle = nil

    ?await fut

    let r = extract(ctx[].result).fromSpawn()
    if r.isErr or (r.isOk and r.get.isNone):
      state.finished.store(true)
    return r

  proc isFinished(): bool =
    state.finished.load()

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

      # don't leak resources
      discard disposeStmtSync(state.stmt)
      deinitLock(state.lock)
      kvstore_sql_active_iterators.dec()

    return success()

  var handle: Future[?!void].Raising([])
  proc dispose(): Future[?!void] {.async: (raises: []).} =
    # Signal workers to stop accepting new work
    state.finished.store(true)

    await noCancel asyncLock.acquire()
    defer:
      if asyncLock.locked:
        if err =? catch(asyncLock.release()).errorOption:
          state.finished.store(true)
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

  kvstore_sql_query_duration_seconds.observe(
    (Moment.now() - startTime).nanos.float64 / 1_000_000_000.0
  )
  kvstore_sql_active_iterators.inc()
  return success QueryIter.new(next, isFinished, isDisposed, dispose)

proc new*(T: type SQLiteKVStore, path: string, tp: Taskpool, readOnly = false): ?!T =
  ## Create a new SQLiteKVStore.
  ##
  ## Parameters:
  ##   - path: Database file path, or SqliteMemory for in-memory
  ##   - tp: Taskpool for async operations (required)
  ##   - readOnly: Open in read-only mode
  let flags =
    if readOnly:
      SQLITE_OPEN_READONLY or SQLITE_OPEN_FULLMUTEX
    else:
      SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE or SQLITE_OPEN_FULLMUTEX

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
