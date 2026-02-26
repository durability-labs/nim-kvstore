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
    tasks: HashSet[Future[?!void]] # Track outstanding tasks for close()
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
    ctx[].result = unsafeIsolate(hasSync(db[], keyId))

proc runHasManyTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    keys: seq[Key],
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runHasManyTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(hasManySync(db[], keys))

proc runGetTask(
    ctx: SharedPtr[TaskCtx[RawKVRecord]], db: ptr SQLiteDsDb, lock: ptr Lock, key: Key
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runGetTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(getSync(db[], key))

proc runGetManyTask(
    ctx: SharedPtr[TaskCtx[seq[RawKVRecord]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    keys: seq[Key],
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runGetManyTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(getManySync(db[], keys))

proc runPutTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    records: seq[RawKVRecord],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runPutTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(putSync(db[], records, readOnly))

proc runDeleteTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    records: seq[KeyKVRecord],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDeleteTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(deleteSync(db[], records, readOnly))

proc runPutAtomicTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    records: seq[RawKVRecord],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runPutAtomicTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(putAtomicSync(db[], records, readOnly))

proc runDeleteAtomicTask(
    ctx: SharedPtr[TaskCtx[seq[Key]]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    records: seq[KeyKVRecord],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDeleteAtomicTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(deleteAtomicSync(db[], records, readOnly))

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
    ctx[].result = unsafeIsolate(moveSync(db[], oldPrefix, newPrefix, readOnly))

proc runMoveMultiTask(
    ctx: SharedPtr[TaskCtx[void]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    moves: seq[(Key, Key)],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runMoveMultiTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(moveSyncMulti(db[], moves, readOnly))

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
    ctx[].result = unsafeIsolate(dropPrefixSync(db[], prefix, readOnly))

proc runDropPrefixMultiTask(
    ctx: SharedPtr[TaskCtx[void]],
    db: ptr SQLiteDsDb,
    lock: ptr Lock,
    prefixes: seq[Key],
    readOnly: bool,
) {.gcsafe.} =
  defer:
    let res = ctx[].signal.fireSync()
    if res.isErr:
      warn "fireSync failed in runDropPrefixMultiTask", error = res.error

  withLock(lock[]):
    ctx[].result = unsafeIsolate(dropPrefixSyncMulti(db[], prefixes, readOnly))

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
    ctx[].result = isolate(success(RawKVRecord.none))
    return

  withLock(lock[]):
    # Double-check after acquiring lock
    if finished[].load():
      ctx[].result = isolate(success(RawKVRecord.none))
      return

    ctx[].result = unsafeIsolate(nextSync(stmt[], queryValue))

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
    let signal =
      ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for has")

    let ctx = newSharedPtr(TaskCtx[bool](signal: signal))
    defer:
      if err =? signal.close().errorOption:
        warn "signal.close failed in has", error = err
      # SharedPtr handles TaskCtx cleanup automatically

    let taskFut = signal.wait()
    self.tp.spawn runHasTask(ctx, addr self.db, addr self.lock, keys[0].id)

    let fut = awaitSignal(taskFut)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    let exists = ?extract(ctx[].result)
    return success(
      if exists:
        @[keys[0]]
      else:
        newSeq[Key]()
    )
  else:
    # Multi-key path
    let signal =
      ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for has")

    let ctx = newSharedPtr(TaskCtx[seq[Key]](signal: signal))
    defer:
      if err =? signal.close().errorOption:
        warn "signal.close failed in has", error = err
      # SharedPtr handles TaskCtx cleanup automatically

    let taskFut = signal.wait()
    self.tp.spawn runHasManyTask(ctx, addr self.db, addr self.lock, keys)

    let fut = awaitSignal(taskFut)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    return extract(ctx[].result)

method getImpl*(
    self: SQLiteKVStore, keys: seq[Key]
): Future[?!seq[RawKVRecord]] {.async: (raises: [CancelledError]).} =
  writeSqlGetMetrics(keys)

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  if keys.len == 0:
    return success(newSeq[RawKVRecord]())

  if keys.len == 1:
    let signal =
      ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for get")

    let ctx = newSharedPtr(TaskCtx[RawKVRecord](signal: signal))
    defer:
      if err =? signal.close().errorOption:
        warn "signal.close failed in get", error = err
      # SharedPtr handles TaskCtx cleanup automatically

    let taskFut = signal.wait()
    self.tp.spawn runGetTask(ctx, addr self.db, addr self.lock, keys[0])

    let fut = awaitSignal(taskFut)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    # Match batch get semantics: return empty seq for missing key, not error
    without extracted =? extract(ctx[].result), err:
      if err of KVStoreKeyNotFound:
        return success(newSeq[RawKVRecord]())
      return failure(err)

    when defined(kvstore_expensive_metrics):
      kvstore_sql_get_value_bytes.observe(extracted.val.len.float64)

    return success(@[extracted])
  else:
    let signal =
      ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for get")

    let ctx = newSharedPtr(TaskCtx[seq[RawKVRecord]](signal: signal))
    defer:
      if err =? signal.close().errorOption:
        warn "signal.close failed in get", error = err
      # SharedPtr handles TaskCtx cleanup automatically

    let taskFut = signal.wait()
    self.tp.spawn runGetManyTask(ctx, addr self.db, addr self.lock, keys)

    let fut = awaitSignal(taskFut)
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)

    ?await fut

    let records = ?extract(ctx[].result)
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

  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for put")

  let ctx = newSharedPtr(TaskCtx[seq[Key]](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in put", error = err
    # SharedPtr handles TaskCtx cleanup automatically

  let taskFut = signal.wait()
  self.tp.spawn runPutTask(ctx, addr self.db, addr self.lock, records, self.readOnly)

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  let skipped = ?extract(ctx[].result)
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

  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for delete")

  let ctx = newSharedPtr(TaskCtx[seq[Key]](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in delete", error = err
    # SharedPtr handles TaskCtx cleanup automatically

  let taskFut = signal.wait()
  self.tp.spawn runDeleteTask(ctx, addr self.db, addr self.lock, records, self.readOnly)

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  let skipped = ?extract(ctx[].result)
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

  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for putAtomic")

  let ctx = newSharedPtr(TaskCtx[seq[Key]](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in putAtomic", error = err
    # SharedPtr handles TaskCtx cleanup automatically

  let taskFut = signal.wait()
  self.tp.spawn runPutAtomicTask(
    ctx, addr self.db, addr self.lock, records, self.readOnly
  )

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  let conflicts = ?extract(ctx[].result)
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

  let signal =
    ?ThreadSignalPtr.new().toKVError(
      context = "Failed to create signal for deleteAtomic"
    )

  let ctx = newSharedPtr(TaskCtx[seq[Key]](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in deleteAtomic", error = err
    # SharedPtr handles TaskCtx cleanup automatically

  let taskFut = signal.wait()
  self.tp.spawn runDeleteAtomicTask(
    ctx, addr self.db, addr self.lock, records, self.readOnly
  )

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  ?await fut

  let conflicts = ?extract(ctx[].result)
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

  let signal =
    ?ThreadSignalPtr.new().toKVError(
      context = "Failed to create signal for moveKeysAtomic"
    )

  let ctx = newSharedPtr(TaskCtx[void](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in moveKeysAtomic", error = err

  let taskFut = signal.wait()
  self.tp.spawn runMoveTask(
    ctx, addr self.db, addr self.lock, oldPrefix, newPrefix, self.readOnly
  )

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  if err =? (await fut).errorOption:
    kvstore_sql_moveatomic_error_total.inc()
    return failure(err)

  extract(ctx[].result)

method moveKeysAtomicImpl*(
    self: SQLiteKVStore, moves: seq[(Key, Key)]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Move multiple prefix pairs atomically in a single transaction.
  ## All pairs succeed or all are rolled back.
  ## Returns KVConflictError if any destination key already exists.
  writeSqlMoveAtomicMetrics()

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let signal =
    ?ThreadSignalPtr.new().toKVError(
      context = "Failed to create signal for moveKeysAtomicMulti"
    )

  let ctx = newSharedPtr(TaskCtx[void](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in moveKeysAtomicMulti", error = err

  let taskFut = signal.wait()
  self.tp.spawn runMoveMultiTask(
    ctx, addr self.db, addr self.lock, moves, self.readOnly
  )

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  if err =? (await fut).errorOption:
    kvstore_sql_moveatomic_error_total.inc()
    return failure(err)

  extract(ctx[].result)

method dropPrefixImpl*(
    self: SQLiteKVStore, prefix: Key
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Delete all records under prefix/* and the prefix key itself, atomically.
  ## Idempotent: no-op if no matching keys exist.
  writeSqlDropPrefixMetrics()

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let signal =
    ?ThreadSignalPtr.new().toKVError(context = "Failed to create signal for dropPrefix")

  let ctx = newSharedPtr(TaskCtx[void](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in dropPrefix", error = err

  let taskFut = signal.wait()
  self.tp.spawn runDropPrefixTask(
    ctx, addr self.db, addr self.lock, prefix, self.readOnly
  )

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  if err =? (await fut).errorOption:
    kvstore_sql_dropprefix_error_total.inc()
    return failure(err)

  extract(ctx[].result)

method dropPrefixImpl*(
    self: SQLiteKVStore, prefixes: seq[Key]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Drop multiple prefixes atomically in a single transaction.
  ## Idempotent: no-op if no matching keys exist.
  writeSqlDropPrefixMetrics()

  if self.closed:
    return failure(newException(KVStoreError, "SQLiteKVStore is closed"))

  let signal =
    ?ThreadSignalPtr.new().toKVError(
      context = "Failed to create signal for dropPrefixMulti"
    )

  let ctx = newSharedPtr(TaskCtx[void](signal: signal))
  defer:
    if err =? signal.close().errorOption:
      warn "signal.close failed in dropPrefixMulti", error = err

  let taskFut = signal.wait()
  self.tp.spawn runDropPrefixMultiTask(
    ctx, addr self.db, addr self.lock, prefixes, self.readOnly
  )

  let fut = awaitSignal(taskFut)
  self.tasks.incl(fut)
  defer:
    self.tasks.excl(fut)

  if err =? (await fut).errorOption:
    kvstore_sql_dropprefix_error_total.inc()
    return failure(err)

  extract(ctx[].result)

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
    state.tp.spawn runNextTask(
      ctx, addr state.stmt, addr state.lock, addr state.finished, state.queryValue
    )

    let fut = awaitSignal(
      taskFut,
      onError = proc() {.async: (raises: [CancelledError]).} =
        state.finished.store(true),
    )

    # disposer task handle for graceful dispose
    state.iterTaskHandle = fut
    self.tasks.incl(fut)
    defer:
      self.tasks.excl(fut)
      state.iterTaskHandle = nil

    ?await fut

    let r = extract(ctx[].result)
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
