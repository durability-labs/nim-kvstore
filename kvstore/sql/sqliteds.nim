{.push raises: [].}

when not compileOption("threads"):
  {.error: "SQLiteKVStore requires --threads:on".}

import std/times
import std/sequtils
import std/options
import std/sets
import std/atomics
import std/locks
import std/os

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
import pkg/taskpools

import ../key
import ../query
import ../kvstore
import ./sqlitedsdb
import ./sqliteutils

export sqlitedsdb

type
  SQLiteKVStore* = ref object of KVStore
    readOnly: bool
    db: SQLiteDsDb
    lock: Lock                    # Serializes access to shared prepared statements
    tp: Taskpool                  # Injected threadpool for async operations

  # TaskCtx bundles per-task state for cross-thread communication
  TaskCtx*[T] = object
    lock*: ptr Lock            # Store-level lock (shared across tasks)
    signal*: ThreadSignalPtr   # Completion notification (per-task)
    result*: T                 # Output value (per-task)

  # Per-iterator state for query operations
  # Each iterator has its own lock (not the store-wide lock) since each
  # has a private prepared statement. The lock only protects against
  # concurrent next() calls on the same iterator instance.
  QueryIterState* = ref object
    stmt*: RawStmtPtr
    lock*: Lock
    finished*: Atomic[bool]    # Atomic for thread-safe access without lock
    isDisposed*: Atomic[bool]  # Prevent double-dispose
    inFlight*: Atomic[int]     # Track in-flight next() workers (non-blocking)
    tp*: Taskpool              # For spawning next() workers
    queryValue*: bool          # Whether to include value in results

proc path*(self: SQLiteKVStore): string =
  self.db.dbPath

proc `readOnly=`*(
  self: SQLiteKVStore
): bool {.error: "readOnly should not be assigned".}

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

type RollbackError* = object of CatchableError

proc newRollbackError(rbErr: ref CatchableError, opErrMsg: string): ref RollbackError =
  let msg =
    "Rollback initiated because of: " & opErrMsg & ". Rollback failed because of: " &
    rbErr.msg
  newException(RollbackError, msg, parentException = rbErr)

proc newRollbackError(
    rbErr: ref CatchableError, opErr: ref CatchableError
): ref RollbackError =
  newRollbackError(rbErr, opErr.msg)

proc ensureWritable(self: SQLiteKVStore): ?!void =
  if self.readOnly:
    return failure(newBackendError("SQLite store opened read-only"))
  success()

proc boundedToken(token: uint64): ?!int64 =
  if token > uint64(high(int64)):
    return failure(newCorruptionError("SQLite token overflow"))
  success token.int64

# =============================================================================
# Sync Operations (for threading - no async, no Chronos dependencies)
# =============================================================================

proc hasSync(db: SQLiteDsDb, keyId: string): ?!bool {.gcsafe.} =
  ## Synchronous check if key exists
  var exists = false
  proc onRow(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  discard ?db.containsStmt.query((keyId), onRow)
  success exists

proc getSync(db: SQLiteDsDb, key: Key): ?!RawRecord {.gcsafe.} =
  ## Synchronous get single record
  var
    rowFound = false
    value: seq[byte]
    token = 0'i64

  proc onRow(s: RawStmtPtr) =
    rowFound = true
    value = dataCol(s, GetSingleStmtDataCol)()
    token = versionCol(s, GetSingleStmtVersionCol)()
    if token < 0:
      raiseAssert("Negative token detected")

  discard ?db.getSingleStmt.query((key.id), onRow)

  if not rowFound:
    return failure(newException(KVStoreKeyNotFound, "Key doesn't exist"))

  success RawRecord.init(key, value, token.uint64)

proc getManySync(db: SQLiteDsDb, keys: seq[Key]): ?!seq[RawRecord] {.gcsafe.} =
  ## Synchronous get multiple records
  if keys.len == 0:
    return success(newSeq[RawRecord]())

  var records: seq[RawRecord]

  proc onRow(s: RawStmtPtr) =
    let
      keyId = idCol(s, GetManyStmtIdCol)()
      value = dataCol(s, GetManyStmtDataCol)()
      token = versionCol(s, GetManyStmtVersionCol)()

    if token < 0:
      raiseAssert("Negative token detected")

    records.add(RawRecord.init(Key.init(keyId).expect("Invalid key from DB"), value, token.uint64))

  let queryStr = makeGetManyParamQuery(keys.len)
  let keyIds = keys.mapIt(it.id)
  discard ?db.env.queryWithStrings(queryStr, keyIds, onRow)

  success records

proc putSync(db: SQLiteDsDb, records: seq[RawRecord], readOnly: bool): ?!seq[Key] {.gcsafe.} =
  ## Synchronous put records
  if readOnly:
    return failure(newBackendError("SQLite store opened read-only"))

  var
    skipped: seq[Key]
    stamp = timestamp()
    inTransaction = false
    committed = false

  ?db.beginStmt.exec()
  inTransaction = true

  defer:
    if inTransaction and not committed:
      discard db.rollbackStmt.exec()

  for record in records:
    var changed = false

    proc onRow(s: RawStmtPtr) =
      changed = true

    if record.token == 0:
      discard ?db.insertStmt.query((record.key.id, record.val, stamp), onRow)
    else:
      let token = ?boundedToken(record.token)
      discard ?db.updateStmt.query((record.val, stamp, record.key.id, token), onRow)

    let changes = ?db.checkChanges()
    if changes > 1:
      return failure(newCorruptionError(
        "Multiple rows affected by CAS operation on key: " & record.key.id))

    if not changed:
      skipped.add(record.key)

  ?db.endStmt.exec()
  committed = true

  success skipped

proc deleteSync(db: SQLiteDsDb, records: seq[KeyRecord], readOnly: bool): ?!seq[Key] {.gcsafe.} =
  ## Synchronous delete records
  if readOnly:
    return failure(newBackendError("SQLite store opened read-only"))

  if records.len == 0:
    return success(newSeq[Key]())

  var
    deletedIds = initHashSet[string]()
    inTransaction = false
    committed = false

  let queryStr = makeDeleteManyParamQuery(records.len) & " RETURNING " & IdColName

  var pairs: seq[(string, int64)]
  for record in records:
    let token = ?boundedToken(record.token)
    pairs.add((record.key.id, token))

  ?db.beginStmt.exec()
  inTransaction = true

  defer:
    if inTransaction and not committed:
      discard db.rollbackStmt.exec()

  proc onRow(s: RawStmtPtr) =
    deletedIds.incl($sqlite3_column_text_not_null(s, 0.cint))

  discard ?db.env.queryWithIdVersionPairs(queryStr, pairs, onRow)

  let changes = ?db.checkChanges()
  if changes > records.len:
    return failure(newCorruptionError(
      "Delete affected more rows (" & $changes & ") than records (" & $records.len & ")"))

  ?db.endStmt.exec()
  committed = true

  var skipped: seq[Key]
  for record in records:
    if record.key.id notin deletedIds:
      skipped.add(record.key)

  success skipped

proc putAtomicSync(db: SQLiteDsDb, records: seq[RawRecord], readOnly: bool): ?!seq[Key] {.gcsafe.} =
  ## Synchronous all-or-nothing batch put
  if readOnly:
    return failure(newBackendError("SQLite store opened read-only"))

  if records.len == 0:
    return success(newSeq[Key]())

  var
    conflicts: seq[Key]
    stamp = timestamp()
    inTransaction = false
    committed = false

  ?db.beginStmt.exec()
  inTransaction = true

  defer:
    if inTransaction and not committed:
      discard db.rollbackStmt.exec()

  # First pass: check ALL CAS conditions, collect conflicts
  for record in records:
    var exists = false
    var currentToken = 0'i64

    proc onRow(s: RawStmtPtr) =
      exists = true
      currentToken = versionCol(s, GetSingleStmtVersionCol)()

    discard ?db.getSingleStmt.query((record.key.id), onRow)

    let conflict =
      if record.token == 0:
        exists  # Insert-only but key exists
      elif not exists:
        true    # Update expected but key missing
      else:
        currentToken != record.token.int64  # Token mismatch

    if conflict:
      conflicts.add(record.key)

  # If any conflicts, rollback and return them
  if conflicts.len > 0:
    ?db.rollbackStmt.exec()
    committed = true  # Prevent finally rollback
    return success conflicts

  # Second pass: apply ALL writes (no conflicts)
  for record in records:
    var changed = false
    proc onRow(s: RawStmtPtr) =
      changed = true

    if record.token == 0:
      discard ?db.insertStmt.query((record.key.id, record.val, stamp), onRow)
    else:
      let token = ?boundedToken(record.token)
      discard ?db.updateStmt.query((record.val, stamp, record.key.id, token), onRow)

  ?db.endStmt.exec()
  committed = true
  success newSeq[Key]()

proc deleteAtomicSync(db: SQLiteDsDb, records: seq[KeyRecord], readOnly: bool): ?!seq[Key] {.gcsafe.} =
  ## Synchronous all-or-nothing batch delete
  if readOnly:
    return failure(newBackendError("SQLite store opened read-only"))

  if records.len == 0:
    return success(newSeq[Key]())

  var
    conflicts: seq[Key]
    inTransaction = false
    committed = false

  ?db.beginStmt.exec()
  inTransaction = true

  defer:
    if inTransaction and not committed:
      discard db.rollbackStmt.exec()

  # First pass: check ALL CAS conditions
  for record in records:
    var exists = false
    var currentToken = 0'i64

    proc onRow(s: RawStmtPtr) =
      exists = true
      currentToken = versionCol(s, GetSingleStmtVersionCol)()

    discard ?db.getSingleStmt.query((record.key.id), onRow)

    if not exists or currentToken != record.token.int64:
      conflicts.add(record.key)

  # If any conflicts, rollback
  if conflicts.len > 0:
    ?db.rollbackStmt.exec()
    committed = true
    return success conflicts

  # Second pass: delete all
  for record in records:
    let token = ?boundedToken(record.token)

    proc onRow(s: RawStmtPtr) =
      discard  # Consume the returned row

    discard ?db.deleteStmt.query((record.key.id, token), onRow)

  ?db.endStmt.exec()
  committed = true
  success newSeq[Key]()

# =============================================================================
# Task Workers (for threadpool - top-level procs)
# =============================================================================

proc runHasTask(ctx: ptr TaskCtx[?!bool], db: ptr SQLiteDsDb,
                lock: ptr Lock, keyId: string) {.gcsafe.} =
  ## Task worker for has() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = hasSync(db[], keyId)

proc runGetTask(ctx: ptr TaskCtx[?!RawRecord], db: ptr SQLiteDsDb,
                lock: ptr Lock, key: Key) {.gcsafe.} =
  ## Task worker for get() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = getSync(db[], key)

proc runGetManyTask(ctx: ptr TaskCtx[?!seq[RawRecord]], db: ptr SQLiteDsDb,
                    lock: ptr Lock, keys: seq[Key]) {.gcsafe.} =
  ## Task worker for get(keys) operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = getManySync(db[], keys)

proc runPutTask(ctx: ptr TaskCtx[?!seq[Key]], db: ptr SQLiteDsDb,
                lock: ptr Lock, records: seq[RawRecord], readOnly: bool) {.gcsafe.} =
  ## Task worker for put() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = putSync(db[], records, readOnly)

proc runDeleteTask(ctx: ptr TaskCtx[?!seq[Key]], db: ptr SQLiteDsDb,
                   lock: ptr Lock, records: seq[KeyRecord], readOnly: bool) {.gcsafe.} =
  ## Task worker for delete() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = deleteSync(db[], records, readOnly)

proc runPutAtomicTask(ctx: ptr TaskCtx[?!seq[Key]], db: ptr SQLiteDsDb,
                      lock: ptr Lock, records: seq[RawRecord], readOnly: bool) {.gcsafe.} =
  ## Task worker for putAtomic() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = putAtomicSync(db[], records, readOnly)

proc runDeleteAtomicTask(ctx: ptr TaskCtx[?!seq[Key]], db: ptr SQLiteDsDb,
                         lock: ptr Lock, records: seq[KeyRecord], readOnly: bool) {.gcsafe.} =
  ## Task worker for deleteAtomic() operation
  defer: discard ctx[].signal.fireSync()
  withLock(lock[]):
    ctx[].result = deleteAtomicSync(db[], records, readOnly)

proc runNextTask(ctx: ptr TaskCtx[?!(?RawRecord)],
                 stmt: ptr RawStmtPtr,
                 lock: ptr Lock,
                 finished: ptr Atomic[bool],
                 inFlight: ptr Atomic[int],
                 queryValue: bool) {.gcsafe.} =
  ## Task worker for query iterator next() operation.
  ## Uses per-iterator lock, not store-wide lock.
  defer:
    discard inFlight[].fetchSub(1)
    discard ctx[].signal.fireSync()

  # Type alias for cleaner Result construction
  type R = typeof(ctx[].result)

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

    let v = sqlite3_step(stmt[])

    case v
    of SQLITE_ROW:
      let keyStr = $sqlite3_column_text_not_null(stmt[], QueryStmtIdCol)
      let key = Key.init(keyStr).valueOr:
        finished[].store(true)
        ctx[].result = R.err(newException(KVStoreError, "Invalid key in database: " & keyStr))
        return

      let blob: pointer =
        if queryValue:
          sqlite3_column_blob(stmt[], QueryStmtDataCol)
        else:
          nil

      # detect out-of-memory error
      if blob.isNil:
        let errCode = sqlite3_errcode(sqlite3_db_handle(stmt[]))
        if not (errCode in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
          finished[].store(true)
          ctx[].result = R.err(newException(KVStoreError, $sqlite3_errstr(errCode)))
          return

      let
        dataLen = sqlite3_column_bytes(stmt[], QueryStmtDataCol)
        data =
          if not blob.isNil:
            @(toOpenArray(cast[ptr UncheckedArray[byte]](blob), 0, dataLen - 1))
          else:
            @[]
        versionCol =
          if queryValue: QueryStmtVersionColWithData else: QueryStmtVersionColNoData
        version = sqlite3_column_int64(stmt[], versionCol.cint).uint64

      ctx[].result = R.ok(RawRecord.init(key, data, version).some)
    of SQLITE_DONE:
      finished[].store(true)
      ctx[].result = R.ok(RawRecord.none)
    else:
      finished[].store(true)
      ctx[].result = R.err(newException(KVStoreError, $sqlite3_errstr(v)))

proc runDisposeTask(ctx: ptr TaskCtx[?!void],
                    inFlight: ptr Atomic[int],
                    lock: ptr Lock,
                    stmt: ptr RawStmtPtr) {.gcsafe.} =
  ## Task worker for query iterator dispose() operation.
  ## Waits for all next() workers to complete, then cleans up.
  defer: discard ctx[].signal.fireSync()

  # Spin-wait for all in-flight next() workers (blocking OK in threadpool)
  while inFlight[].load() > 0:
    sleep(1)  # 1ms sleep to avoid busy-waiting

  # Now safe - no workers can be using the lock
  withLock(lock[]):
    discard sqlite3_finalize(stmt[])
  deinitLock(lock[])

  ctx[].result = success()

# =============================================================================
# Async Helper for Cancellation-Safe Wait
# =============================================================================

proc awaitSignal(signal: ThreadSignalPtr): Future[?!void] {.async: (raises: [CancelledError]).} =
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

# =============================================================================
# Async Methods (public API)
# =============================================================================

method has*(
    self: SQLiteKVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  var ctx = TaskCtx[?!bool](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runHasTask(addr ctx, addr self.db, addr self.lock, key.id)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method get*(
    self: SQLiteKVStore, key: Key
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
  var ctx = TaskCtx[?!RawRecord](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runGetTask(addr ctx, addr self.db, addr self.lock, key)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method get*(
    self: SQLiteKVStore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  if keys.len == 0:
    return success(newSeq[RawRecord]())

  var ctx = TaskCtx[?!seq[RawRecord]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runGetManyTask(addr ctx, addr self.db, addr self.lock, keys)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method put*(
    self: SQLiteKVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  var ctx = TaskCtx[?!seq[Key]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runPutTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method delete*(
    self: SQLiteKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if records.len == 0:
    return success(newSeq[Key]())

  var ctx = TaskCtx[?!seq[Key]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runDeleteTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method close*(
    self: SQLiteKVStore
): Future[?!void] {.async: (raises: [CancelledError]).} =
  deinitLock(self.lock)
  return self.db.close()

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

  if records.len == 0:
    return success(newSeq[Key]())

  var ctx = TaskCtx[?!seq[Key]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runPutAtomicTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method deleteAtomic*(
    self: SQLiteKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## All-or-nothing batch delete with CAS.
  ## Same semantics as putAtomic().

  if records.len == 0:
    return success(newSeq[Key]())

  var ctx = TaskCtx[?!seq[Key]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runDeleteAtomicTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  ?await awaitSignal(ctx.signal)
  return ctx.result

method query*(
    self: SQLiteKVStore, query: Query
): Future[?!QueryIterRaw] {.async: (raises: [CancelledError]).} =
  var queryStr = if query.value: QueryStmtDataIdStr else: QueryStmtIdStr

  if query.sort == SortOrder.Descending:
    queryStr &= QueryStmtOrderDescending
  else:
    queryStr &= QueryStmtOrderAscending

  if query.limit != 0:
    queryStr &= QueryStmtLimit

  if query.offset != 0:
    queryStr &= QueryStmtOffset

  # Prepare private statement (no store lock needed - SQLite FULLMUTEX handles it)
  let
    queryStmt = QueryStmt.prepare(self.db.env, queryStr).expect("Query prepare should not fail")
    s = RawStmtPtr(queryStmt)

  var v = sqlite3_bind_text(
    s, 1.cint, (query.key.id & "*").cstring, -1.cint, SQLITE_TRANSIENT_GCSAFE
  )

  if v != SQLITE_OK:
    return failure newException(KVStoreError, $sqlite3_errstr(v))

  if query.limit != 0:
    v = sqlite3_bind_int(s, 2.cint, query.limit.cint)

    if v != SQLITE_OK:
      return failure newException(KVStoreError, $sqlite3_errstr(v))

  if query.offset != 0:
    v = sqlite3_bind_int(s, 3.cint, query.offset.cint)

    if v != SQLITE_OK:
      return failure newException(KVStoreError, $sqlite3_errstr(v))

  # Create per-iterator state using module-level type
  var state = QueryIterState(
    stmt: s,
    tp: self.tp,
    queryValue: query.value
  )
  state.finished.store(false)
  state.isDisposed.store(false)
  state.inFlight.store(0)
  initLock(state.lock)

  proc next(): Future[?!(?RawRecord)] {.async: (raises: [CancelledError]).} =
    # Early check with atomic load (fast path for finished iterator)
    if state.finished.load():
      return success(RawRecord.none)

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[?!(?RawRecord)](
      lock: addr state.lock,
      signal: signal
    )
    defer: discard ctx.signal.close()

    # Increment in-flight counter BEFORE spawn (non-blocking atomic op)
    discard state.inFlight.fetchAdd(1)
    state.tp.spawn runNextTask(addr ctx, addr state.stmt, addr state.lock,
                               addr state.finished, addr state.inFlight, state.queryValue)

    # Wait for result - on cancellation, mark finished first then wait for worker
    let taskFut = ctx.signal.wait()
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

  proc dispose() =
    # Sync dispose - used by destructor as fallback
    # Marks as finished but can't safely wait for workers
    if state.isDisposed.exchange(true):
      return  # Already disposed
    state.finished.store(true)

  proc disposeAsync(): Future[?!void] {.async: (raises: [CancelledError]).} =
    # Atomic check-and-set to prevent double-dispose
    if state.isDisposed.exchange(true):
      return success()

    # Signal workers to stop accepting new work
    state.finished.store(true)

    let signal = ThreadSignalPtr.new().valueOr:
      return failure(newException(KVStoreError, error))
    var ctx = TaskCtx[?!void](
      lock: addr state.lock,
      signal: signal
    )
    defer: discard ctx.signal.close()

    state.tp.spawn runDisposeTask(addr ctx, addr state.inFlight,
                                   addr state.lock, addr state.stmt)

    # Cancellation-safe wait for cleanup worker
    let taskFut = ctx.signal.wait()
    if err =? catch(await taskFut.join()).errorOption:
      # Must wait for cleanup worker to finish
      ?catch(await noCancel taskFut)
      if err of CancelledError:
        raise (ref CancelledError)(err)
      return failure(err)

    return ctx.result

  return success QueryIter.new(next, isFinished, dispose, disposeAsync)

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
