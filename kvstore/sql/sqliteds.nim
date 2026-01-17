{.push raises: [].}

import std/times
import std/sequtils
import std/options
import std/sets
import std/atomics
import std/locks

import pkg/chronos
import pkg/chronos/threadsync
import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi
import pkg/taskpools

import ../key
import ../query
import ../rawkvstore
import ./sqlitedsdb
import ./sqliteutils

export sqlitedsdb

type
  SQLiteKVStore* = ref object of KVStore
    readOnly: bool
    db: SQLiteDsDb
    activeIterators: Atomic[int]  # Track outstanding query iterators
    lock: Lock                    # Serializes access to shared prepared statements
    tp: Taskpool                  # Injected threadpool for async operations

  # TaskCtx bundles per-task state for cross-thread communication
  TaskCtx*[T] = object
    lock*: ptr Lock            # Store-level lock (shared across tasks)
    signal*: ThreadSignalPtr   # Completion notification (per-task)
    result*: T                 # Output value (per-task)

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

# =============================================================================
# Async Helper for Cancellation-Safe Wait
# =============================================================================

proc awaitSignal(signal: ThreadSignalPtr) {.async: (raises: [CancelledError]).} =
  ## Cancellation-safe wait for task completion.
  ## On cancellation, waits for worker to complete before re-raising.
  try:
    await signal.wait()
  except CancelledError as e:
    # Wait for worker to finish even if we're cancelled
    # This ensures ctx (stack-allocated) isn't a dangling pointer
    try:
      await noCancel signal.wait()
    except CancelledError:
      discard
    except AsyncError:
      discard
    raise e
  except AsyncError:
    # Signal wait failed - worker may not have completed properly
    discard

# =============================================================================
# Async Methods (public API)
# =============================================================================

method has*(
    self: SQLiteKVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  if self.tp.isNil:
    # No taskpool - run synchronously (legacy behavior)
    withLock(self.lock):
      return hasSync(self.db, key.id)

  # Use taskpool for true async
  var ctx = TaskCtx[?!bool](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runHasTask(addr ctx, addr self.db, addr self.lock, key.id)
  await awaitSignal(ctx.signal)
  return ctx.result

method get*(
    self: SQLiteKVStore, key: Key
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
  if self.tp.isNil:
    # No taskpool - run synchronously (legacy behavior)
    withLock(self.lock):
      return getSync(self.db, key)

  # Use taskpool for true async
  var ctx = TaskCtx[?!RawRecord](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runGetTask(addr ctx, addr self.db, addr self.lock, key)
  await awaitSignal(ctx.signal)
  return ctx.result

method get*(
    self: SQLiteKVStore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  if keys.len == 0:
    return success(newSeq[RawRecord]())

  if self.tp.isNil:
    # No taskpool - run synchronously (legacy behavior)
    withLock(self.lock):
      return getManySync(self.db, keys)

  # Use taskpool for true async
  var ctx = TaskCtx[?!seq[RawRecord]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runGetManyTask(addr ctx, addr self.db, addr self.lock, keys)
  await awaitSignal(ctx.signal)
  return ctx.result

method put*(
    self: SQLiteKVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if self.tp.isNil:
    # No taskpool - run synchronously (legacy behavior)
    withLock(self.lock):
      return putSync(self.db, records, self.readOnly)

  # Use taskpool for true async
  var ctx = TaskCtx[?!seq[Key]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runPutTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  await awaitSignal(ctx.signal)
  return ctx.result

method delete*(
    self: SQLiteKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if records.len == 0:
    return success(newSeq[Key]())

  if self.tp.isNil:
    # No taskpool - run synchronously (legacy behavior)
    withLock(self.lock):
      return deleteSync(self.db, records, self.readOnly)

  # Use taskpool for true async
  var ctx = TaskCtx[?!seq[Key]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runDeleteTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  await awaitSignal(ctx.signal)
  return ctx.result

method close*(
    self: SQLiteKVStore
): Future[?!void] {.async: (raises: [CancelledError]).} =
  # Debug check: warn if closing with active iterators
  let activeCount = self.activeIterators.load()
  if activeCount > 0:
    # In debug builds, this would be an assertion
    # In release, we just log and continue (sqlite3_close_v2 handles cleanup)
    debugEcho "WARNING: SQLiteKVStore.close() called with ", activeCount, " active iterator(s)"

  # Deinitialize the lock
  deinitLock(self.lock)

  # Propagate close errors
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

  if self.tp.isNil:
    # No taskpool - run synchronously (legacy behavior)
    withLock(self.lock):
      return putAtomicSync(self.db, records, self.readOnly)

  # Use taskpool for true async
  var ctx = TaskCtx[?!seq[Key]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runPutAtomicTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  await awaitSignal(ctx.signal)
  return ctx.result

method deleteAtomic*(
    self: SQLiteKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## All-or-nothing batch delete with CAS.
  ## Same semantics as putAtomic().

  if records.len == 0:
    return success(newSeq[Key]())

  if self.tp.isNil:
    # No taskpool - run synchronously (legacy behavior)
    withLock(self.lock):
      return deleteAtomicSync(self.db, records, self.readOnly)

  # Use taskpool for true async
  var ctx = TaskCtx[?!seq[Key]](
    lock: addr self.lock,
    signal: ThreadSignalPtr.new().expect("Failed to create thread signal")
  )
  defer: discard ctx.signal.close()

  self.tp.spawn runDeleteAtomicTask(addr ctx, addr self.db, addr self.lock, records, self.readOnly)
  await awaitSignal(ctx.signal)
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

  let
    queryStmt =
      QueryStmt.prepare(self.db.env, queryStr).expect("Query prepare should not fail")
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

  var finished = false

  # Track this iterator
  discard self.activeIterators.fetchAdd(1)

  proc next(): Future[?!(?RawRecord)] {.async: (raises: [CancelledError]).} =
    if finished:
      return failure(newException(QueryEndedError, "Calling next on a finished query!"))

    let v = sqlite3_step(s)

    case v
    of SQLITE_ROW:
      let
        key = Key.init($sqlite3_column_text_not_null(s, QueryStmtIdCol)).expect(
            "Key should should not fail"
          )

        blob: pointer =
          if query.value:
            sqlite3_column_blob(s, QueryStmtDataCol)
          else:
            nil

      # detect out-of-memory error
      # see the conversion table and final paragraph of:
      # https://www.sqlite.org/c3ref/column_blob.html
      # see also https://www.sqlite.org/rescode.html

      # the "data" column can be NULL so in order to detect an out-of-memory
      # error it is necessary to check that the result is a null pointer and
      # that the result code is an error code
      if blob.isNil:
        let v = sqlite3_errcode(sqlite3_db_handle(s))

        if not (v in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
          finished = true
          return failure newException(KVStoreError, $sqlite3_errstr(v))

      let
        dataLen = sqlite3_column_bytes(s, QueryStmtDataCol)
        data =
          if not blob.isNil:
            @(toOpenArray(cast[ptr UncheckedArray[byte]](blob), 0, dataLen - 1))
          else:
            @[]
        versionCol =
          if query.value: QueryStmtVersionColWithData else: QueryStmtVersionColNoData
        version = sqlite3_column_int64(s, versionCol.cint).uint64

      return success RawRecord.init(key, data, version).some
    of SQLITE_DONE:
      finished = true
      return success RawRecord.none
    else:
      finished = true
      return failure newException(KVStoreError, $sqlite3_errstr(v))

  proc isFinished(): bool =
    finished

  proc dispose() =
    # Finalize the prepared statement to release resources
    # sqlite3_finalize returns SQLITE_OK even if statement is nil
    discard sqlite3_finalize(s)
    # Decrement active iterator count
    discard self.activeIterators.fetchSub(1)

  return success QueryIter.new(next, isFinished, dispose)

proc new*(T: type SQLiteKVStore, path: string, readOnly = false, tp: Taskpool = nil): ?!T =
  ## Create a new SQLiteKVStore.
  ##
  ## Parameters:
  ##   - path: Database file path, or SqliteMemory for in-memory
  ##   - readOnly: Open in read-only mode
  ##   - tp: Optional taskpool for true async (nil = legacy sync behavior)
  let flags =
    if readOnly:
      SQLITE_OPEN_READONLY
    else:
      SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  var store = T(db: ?SQLiteDsDb.open(path, flags), readOnly: readOnly, tp: tp)
  initLock(store.lock)
  success store

proc new*(T: type SQLiteKVStore, db: SQLiteDsDb, tp: Taskpool = nil): ?!T =
  ## Create a new SQLiteKVStore from an existing database handle.
  ##
  ## Parameters:
  ##   - db: Pre-opened SQLiteDsDb
  ##   - tp: Optional taskpool for true async (nil = legacy sync behavior)
  var store = T(db: db, readOnly: db.readOnly, tp: tp)
  initLock(store.lock)
  success store
