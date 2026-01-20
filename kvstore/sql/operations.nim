{.push raises: [].}

## Synchronous SQLite database operations.
##
## These are pure database operations with no async or threading concerns.
## They are used by the threading layer in sqliteds.nim.

import std/times
import std/sequtils
import std/sets
import std/options

import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi

import ../key
import ../query
import ../kvstore
import ./sqlitedsdb
import ./sqliteutils

const
  # SQLite default SQLITE_MAX_VARIABLE_NUMBER is 999
  # Leave some headroom for safety
  MaxSqliteParams* = 990
  # For delete, each record uses 2 params (id, token)
  MaxDeleteChunkSize* = MaxSqliteParams div 2

# =============================================================================
# Utilities
# =============================================================================

proc timestamp*(t = epochTime()): int64 =
  (t * 1_000_000).int64

type RollbackError* = object of CatchableError

proc newRollbackError*(rbErr: ref CatchableError, opErrMsg: string): ref RollbackError =
  let msg =
    "Rollback initiated because of: " & opErrMsg & ". Rollback failed because of: " &
    rbErr.msg
  newException(RollbackError, msg, parentException = rbErr)

proc newRollbackError*(
    rbErr: ref CatchableError, opErr: ref CatchableError
): ref RollbackError =
  newRollbackError(rbErr, opErr.msg)

proc boundedToken*(token: uint64): ?!int64 =
  if token > uint64(high(int64)):
    return failure(newCorruptionError("SQLite token overflow"))
  success token.int64

proc checkWritable*(readOnly: bool): ?!void =
  if readOnly:
    return failure(newBackendError("SQLite store opened read-only"))
  success()

# =============================================================================
# Transaction Helper
# =============================================================================

template withTransaction*(db: SQLiteDsDb, body: untyped): ?!void =
  ## Execute body within a transaction with automatic rollback on error.
  ## The body must set `result` to indicate success/failure.
  ## On success, commits. On failure or exception, rolls back.
  block:
    ?db.beginStmt.exec()
    var committed = false

    defer:
      if not committed:
        discard db.rollbackStmt.exec()

    body

    ?db.endStmt.exec()
    committed = true
    success()

# =============================================================================
# Sync Operations
# =============================================================================

proc hasSync*(db: SQLiteDsDb, keyId: string): ?!bool {.gcsafe.} =
  ## Synchronous check if key exists
  var exists = false
  proc onRow(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  discard ?db.containsStmt.query((keyId), onRow)
  success exists

proc getSync*(db: SQLiteDsDb, key: Key): ?!RawRecord {.gcsafe.} =
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

proc getManySync*(db: SQLiteDsDb, keys: seq[Key]): ?!seq[RawRecord] {.gcsafe.} =
  ## Synchronous get multiple records.
  ## Automatically chunks large batches to stay within SQLite parameter limits.
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

    records.add(
      RawRecord.init(Key.init(keyId).expect("Invalid key from DB"), value, token.uint64)
    )

  # Chunk keys to stay within SQLite parameter limits
  var offset = 0
  while offset < keys.len:
    let
      chunkSize = min(MaxSqliteParams, keys.len - offset)
      chunk = keys[offset ..< offset + chunkSize]
      queryStr = makeGetManyParamQuery(chunk.len)
      keyIds = chunk.mapIt(it.id)

    discard ?db.env.queryWithStrings(queryStr, keyIds, onRow)
    offset += chunkSize

  success records

proc putSync*(
    db: SQLiteDsDb, records: seq[RawRecord], readOnly: bool
): ?!seq[Key] {.gcsafe.} =
  ## Synchronous put records
  ?checkWritable(readOnly)

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
      return failure(
        newCorruptionError(
          "Multiple rows affected by CAS operation on key: " & record.key.id
        )
      )

    if not changed:
      skipped.add(record.key)

  ?db.endStmt.exec()
  committed = true

  success skipped

proc deleteSync*(
    db: SQLiteDsDb, records: seq[KeyRecord], readOnly: bool
): ?!seq[Key] {.gcsafe.} =
  ## Synchronous delete records.
  ## Automatically chunks large batches to stay within SQLite parameter limits.
  ?checkWritable(readOnly)

  if records.len == 0:
    return success(newSeq[Key]())

  var
    deletedIds = initHashSet[string]()
    inTransaction = false
    committed = false
    totalChanges = 0

  ?db.beginStmt.exec()
  inTransaction = true

  defer:
    if inTransaction and not committed:
      discard db.rollbackStmt.exec()

  proc onRow(s: RawStmtPtr) =
    deletedIds.incl($sqlite3_column_text_not_null(s, 0.cint))

  # Chunk records to stay within SQLite parameter limits (2 params per record)
  var offset = 0
  while offset < records.len:
    let
      chunkSize = min(MaxDeleteChunkSize, records.len - offset)
      chunk = records[offset ..< offset + chunkSize]
      queryStr = makeDeleteManyParamQuery(chunk.len) & " RETURNING " & IdColName
      pairs = chunk.mapIt((it.key.id, ?boundedToken(it.token)))

    discard ?db.env.queryWithIdVersionPairs(queryStr, pairs, onRow)

    let changes = ?db.checkChanges()
    totalChanges += changes
    offset += chunkSize

  if totalChanges > records.len:
    return failure(
      newCorruptionError(
        "Delete affected more rows (" & $totalChanges & ") than records (" & $records.len &
          ")"
      )
    )

  ?db.endStmt.exec()
  committed = true

  let skipped = records.filterIt(it.key.id notin deletedIds).mapIt(it.key)
  success skipped

proc putAtomicSync*(
    db: SQLiteDsDb, records: seq[RawRecord], readOnly: bool
): ?!seq[Key] {.gcsafe.} =
  ## Synchronous all-or-nothing batch put
  ?checkWritable(readOnly)

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
    var
      exists = false
      currentToken = 0'i64

    proc onRow(s: RawStmtPtr) =
      exists = true
      currentToken = versionCol(s, GetSingleStmtVersionCol)()

    discard ?db.getSingleStmt.query((record.key.id), onRow)

    let conflict =
      if record.token == 0:
        exists # Insert-only but key exists
      elif not exists:
        true # Update expected but key missing
      else:
        currentToken != record.token.int64 # Token mismatch

    if conflict:
      conflicts.add(record.key)

  # If any conflicts, rollback and return them
  if conflicts.len > 0:
    ?db.rollbackStmt.exec()
    committed = true # Prevent defer rollback
    return success conflicts

  # Second pass: apply ALL writes (no conflicts)
  for record in records:
    proc onRow(s: RawStmtPtr) =
      discard # Consume RETURNING clause

    if record.token == 0:
      discard ?db.insertStmt.query((record.key.id, record.val, stamp), onRow)
    else:
      let token = ?boundedToken(record.token)
      discard ?db.updateStmt.query((record.val, stamp, record.key.id, token), onRow)

  ?db.endStmt.exec()
  committed = true
  success newSeq[Key]()

proc deleteAtomicSync*(
    db: SQLiteDsDb, records: seq[KeyRecord], readOnly: bool
): ?!seq[Key] {.gcsafe.} =
  ## Synchronous all-or-nothing batch delete
  ?checkWritable(readOnly)

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
    var
      exists = false
      currentToken = 0'i64

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
      discard # Consume the returned row

    discard ?db.deleteStmt.query((record.key.id, token), onRow)

  ?db.endStmt.exec()
  committed = true
  success newSeq[Key]()

# =============================================================================
# Query Operations
# =============================================================================

proc buildQueryString*(query: Query): string =
  ## Build the SQL query string based on query options.
  result = if query.value: QueryStmtDataIdStr else: QueryStmtIdStr

  if query.sort == SortOrder.Descending:
    result &= QueryStmtOrderDescending
  else:
    result &= QueryStmtOrderAscending

  if query.limit != 0:
    result &= QueryStmtLimit

  if query.offset != 0:
    result &= QueryStmtOffset

proc prepareQueryStmt*(env: SQLite, query: Query): ?!RawStmtPtr {.gcsafe.} =
  ## Prepare and bind a query statement.
  let
    queryStr = buildQueryString(query)
    queryStmt = QueryStmt.prepare(env, queryStr).valueOr:
      return failure(newException(KVStoreError, "Query prepare failed: " & error.msg))
    s = RawStmtPtr(queryStmt)

  var v = sqlite3_bind_text(
    s, 1.cint, (query.key.id & "*").cstring, -1.cint, SQLITE_TRANSIENT_GCSAFE
  )

  if v != SQLITE_OK:
    discard sqlite3_finalize(s)
    return failure(newException(KVStoreError, $sqlite3_errstr(v)))

  if query.limit != 0:
    v = sqlite3_bind_int(s, 2.cint, query.limit.cint)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s)
      return failure(newException(KVStoreError, $sqlite3_errstr(v)))

  if query.offset != 0:
    v = sqlite3_bind_int(s, 3.cint, query.offset.cint)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s)
      return failure(newException(KVStoreError, $sqlite3_errstr(v)))

  success s

proc nextSync*(stmt: RawStmtPtr, queryValue: bool): ?!Option[RawRecord] {.gcsafe.} =
  ## Step through query results and return the next record.
  ## Returns none when iteration is complete.
  let v = sqlite3_step(stmt)

  case v
  of SQLITE_ROW:
    let keyStr = $sqlite3_column_text_not_null(stmt, QueryStmtIdCol)
    let key = Key.init(keyStr).valueOr:
      return failure(newException(KVStoreError, "Invalid key in database: " & keyStr))

    var data: seq[byte] = @[]

    if queryValue:
      let blob = sqlite3_column_blob(stmt, QueryStmtDataCol)

      # detect out-of-memory error
      if blob.isNil:
        let errCode = sqlite3_errcode(sqlite3_db_handle(stmt))
        if not (errCode in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
          return failure(newException(KVStoreError, $sqlite3_errstr(errCode)))

      let dataLen = sqlite3_column_bytes(stmt, QueryStmtDataCol)
      if not blob.isNil:
        data = @(toOpenArray(cast[ptr UncheckedArray[byte]](blob), 0, dataLen - 1))

    let
      versionCol =
        if queryValue: QueryStmtVersionColWithData else: QueryStmtVersionColNoData
      version = sqlite3_column_int64(stmt, versionCol.cint).uint64

    success some(RawRecord.init(key, data, version))
  of SQLITE_DONE:
    success none(RawRecord)
  else:
    failure(newException(KVStoreError, $sqlite3_errstr(v)))

proc disposeStmtSync*(stmt: RawStmtPtr): ?!void {.gcsafe.} =
  ## Finalize a prepared statement.
  discard sqlite3_finalize(stmt)
  success()
