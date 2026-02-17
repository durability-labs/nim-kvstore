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
import ../taskutils
import ./sqlitedsdb
import ./sqliteutils

const
  # SQLite >= 3.32.0 raised SQLITE_MAX_VARIABLE_NUMBER from 999 to 32766.
  # Use 80% of the limit to leave headroom.
  MaxSqliteParams* = 26212
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
    return failure(newException(KVStoreCorruption, "SQLite token overflow"))
  success token.int64

proc checkWritable*(readOnly: bool): ?!void =
  if readOnly:
    return failure(newException(KVStoreBackendError, "SQLite store opened read-only"))
  success()

proc upsertVersion*(token: uint64): ?!int64 =
  ## Convert a CAS token to the version value for the batch upsert VALUES clause.
  ## For token=0 (insert): version=1
  ## For token!=0 (update): version=token+1 (must fit in int64)
  if token >= uint64(high(int64)):
    return failure(newException(KVStoreCorruption, "SQLite token overflow on upsert"))
  success((token + 1).int64)

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

proc hasManySync*(db: SQLiteDsDb, keys: seq[Key]): ?!seq[Key] {.gcsafe.} =
  ## Synchronous check for multiple keys.
  ## Returns the subset of input keys that exist in the store, in input order.
  ## Duplicate keys in input are deduplicated (first occurrence wins).
  ## Automatically chunks large batches to stay within SQLite parameter limits.
  if keys.len == 0:
    return success(newSeq[Key]())

  # Build unique IDs list in first-seen order (deduplicates query params)
  let uniqueIds = keys.deduplicate()

  # Collect ids that exist in DB
  var foundIds = initHashSet[Key]()
  proc onRow(s: RawStmtPtr) =
    let keyId = idCol(s, HasManyStmtIdCol)()
    foundIds.incl(Key.init(keyId).expect("Invalid key from DB"))

  # Chunk unique IDs to stay within SQLite parameter limits
  var offset = 0
  while offset < uniqueIds.len:
    let
      chunkSize = min(MaxSqliteParams, uniqueIds.len - offset)
      chunk = uniqueIds[offset ..< offset + chunkSize]
      queryStr = makeHasManyParamQuery(chunk.len)

    discard ?db.env.queryWithStrings(queryStr, chunk.mapIt($it), onRow)
    offset += chunkSize

  success uniqueIds.filterIt(it in foundIds)

proc getSync*(db: SQLiteDsDb, key: Key): ?!RawKVRecord {.gcsafe.} =
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

  success RawKVRecord.init(key, value, token.uint64)

proc getManySync*(db: SQLiteDsDb, keys: seq[Key]): ?!seq[RawKVRecord] {.gcsafe.} =
  ## Synchronous get multiple records.
  ## Automatically chunks large batches to stay within SQLite parameter limits.
  if keys.len == 0:
    return success(newSeq[RawKVRecord]())

  var records: seq[RawKVRecord]

  proc onRow(s: RawStmtPtr) =
    let
      keyId = idCol(s, GetManyStmtIdCol)()
      value = dataCol(s, GetManyStmtDataCol)()
      token = versionCol(s, GetManyStmtVersionCol)()

    if token < 0:
      raiseAssert("Negative token detected")

    records.add(
      RawKVRecord.init(
        Key.init(keyId).expect("Invalid key from DB"), value, token.uint64
      )
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
    db: SQLiteDsDb, records: seq[RawKVRecord], readOnly: bool
): ?!seq[Key] {.gcsafe.} =
  ## Synchronous put records using a single-statement batch upsert.
  ## All records (inserts and updates) are handled in one CTE-based
  ## INSERT ... ON CONFLICT DO UPDATE statement per chunk.
  ?checkWritable(readOnly)

  if records.len == 0:
    return success(newSeq[Key]())

  let
    maxPerChunk = MaxSqliteParams div BatchUpsertParamsPerRow
    stamp = timestamp()

  var
    affectedIds = initHashSet[string]()
    inTransaction = false
    committed = false

  ?db.beginStmt.exec()
  inTransaction = true

  defer:
    if inTransaction and not committed:
      discard db.rollbackStmt.exec()

  var offset = 0
  while offset < records.len:
    let
      chunkSize = min(maxPerChunk, records.len - offset)
      chunk = records[offset ..< offset + chunkSize]
      queryStr = makeBatchUpsertQuery(chunkSize)
      params = chunk.mapIt((it.key.id, it.val, ?upsertVersion(it.token), stamp))

    proc onRow(s: RawStmtPtr) =
      affectedIds.incl($sqlite3_column_text_not_null(s, BatchUpsertReturnIdCol.cint))

    discard ?db.env.queryWithUpsertRecords(queryStr, params, onRow)

    let changes = ?db.checkChanges()
    if changes > chunkSize:
      return failure(
        newException(
          KVStoreCorruption,
          "Batch upsert affected more rows (" & $changes & ") than records (" &
            $chunkSize & ")",
        )
      )

    offset += chunkSize

  ?db.endStmt.exec()
  committed = true

  # Any record not in the affected set was skipped
  let skipped = records.filterIt(it.key.id notin affectedIds).mapIt(it.key)
  success skipped

proc deleteSync*(
    db: SQLiteDsDb, records: seq[KeyKVRecord], readOnly: bool
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
      newException(
        KVStoreCorruption,
        "Delete affected more rows (" & $totalChanges & ") than records (" & $records.len &
          ")",
      )
    )

  ?db.endStmt.exec()
  committed = true

  let skipped = records.filterIt(it.key.id notin deletedIds).mapIt(it.key)
  success skipped

proc putAtomicSync*(
    db: SQLiteDsDb, records: seq[RawKVRecord], readOnly: bool
): ?!seq[Key] {.gcsafe.} =
  ## Synchronous all-or-nothing batch put using a single-statement batch upsert.
  ## Any conflict (insert or update) rolls back the entire transaction.
  ?checkWritable(readOnly)

  if records.len == 0:
    return success(newSeq[Key]())

  let
    maxPerChunk = MaxSqliteParams div BatchUpsertParamsPerRow
    stamp = timestamp()

  var
    affectedIds = initHashSet[string]()
    inTransaction = false
    committed = false

  ?db.beginStmt.exec()
  inTransaction = true

  defer:
    if inTransaction and not committed:
      discard db.rollbackStmt.exec()

  var offset = 0
  while offset < records.len:
    let
      chunkSize = min(maxPerChunk, records.len - offset)
      chunk = records[offset ..< offset + chunkSize]
      queryStr = makeBatchUpsertQuery(chunkSize)
      params = chunk.mapIt((it.key.id, it.val, ?upsertVersion(it.token), stamp))

    proc onRow(s: RawStmtPtr) =
      affectedIds.incl($sqlite3_column_text_not_null(s, BatchUpsertReturnIdCol.cint))

    discard ?db.env.queryWithUpsertRecords(queryStr, params, onRow)
    offset += chunkSize

  # Any record not in the affected set is a conflict
  let conflicts = records.filterIt(it.key.id notin affectedIds).mapIt(it.key)

  if conflicts.len > 0:
    ?db.rollbackStmt.exec()
    committed = true # Prevent defer rollback
    return success conflicts

  ?db.endStmt.exec()
  committed = true
  success newSeq[Key]()

proc deleteAtomicSync*(
    db: SQLiteDsDb, records: seq[KeyKVRecord], readOnly: bool
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
    queryStmt =
      ?QueryStmt.prepare(env, queryStr).toKVError(context = "Query prepare failed")
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

proc nextSync*(stmt: RawStmtPtr, queryValue: bool): ?!Option[RawKVRecord] {.gcsafe.} =
  ## Step through query results and return the next record.
  ## Returns none when iteration is complete.
  let v = sqlite3_step(stmt)

  case v
  of SQLITE_ROW:
    let keyStr = $sqlite3_column_text_not_null(stmt, QueryStmtIdCol)
    let key =
      ?Key.init(keyStr).toKVError(context = "Invalid key in database: " & keyStr)

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

    success some(RawKVRecord.init(key, data, version))
  of SQLITE_DONE:
    success none(RawKVRecord)
  else:
    failure(newException(KVStoreError, $sqlite3_errstr(v)))

# =============================================================================
# Move Operations
# =============================================================================

proc moveSync*(
    db: SQLiteDsDb, oldPrefix, newPrefix: Key, readOnly: bool
): ?!seq[Key] {.gcsafe.} =
  ## Move all records from oldPrefix/* to newPrefix/* using a single
  ## UPDATE statement. A single statement in SQLite autocommit mode is
  ## already atomic, so this is used by both moveKeysImpl and
  ## moveKeysAtomicImpl.
  ##
  ## Matches both prefix children (GLOB prefix/*) and the prefix key
  ## itself (id = prefix).
  ##
  ## Fails on UNIQUE constraint if any destination key already exists.
  ## Returns empty seq on success (SQL UPDATE has no partial failures).
  ##
  ?checkWritable(readOnly)

  let
    globPattern = oldPrefix.id & "/*"
    exactKey = oldPrefix.id
    # SQLite SUBSTR is 1-based; skip oldPrefix chars, keep separator + rest
    substrOffset = (oldPrefix.id.len + 1).int64
    newPrefixStr = newPrefix.id

  proc onRow(s: RawStmtPtr) =
    discard # Consume RETURNING clause

  discard ?db.moveStmt.query((newPrefixStr, substrOffset, globPattern, exactKey), onRow)
  success(newSeq[Key]())

proc moveSyncMulti*(
    db: SQLiteDsDb, moves: seq[(Key, Key)], readOnly: bool
): ?!seq[Key] {.gcsafe.} =
  ## Move multiple prefix pairs atomically in a single transaction.
  ##
  ## Each pair moves all records from oldPrefix/* to newPrefix/*
  ## (including the prefix key itself). All moves succeed or all
  ## are rolled back.
  ##
  ## Fails on UNIQUE constraint if any destination key already exists.
  ##
  ?checkWritable(readOnly)

  if moves.len == 0:
    return success(newSeq[Key]())

  if moves.len == 1:
    return moveSync(db, moves[0][0], moves[0][1], readOnly)

  var committed = false
  ?db.beginStmt.exec()
  defer:
    if not committed:
      discard db.rollbackStmt.exec()

  for (oldPrefix, newPrefix) in moves:
    let
      globPattern = oldPrefix.id & "/*"
      exactKey = oldPrefix.id
      substrOffset = (oldPrefix.id.len + 1).int64
      newPrefixStr = newPrefix.id

    proc onRow(s: RawStmtPtr) =
      discard

    discard ?db.moveStmt.query(
      (newPrefixStr, substrOffset, globPattern, exactKey), onRow
    )

  ?db.endStmt.exec()
  committed = true
  success(newSeq[Key]())

# =============================================================================
# Statement Disposal
# =============================================================================

proc disposeStmtSync*(stmt: RawStmtPtr): ?!void {.gcsafe.} =
  ## Finalize a prepared statement.
  discard sqlite3_finalize(stmt)
  success()
