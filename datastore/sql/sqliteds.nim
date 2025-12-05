{.push raises: [].}

import std/times
import std/sequtils
import std/options
import std/sets

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi

import ../key
import ../query
import ../datastore
import ./sqlitedsdb
import ./sqliteutils

export sqlitedsdb

type SQLiteDatastore* = ref object of Datastore
  readOnly: bool
  db: SQLiteDsDb

proc path*(self: SQLiteDatastore): string =
  self.db.dbPath

proc `readOnly=`*(
  self: SQLiteDatastore
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

proc ensureWritable(self: SQLiteDatastore): ?!void =
  if self.readOnly:
    return failure(newBackendError("SQLite datastore opened read-only"))
  success()

proc boundedToken(token: uint64): ?!int64 =
  if token > uint64(high(int64)):
    return failure(newCorruptionError("SQLite token overflow"))
  success token.int64

method has*(
    self: SQLiteDatastore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  var exists = false
  proc onRow(s: RawStmtPtr) =
    exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  if err =? self.db.containsStmt.query((key.id), onRow).errorOption:
    return failure(err)

  return success exists

method get*(
    self: SQLiteDatastore, key: Key
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
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

  if err =? self.db.getSingleStmt.query((key.id), onRow).errorOption:
    return failure(err)

  if not rowFound:
    return failure(newException(DatastoreKeyNotFound, "Key doesn't exist"))

  return success RawRecord.init(key, value, token.uint64)

method get*(
    self: SQLiteDatastore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  var records: seq[RawRecord]

  proc onRow(s: RawStmtPtr) =
    let
      key = idCol(s, GetManyStmtIdCol)()
      value = dataCol(s, GetManyStmtDataCol)()
      token = versionCol(s, GetManyStmtVersionCol)()

    if token < 0:
      raiseAssert("Negative token detected")

    records.add(RawRecord.init(?Key.init(key), value, token.uint64))

  let queryStr = ?makeGetManyStmStr(keys.mapIt(it.id))
  if err =? self.db.env.query(queryStr, onRow).errorOption:
    return failure err

  return success records

method put*(
    self: SQLiteDatastore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  if err =? self.ensureWritable().errorOption:
    return failure(err)

  var
    skipped: seq[Key]
    stamp = timestamp()

  ?self.db.beginStmt.exec() # begin transaction
  for record in records:
    var record = record
    var changed = false

    proc onRow(s: RawStmtPtr) =
      changed = true

    if err =?
        self.db.upsertStmt.query(
          (record.key.id, record.val, (?boundedToken(record.token)), stamp), onRow
        ).errorOption:
      ?self.db.rollbackStmt.exec() # revert transaction
      return failure(err)

    if not changed:
      skipped.add(record.key)

  ?self.db.endStmt.exec() # commit transaction

  return success skipped

method delete*(
    self: SQLiteDatastore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ?self.ensureWritable()

  if records.len == 0:
    return success(newSeq[Key]())

  var deletedIds = initHashSet[string]()

  # Build dynamic DELETE statement with all (id, version) pairs and RETURNING
  let queryStr = ?makeDeleteManyStmStr(records.mapIt((it.key.id, it.token)))
  let queryWithReturning = queryStr & " RETURNING " & IdColName

  ?self.db.beginStmt.exec()

  # Execute delete and track what was actually deleted via RETURNING
  proc onRow(s: RawStmtPtr) =
    deletedIds.incl($sqlite3_column_text_not_null(s, 0.cint))

  if err =? self.db.env.query(queryWithReturning, onRow).errorOption:
    ?self.db.rollbackStmt.exec()
    return failure err

  ?self.db.endStmt.exec()

  # Find skipped keys (those not in deletedIds)
  var skipped: seq[Key]
  for record in records:
    if record.key.id notin deletedIds:
      skipped.add(record.key)

  return success skipped

method close*(
    self: SQLiteDatastore
): Future[?!void] {.async: (raises: [CancelledError]).} =
  self.db.close()
  return success()

method query*(
    self: SQLiteDatastore, query: Query
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
    return failure newException(DatastoreError, $sqlite3_errstr(v))

  if query.limit != 0:
    v = sqlite3_bind_int(s, 2.cint, query.limit.cint)

    if v != SQLITE_OK:
      return failure newException(DatastoreError, $sqlite3_errstr(v))

  if query.offset != 0:
    v = sqlite3_bind_int(s, 3.cint, query.offset.cint)

    if v != SQLITE_OK:
      return failure newException(DatastoreError, $sqlite3_errstr(v))

  var finished = false
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
          return failure newException(DatastoreError, $sqlite3_errstr(v))

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
      return failure newException(DatastoreError, $sqlite3_errstr(v))

  proc isFinished(): bool =
    finished

  proc dispose() =
    discard sqlite3_reset(s)
    discard sqlite3_clear_bindings(s)

  return success QueryIter.new(next, isFinished, dispose)

proc new*(T: type SQLiteDatastore, path: string, readOnly = false): ?!T =
  let flags =
    if readOnly:
      SQLITE_OPEN_READONLY
    else:
      SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE

  success T(db: ?SQLiteDsDb.open(path, flags), readOnly: readOnly)

proc new*(T: type SQLiteDatastore, db: SQLiteDsDb): ?!T =
  success T(db: db, readOnly: db.readOnly)
