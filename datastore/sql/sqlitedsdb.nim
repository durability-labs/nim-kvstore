{.push raises: [].}

import std/os
import std/strformat
import std/strutils
import std/sequtils

import pkg/questionable
import pkg/questionable/results

import ./sqliteutils

type
  BoundIdCol* = proc(): string {.closure, gcsafe, raises: [].}
  BoundVersionCol* = proc(): int64 {.closure, gcsafe, raises: [].}
  BoundDataCol* = proc(): seq[byte] {.closure, gcsafe, raises: [].}
  BoundTimestampCol* = proc(): int64 {.closure, gcsafe, raises: [].}

  # feels odd to use `void` for prepared statements corresponding to SELECT
  # queries but it fits with the rest of the SQLite wrapper adapted from
  # status-im/nwaku, at least in its current form in ./sqlite
  ContainsStmt* = SQLiteStmt[(string), void]
  QueryStmt* = SQLiteStmt[(string), void]
  GetSingleStmt* = SQLiteStmt[(string), void]
  UpsertStmt* = SQLiteStmt[(string, seq[byte], int64, int64), void]
  DeleteStmt* = SQLiteStmt[(string, int64), void]
  GetChangesStmt* = NoParamsStmt
  BeginStmt* = NoParamsStmt
  EndStmt* = NoParamsStmt
  RollbackStmt* = NoParamsStmt

  SQLiteDsDb* = object
    readOnly*: bool
    dbPath*: string
    env*: SQLite
    containsStmt*: ContainsStmt
    getSingleStmt*: GetSingleStmt
    upsertStmt*: UpsertStmt
    deleteStmt*: DeleteStmt
    getChangesStmt*: GetChangesStmt
    beginStmt*: BeginStmt
    endStmt*: EndStmt
    rollbackStmt*: RollbackStmt

const
  DbExt* = ".sqlite3"
  TableName* = "Store"

  IdColName* = "id"
  DataColName* = "data"
  VersionColName* = "version"
  TimestampColName* = "timestamp"

  IdColType = "TEXT"
  DataColType = "BLOB"
  VersionColType = "INTEGER"
  TimestampColType = "INTEGER"

  Memory* = ":memory:"

  # https://stackoverflow.com/a/9756276
  # EXISTS returns a boolean value represented by an integer:
  # https://sqlite.org/datatype3.html#boolean_datatype
  # https://sqlite.org/lang_expr.html#the_exists_operator
  ContainsStmtStr* =
    fmt"""
    SELECT EXISTS(
      SELECT 1 FROM {TableName}
      WHERE {IdColName} = ?
    )
  """

  ContainsStmtExistsCol* = 0

  CreateStmtStr* =
    fmt"""
    CREATE TABLE IF NOT EXISTS {TableName} (
      {IdColName} {IdColType} NOT NULL PRIMARY KEY,
      {DataColName} {DataColType},
      {VersionColName} {VersionColType} NOT NULL,
      {TimestampColName} {TimestampColType} NOT NULL
    ) WITHOUT ROWID;
  """

  QueryStmtIdStr* =
    fmt"""
    SELECT {IdColName}, {VersionColName} FROM {TableName}
        WHERE {IdColName} GLOB ?
  """

  QueryStmtDataIdStr* =
    fmt"""
    SELECT {IdColName}, {DataColName}, {VersionColName} FROM {TableName}
        WHERE {IdColName} GLOB ?
  """

  QueryStmtOffset* =
    """
    OFFSET ?
  """

  QueryStmtLimit* =
    """
    LIMIT ?
  """

  QueryStmtOrderAscending* =
    fmt"""
    ORDER BY {IdColName} ASC
  """

  QueryStmtOrderDescending* =
    fmt"""
    ORDER BY {IdColName} DESC
  """

  GetSingleStmtDataCol* = 0
  GetSingleStmtVersionCol* = 1

  # NOTE: This SELECT is left as an oprimization, it could be done with GetManyStmtStr
  GetSingleStmtStr* =
    fmt"""
    SELECT {DataColName}, {VersionColName} FROM {TableName}
    WHERE {IdColName} = ?
  """

  GetManyStmtIdCol* = 0
  GetManyStmtDataCol* = 1
  GetManyStmtVersionCol* = 2

  # NOTE: This statment is not prepared, it is contructed dynamicaly
  GetManyStmtStr* =
    fmt"""
    SELECT {IdColName}, {DataColName}, {VersionColName} FROM {TableName}
    WHERE {IdColName} IN ($1)
  """

  UpsertStmtDataCol* = 0
  UpsertStmtVersionCol* = 1

  UpsertStmtStr* =
    fmt"""
    INSERT INTO {TableName}
    (
      {IdColName},
      {DataColName},
      {VersionColName},
      {TimestampColName}
    )
    VALUES (?, ?, ? + 1, ?)
    ON CONFLICT({IdColName}) DO UPDATE
    SET
      {DataColName} = excluded.{DataColName},
      {VersionColName} = excluded.{VersionColName},
      {TimestampColName} = excluded.{TimestampColName}
    WHERE
      {TableName}.{IdColName} = excluded.{IdColName} AND
      {TableName}.{VersionColName} = excluded.{VersionColName} - 1
    RETURNING {DataColName}, {VersionColName}
  """

  DeleteStmtStr* =
    fmt"""
    DELETE FROM {TableName}
    WHERE {IdColName} = ? AND {VersionColName} = ?
    RETURNING 1
  """

  DeleteManyStmtStr* =
    fmt"""
    DELETE FROM {TableName}
    WHERE ({IdColName}, {VersionColName}) IN ($1)
  """

  GetChangesStmtStr* =
    fmt"""
    SELECT changes()
  """

  BeginTransactionStr* =
    """
    BEGIN;
  """

  EndTransactionStr* =
    """
    END;
  """

  RollbackTransactionStr* =
    """
    ROLLBACK;
  """

  QueryStmtIdCol* = 0
  QueryStmtDataCol* = 1
  QueryStmtVersionColNoData* = 1  # When value=false: id=0, version=1
  QueryStmtVersionColWithData* = 2  # When value=true: id=0, data=1, version=2

template makeGetManyStmStr*(ids: openArray[string]): ?!string =
  catch (GetManyStmtStr % ids.mapIt("\"" & it & "\"").join(", "))

template makeDeleteManyStmStr*(ids: openArray[(string, uint64)]): ?!string =
  catch (
    DeleteManyStmtStr % ids.mapIt("(\"" & it[0] & "\", " & $it[1] & ")").join(", ")
  )

proc checkColMetadata(s: RawStmtPtr, i: int, expectedName: string) =
  let colName = sqlite3_column_origin_name(s, i.cint)

  if colName.isNil:
    raise (ref Defect)(
      msg: "no column exists for index " & $i & " in `" & $sqlite3_sql(s) & "`"
    )

  if $colName != expectedName:
    raise (ref Defect)(
      msg:
        "original column name for index " & $i & " was \"" & $colName & "\" in `" &
        $sqlite3_sql(s) & "` but callee expected \"" & expectedName & "\""
    )

proc idCol*(s: RawStmtPtr, index: int): BoundIdCol =
  checkColMetadata(s, index, IdColName)

  return proc(): string =
    $sqlite3_column_text_not_null(s, index.cint)

proc dataCol*(s: RawStmtPtr, index: int): BoundDataCol =
  checkColMetadata(s, index, DataColName)

  return proc(): seq[byte] =
    let
      i = index.cint
      blob = sqlite3_column_blob(s, i)

    # detect out-of-memory error
    # see the conversion table and final paragraph of:
    # https://www.sqlite.org/c3ref/column_blob.html
    # see also https://www.sqlite.org/rescode.html

    # the "data" column can be NULL so in order to detect an out-of-memory error
    # it is necessary to check that the result is a null pointer and that the
    # result code is an error code
    if blob.isNil:
      let v = sqlite3_errcode(sqlite3_db_handle(s))

      if not (v in [SQLITE_OK, SQLITE_ROW, SQLITE_DONE]):
        raise (ref Defect)(msg: $sqlite3_errstr(v))

    let
      dataLen = sqlite3_column_bytes(s, i)
      dataBytes = cast[ptr UncheckedArray[byte]](blob)

    @(toOpenArray(dataBytes, 0, dataLen - 1))

proc timestampCol*(s: RawStmtPtr, index: int): BoundTimestampCol =
  checkColMetadata(s, index, TimestampColName)

  return proc(): int64 =
    sqlite3_column_int64(s, index.cint)

proc versionCol*(s: RawStmtPtr, index: int): BoundVersionCol =
  checkColMetadata(s, index, VersionColName)

  return proc(): int64 =
    sqlite3_column_int64(s, index.cint)

proc changesCol*(s: RawStmtPtr, index: int): BoundVersionCol =
  return proc(): int64 =
    sqlite3_column_int64(s, index.cint)

proc getDBFilePath*(path: string): ?!string =
  try:
    let
      (parent, name, ext) = path.normalizePathEnd.splitFile
      dbExt = if ext == "": DbExt else: ext
      absPath =
        if parent.isAbsolute:
          parent
        else:
          getCurrentDir() / parent
      dbPath = absPath / name & dbExt

    return success dbPath
  except CatchableError as exc:
    return failure(exc.msg)

proc checkChanges*(self: SQLiteDsDb): ?!int =
  var changes = 0
  proc onChanges(s: RawStmtPtr) =
    changes = changesCol(s, 0)()

  if err =? self.getChangesStmt.query((), onChanges).errorOption:
    return failure(err)

  return success changes

proc close*(self: var SQLiteDsDb) =
  var env: AutoDisposed[SQLite]

  defer:
    disposeIfUnreleased(env)

  env.val = self.env

  if not RawStmtPtr(self.containsStmt).isNil:
    self.containsStmt.dispose

  if not RawStmtPtr(self.beginStmt).isNil:
    self.beginStmt.dispose

  if not RawStmtPtr(self.endStmt).isNil:
    self.endStmt.dispose

  if not RawStmtPtr(self.rollbackStmt).isNil:
    self.rollbackStmt.dispose

  if not RawStmtPtr(self.getSingleStmt).isNil:
    self.getSingleStmt.dispose

  if not RawStmtPtr(self.upsertStmt).isNil:
    self.upsertStmt.dispose

  if not RawStmtPtr(self.deleteStmt).isNil:
    self.deleteStmt.dispose

  if not RawStmtPtr(self.getChangesStmt).isNil:
    self.getChangesStmt.dispose

proc open*(
    _: type SQLiteDsDb, path = Memory, flags = SQLITE_OPEN_READONLY
): ?!SQLiteDsDb =
  # make it optional to enable WAL with it enabled being the default?

  # make it possible to specify a custom page size?
  # https://www.sqlite.org/pragma.html#pragma_page_size
  # https://www.sqlite.org/intern-v-extern-blob.html

  var env: AutoDisposed[SQLite]

  defer:
    disposeIfUnreleased(env)

  let
    isMemory = path == Memory
    absPath =
      if isMemory:
        Memory
      else:
        ?path.getDBFilePath
    readOnly = (SQLITE_OPEN_READONLY and flags).bool

  if not isMemory:
    if readOnly and not fileExists(absPath):
      return failure "read-only database does not exist: " & absPath
    elif not dirExists(absPath.parentDir):
      return failure "directory does not exist: " & absPath

  open(absPath, env.val, flags)

  var pragmaStmt = journalModePragmaStmt(env.val)

  checkExec(pragmaStmt)

  var
    containsStmt: ContainsStmt
    getSingleStmt: GetSingleStmt
    upsertStmt: UpsertStmt
    deleteStmt: DeleteStmt
    getChangesStmt: GetChangesStmt
    beginStmt: BeginStmt
    endStmt: EndStmt
    rollbackStmt: RollbackStmt

  if not readOnly:
    checkExec(env.val, CreateStmtStr)

    upsertStmt = ?UpsertStmt.prepare(env.val, UpsertStmtStr, SQLITE_PREPARE_PERSISTENT)

    deleteStmt = ?DeleteStmt.prepare(env.val, DeleteStmtStr, SQLITE_PREPARE_PERSISTENT)

    getChangesStmt =
      ?GetChangesStmt.prepare(env.val, GetChangesStmtStr, SQLITE_PREPARE_PERSISTENT)

  beginStmt =
    ?BeginStmt.prepare(env.val, BeginTransactionStr, SQLITE_PREPARE_PERSISTENT)

  endStmt = ?EndStmt.prepare(env.val, EndTransactionStr, SQLITE_PREPARE_PERSISTENT)

  rollbackStmt =
    ?RollbackStmt.prepare(env.val, RollbackTransactionStr, SQLITE_PREPARE_PERSISTENT)

  containsStmt =
    ?ContainsStmt.prepare(env.val, ContainsStmtStr, SQLITE_PREPARE_PERSISTENT)

  getSingleStmt =
    ?GetSingleStmt.prepare(env.val, GetSingleStmtStr, SQLITE_PREPARE_PERSISTENT)

  success SQLiteDsDb(
    readOnly: readOnly,
    dbPath: path,
    env: env.release,
    containsStmt: containsStmt,
    getSingleStmt: getSingleStmt,
    upsertStmt: upsertStmt,
    deleteStmt: deleteStmt,
    getChangesStmt: getChangesStmt,
    beginStmt: beginStmt,
    endStmt: endStmt,
    rollbackStmt: rollbackStmt,
  )
