{.push raises: [].}

import std/os
import std/sequtils
import std/strformat
import std/strutils

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
  MoveStmt* = SQLiteStmt[(string, int64, string, string), void]

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
    moveStmt*: MoveStmt
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

  SqliteMemory* = ":memory:"

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
      {VersionColName} {VersionColType} NOT NULL CHECK({VersionColName} >= 1),
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

  # NOTE: This statement is not prepared, it is constructed dynamically
  # with parameterized placeholders for safety
  GetManyStmtStr* =
    fmt"""
    SELECT {IdColName}, {DataColName}, {VersionColName} FROM {TableName}
    WHERE {IdColName} IN ($1)
  """

  # NOTE: Similar to GetMany but only returns id for existence check
  HasManyStmtStr* =
    fmt"""
    SELECT {IdColName} FROM {TableName}
    WHERE {IdColName} IN ($1)
  """

  HasManyStmtIdCol* = 0




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

  # Move/rename keys with a given prefix
  # Matches both prefix children (GLOB prefix/*) and the prefix key itself (id = prefix)
  MoveStmtStr* =
    fmt"""
    UPDATE {TableName}
    SET {IdColName} = ? || SUBSTR({IdColName}, ?)
    WHERE ({IdColName} GLOB ? OR {IdColName} = ?)
    RETURNING {IdColName}, {VersionColName}
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
  QueryStmtVersionColNoData* = 1 # When value=false: id=0, version=1
  QueryStmtVersionColWithData* = 2 # When value=true: id=0, data=1, version=2

  # Batch upsert: 4 params per row (id, data, version, timestamp)
  # version = 1 for inserts (token=0), version = token+1 for updates (token!=0)
  BatchUpsertParamsPerRow* = 4
  BatchUpsertReturnIdCol* = 0

  # CTE-based upsert:
  # - Inserts (version=1): inserted directly; conflict -> silently skipped
  #   (WHERE Store.version = 0 never matches since CHECK(version >= 1))
  # - Updates (version>1): only attempted if key EXISTS; conflict triggers
  #   CAS check via WHERE Store.version = excluded.version - 1
  BatchUpsertStmtStr* =
    fmt"""
    WITH v(id, data, version, ts) AS (
      VALUES $1
    )
    INSERT INTO {TableName} ({IdColName}, {DataColName}, {VersionColName}, {TimestampColName})
    SELECT id, data, version, ts
    FROM v
    WHERE v.version = 1
       OR EXISTS (SELECT 1 FROM {TableName} s WHERE s.{IdColName} = v.id)
    ON CONFLICT({IdColName}) DO UPDATE SET
      {DataColName} = excluded.{DataColName},
      {VersionColName} = {TableName}.{VersionColName} + 1,
      {TimestampColName} = excluded.{TimestampColName}
    WHERE {TableName}.{VersionColName} = excluded.{VersionColName} - 1
    RETURNING {IdColName}
  """

# Build batch upsert query with (?, ?, ?, ?) value tuples
proc makeBatchUpsertQuery*(count: int): string {.raises: [].} =
  if count == 0:
    return ""
  let tuples = newSeqWith(count, "(?, ?, ?, ?)").join(", ")
  try:
    BatchUpsertStmtStr % tuples
  except ValueError:
    raiseAssert("Invalid BatchUpsertStmtStr format")

# Build parameterized GET MANY query with ? placeholders
# Returns sql_string
proc makeGetManyParamQuery*(count: int): string {.raises: [].} =
  if count == 0:
    return ""
  let placeholders = newSeqWith(count, "?").join(", ")
  try:
    GetManyStmtStr % placeholders
  except ValueError:
    # Should never happen with controlled placeholders
    raiseAssert("Invalid GetManyStmtStr format")

# Build parameterized HAS MANY query with ? placeholders
# Returns sql_string - only selects id column for existence check
proc makeHasManyParamQuery*(count: int): string {.raises: [].} =
  if count == 0:
    return ""
  let placeholders = newSeqWith(count, "?").join(", ")
  try:
    HasManyStmtStr % placeholders
  except ValueError:
    # Should never happen with controlled placeholders
    raiseAssert("Invalid HasManyStmtStr format")

# Build parameterized DELETE MANY query with ? placeholders
# Each record needs 2 params: (id, version), using VALUES clause for tuples
# Returns sql string - caller binds id1, ver1, id2, ver2, ... in order
proc makeDeleteManyParamQuery*(count: int): string {.raises: [].} =
  if count == 0:
    return ""
  # Build: WHERE (id, version) IN ((?, ?), (?, ?), ...)
  let tuples = newSeqWith(count, "(?, ?)").join(", ")
  try:
    DeleteManyStmtStr % tuples
  except ValueError:
    # Should never happen with controlled placeholders
    raiseAssert("Invalid DeleteManyStmtStr format")


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
  let
    (parent, name, ext) = path.normalizePathEnd.splitFile
    dbExt = if ext == "": DbExt else: ext
    absPath =
      if parent.isAbsolute:
        parent
      else:
        ?catch(getCurrentDir()) / parent
    dbPath = absPath / name & dbExt

  return success dbPath

proc checkChanges*(self: SQLiteDsDb): ?!int =
  var changes = 0
  proc onChanges(s: RawStmtPtr) =
    changes = changesCol(s, 0)()

  if err =? self.getChangesStmt.query((), onChanges).errorOption:
    return failure(err)

  return success changes

proc close*(self: var SQLiteDsDb): ?!void =
  ## Close the database and all prepared statements.
  ## Returns failure if the database cannot be closed properly.

  # Idempotent: skip if already closed
  if self.env.isNil:
    return success()

  let env = self.env
  self.env = nil # Mark as closed immediately to prevent double-close

  # Finalize all prepared statements first
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

  if not RawStmtPtr(self.moveStmt).isNil:
    self.moveStmt.dispose

  if not RawStmtPtr(self.deleteStmt).isNil:
    self.deleteStmt.dispose

  if not RawStmtPtr(self.getChangesStmt).isNil:
    self.getChangesStmt.dispose

  # Now close the database connection
  return closeDb(env)

proc open*(
    _: type SQLiteDsDb, path = SqliteMemory, flags = SQLITE_OPEN_READONLY
): ?!SQLiteDsDb =
  # make it optional to enable WAL with it enabled being the default?

  # make it possible to specify a custom page size?
  # https://www.sqlite.org/pragma.html#pragma_page_size
  # https://www.sqlite.org/intern-v-extern-blob.html

  var env: AutoDisposed[SQLite]

  defer:
    disposeIfUnreleased(env)

  let
    isMemory = path == SqliteMemory
    absPath =
      if isMemory:
        SqliteMemory
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

  # Set production pragmas for durability and performance
  ?setProductionPragmas(env.val)

  var
    containsStmt: ContainsStmt
    getSingleStmt: GetSingleStmt
    moveStmt: MoveStmt
    deleteStmt: DeleteStmt
    getChangesStmt: GetChangesStmt
    beginStmt: BeginStmt
    endStmt: EndStmt
    rollbackStmt: RollbackStmt

  if not readOnly:
    checkExec(env.val, CreateStmtStr)

    moveStmt = ?MoveStmt.prepare(env.val, MoveStmtStr, SQLITE_PREPARE_PERSISTENT)

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
    moveStmt: moveStmt,
    deleteStmt: deleteStmt,
    getChangesStmt: getChangesStmt,
    beginStmt: beginStmt,
    endStmt: endStmt,
    rollbackStmt: rollbackStmt,
  )
