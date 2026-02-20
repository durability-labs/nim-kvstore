{.push raises: [].}

import pkg/questionable
import pkg/questionable/results
import pkg/sqlite3_abi

export sqlite3_abi

# Adapted from:
# https://github.com/status-im/nwaku/blob/master/waku/v2/node/storage/sqlite.nim

# see https://www.sqlite.org/c3ref/column_database_name.html
# can pass `--forceBuild:on` to the Nim compiler if a SQLite build without
# `-DSQLITE_ENABLE_COLUMN_METADATA` option is stuck in the build cache,
# e.g. `nimble test --forceBuild:on`
{.passc: "-DSQLITE_ENABLE_COLUMN_METADATA".}

type
  AutoDisposed*[T: ptr | ref] = object
    val*: T

  DataProc* = proc(s: RawStmtPtr) {.closure, gcsafe, raises: [].}

  NoParams* =
    tuple
      # empty tuple

  NoParamsStmt* = SQLiteStmt[NoParams, void]

  RawStmtPtr* = ptr sqlite3_stmt

  SQLite* = ptr sqlite3

  SQLiteStmt*[Params, Res] = distinct RawStmtPtr

  # see https://github.com/arnetheduck/nim-sqlite3-abi/issues/4
  sqlite3_destructor_type_gcsafe = proc(a1: pointer) {.cdecl, gcsafe, raises: [].}

const SQLITE_TRANSIENT_GCSAFE* = cast[sqlite3_destructor_type_gcsafe](-1)

proc bindParam(s: RawStmtPtr, n: int, val: auto): cint =
  when val is openArray[byte] | seq[byte]:
    if val.len > 0:
      # `SQLITE_TRANSIENT` "indicate[s] that the object is to be copied prior
      # to the return from sqlite3_bind_*(). The object and pointer to it
      # must remain valid until then. SQLite will then manage the lifetime of
      # its private copy."
      sqlite3_bind_blob(s, n.cint, unsafeAddr val[0], val.len.cint, SQLITE_TRANSIENT)
    else:
      sqlite3_bind_null(s, n.cint)
  elif val is int32:
    sqlite3_bind_int(s, n.cint, val)
  elif val is uint32 | int64:
    sqlite3_bind_int64(s, n.cint, val.int64)
  elif val is float32 | float64:
    sqlite3_bind_double(s, n.cint, val.float64)
  elif val is string:
    # `-1` implies string length is num bytes up to first null-terminator;
    # `SQLITE_TRANSIENT` "indicate[s] that the object is to be copied prior
    # to the return from sqlite3_bind_*(). The object and pointer to it must
    # remain valid until then. SQLite will then manage the lifetime of its
    # private copy."
    sqlite3_bind_text(s, n.cint, val.cstring, -1.cint, SQLITE_TRANSIENT)
  else:
    {.fatal: "Please add support for the '" & $typeof(val) & "' type".}

template bindParams(s: RawStmtPtr, params: auto) =
  when params is tuple:
    when params isnot NoParams:
      var i = 1
      for param in fields(params):
        checkErr bindParam(s, i, param)
        inc i
  else:
    checkErr bindParam(s, 1, params)

template checkErr*(op: untyped) =
  if (let v = (op); v != SQLITE_OK):
    return failure $sqlite3_errstr(v)

template dispose*(rawStmt: RawStmtPtr) =
  doAssert SQLITE_OK == sqlite3_finalize(rawStmt)
  rawStmt = nil

template checkExec*(s: RawStmtPtr) =
  if (let x = sqlite3_step(s); x != SQLITE_DONE):
    s.dispose
    return failure $sqlite3_errstr(x)

  if (let x = sqlite3_finalize(s); x != SQLITE_OK):
    return failure $sqlite3_errstr(x)

template prepare*(env: SQLite, q: string, prepFlags: cuint = 0): RawStmtPtr =
  var s: RawStmtPtr

  checkErr sqlite3_prepare_v3(env, q.cstring, q.len.cint, prepFlags, addr s, nil)

  s

template checkExec*(env: SQLite, q: string) =
  var s = prepare(env, q)

  checkExec(s)

proc closeDb*(db: SQLite): ?!void =
  ## Close the database with proper error handling.
  ## Uses sqlite3_close_v2 which allows for pending statements to be finalized
  ## automatically when they are no longer in use.
  if db.isNil:
    return success()

  let closeResult = sqlite3_close_v2(db)
  if closeResult != SQLITE_OK:
    # In debug builds, also output to stderr for visibility
    return failure("Failed to close database: " & $sqlite3_errstr(closeResult))

  success()

template dispose*(db: SQLite) {.deprecated: "Use closeDb()".} =
  # Legacy template for backward compatibility
  # Prefer using closeDb() for proper error handling
  let closeResult = sqlite3_close_v2(db)
  doAssert(
    closeResult == SQLITE_OK, "sqlite3_close_v2 failed: " & $sqlite3_errstr(closeResult)
  )

template dispose*(sqliteStmt: SQLiteStmt) =
  doAssert SQLITE_OK == sqlite3_finalize(RawStmtPtr(sqliteStmt))
  # nil literals can no longer be directly assigned to variables or fields of distinct pointer types.
  # They must be converted instead.
  # See https://nim-lang.org/blog/2022/12/21/version-20-rc.html#:~:text=nil%20literals%20can%20no%20longer%20be%20directly%20assigned%20to%20variables%20or%20fields%20of%20distinct%20pointer%20types.%20They%20must%20be%20converted%20instead.
  # SQLiteStmt(nil) is generating a SIGSEGV, so we need to cast it
  sqliteStmt = cast[typeof sqliteStmt](nil)

proc release*[T](x: var AutoDisposed[T]): T =
  result = x.val
  x.val = nil

proc disposeIfUnreleased*[T](x: var AutoDisposed[T]) =
  mixin dispose
  if x.val != nil:
    dispose(x.release)

proc prepare*[Params, Res](
    T: type SQLiteStmt[Params, Res], env: SQLite, stmt: string, prepFlags: cuint = 0
): ?!T =
  var s: RawStmtPtr

  checkErr sqlite3_prepare_v3(env, stmt.cstring, stmt.len.cint, prepFlags, addr s, nil)

  success T(s)

proc exec*[P](s: SQLiteStmt[P, void], params: P = ()): ?!void =
  let s = RawStmtPtr(s)

  bindParams(s, params)

  let res =
    if (let v = sqlite3_step(s); v != SQLITE_DONE):
      failure $sqlite3_errstr(v)
    else:
      success()

  # release implicit transaction - discard to preserve original error in res
  discard sqlite3_reset(s) # same return information as step
  discard sqlite3_clear_bindings(s) # no errors possible

  res

proc sqlite3_column_text_not_null*(s: RawStmtPtr, index: cint): cstring =
  let text = sqlite3_column_text(s, index).cstring

  if text.isNil:
    # see the conversion table and final paragraph of:
    # https://www.sqlite.org/c3ref/column_blob.html
    # a null pointer here implies an out-of-memory error
    let v = sqlite3_errcode(sqlite3_db_handle(s))

    raise (ref Defect)(msg: $sqlite3_errstr(v))

  text

template journalModePragmaStmt*(env: SQLite): RawStmtPtr =
  var s = prepare(env, "PRAGMA journal_mode = WAL;")

  if (let x = sqlite3_step(s); x != SQLITE_ROW):
    s.dispose
    return failure $sqlite3_errstr(x)

  if (let x = sqlite3_column_type(s, 0); x != SQLITE3_TEXT):
    s.dispose
    return failure $sqlite3_errstr(x)

  let x = $sqlite3_column_text_not_null(s, 0)

  if not (x in ["memory", "wal"]):
    s.dispose
    return failure "Invalid pragma result: \"" & x & "\""

  s

template open*(dbPath: string, env: var SQLite, flags = 0) =
  checkErr sqlite3_open_v2(dbPath.cstring, addr env, flags.cint, nil)

proc exec*(env: SQLite, q: string): ?!void =
  var s = ?NoParamsStmt.prepare(env, q)

  ?s.exec()
  # NB: dispose of the prepared query statement and free associated memory
  s.dispose

  success()

type SQLiteStepError* = object of CatchableError
  code*: cint

proc newSQLiteStepError*(msg: string, code: cint): ref SQLiteStepError =
  result = newException(SQLiteStepError, msg)
  result.code = code

proc query*[P](s: SQLiteStmt[P, void], params: P, onData: DataProc): ?!bool =
  let s = RawStmtPtr(s)

  bindParams(s, params)

  var res = success false

  while true:
    let v = sqlite3_step(s)

    case v
    of SQLITE_ROW:
      onData(s)
      res = success true
    of SQLITE_DONE:
      break
    else:
      res = failure(newSQLiteStepError($sqlite3_errstr(v), v))
      break

  # release implicit transaction - discard to preserve original error in res
  discard sqlite3_reset(s) # same return information as step
  discard sqlite3_clear_bindings(s) # no errors possible

  res

proc query*(env: SQLite, query: string, onData: DataProc): ?!bool =
  var s = ?NoParamsStmt.prepare(env, query)

  var res = s.query((), onData)
  # NB: dispose of the prepared query statement and free associated memory
  s.dispose
  return res

proc queryWithStrings*(
    env: SQLite, query: string, params: openArray[string], onData: DataProc
): ?!bool =
  ## Execute a query with dynamically bound string parameters
  ## Used for parameterized IN clauses to prevent SQL injection
  var s: RawStmtPtr
  checkErr sqlite3_prepare_v3(env, query.cstring, query.len.cint, 0, addr s, nil)

  # Bind all string parameters
  for i, param in params:
    let v = sqlite3_bind_text(s, (i + 1).cint, param.cstring, -1.cint, SQLITE_TRANSIENT)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s) # discard to preserve bind error
      return failure $sqlite3_errstr(v)

  var res = success false

  while true:
    let v = sqlite3_step(s)
    case v
    of SQLITE_ROW:
      onData(s)
      res = success true
    of SQLITE_DONE:
      break
    else:
      res = failure $sqlite3_errstr(v)
      break

  checkErr sqlite3_finalize(s)
  res

proc queryWithIdVersionPairs*(
    env: SQLite, query: string, pairs: openArray[(string, int64)], onData: DataProc
): ?!bool =
  ## Execute a query with dynamically bound (id, version) pairs
  ## Binds as: id1, ver1, id2, ver2, ... for IN ((?, ?), (?, ?), ...) clauses
  var s: RawStmtPtr
  checkErr sqlite3_prepare_v3(env, query.cstring, query.len.cint, 0, addr s, nil)

  # Bind all pairs: id1, ver1, id2, ver2, ...
  var paramIdx = 1
  for (id, version) in pairs:
    var v = sqlite3_bind_text(s, paramIdx.cint, id.cstring, -1.cint, SQLITE_TRANSIENT)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s) # discard to preserve bind error
      return failure $sqlite3_errstr(v)
    inc paramIdx

    v = sqlite3_bind_int64(s, paramIdx.cint, version)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s) # discard to preserve bind error
      return failure $sqlite3_errstr(v)
    inc paramIdx

  var res = success false

  while true:
    let v = sqlite3_step(s)
    case v
    of SQLITE_ROW:
      onData(s)
      res = success true
    of SQLITE_DONE:
      break
    else:
      res = failure $sqlite3_errstr(v)
      break

  checkErr sqlite3_finalize(s)
  res

proc queryWithUpsertRecords*(
    env: SQLite,
    query: string,
    records: openArray[(string, seq[byte], int64, int64)],
    onData: DataProc,
): ?!bool =
  ## Execute a batch upsert with dynamically bound (id, data, version, timestamp) tuples.
  ## Binds as: id1, data1, ver1, ts1, id2, data2, ver2, ts2, ...
  var s: RawStmtPtr
  checkErr sqlite3_prepare_v3(env, query.cstring, query.len.cint, 0, addr s, nil)

  var paramIdx = 1
  for (id, data, version, stamp) in records:
    var v = sqlite3_bind_text(s, paramIdx.cint, id.cstring, -1.cint, SQLITE_TRANSIENT)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s)
      return failure $sqlite3_errstr(v)
    inc paramIdx

    v =
      if data.len > 0:
        sqlite3_bind_blob(
          s, paramIdx.cint, unsafeAddr data[0], data.len.cint, SQLITE_TRANSIENT
        )
      else:
        sqlite3_bind_null(s, paramIdx.cint)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s)
      return failure $sqlite3_errstr(v)
    inc paramIdx

    v = sqlite3_bind_int64(s, paramIdx.cint, version)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s)
      return failure $sqlite3_errstr(v)
    inc paramIdx

    v = sqlite3_bind_int64(s, paramIdx.cint, stamp)
    if v != SQLITE_OK:
      discard sqlite3_finalize(s)
      return failure $sqlite3_errstr(v)
    inc paramIdx

  var res = success false

  while true:
    let v = sqlite3_step(s)
    case v
    of SQLITE_ROW:
      onData(s)
      res = success true
    of SQLITE_DONE:
      break
    else:
      res = failure $sqlite3_errstr(v)
      break

  checkErr sqlite3_finalize(s)
  res

proc execPragma(env: SQLite, pragma: string): ?!void =
  ## Execute a PRAGMA statement that returns a result row.
  ## Steps until done, then finalizes.
  var s = prepare(env, pragma)

  # Step through - PRAGMAs typically return SQLITE_ROW then SQLITE_DONE
  while true:
    let v = sqlite3_step(s)
    case v
    of SQLITE_ROW:
      continue # Consume result row
    of SQLITE_DONE:
      break
    else:
      discard sqlite3_finalize(s) # discard to preserve step error
      return failure $sqlite3_errstr(v)

  let finalizeResult = sqlite3_finalize(s)
  if finalizeResult != SQLITE_OK:
    return failure $sqlite3_errstr(finalizeResult)

  success()

proc setProductionPragmas*(env: SQLite): ?!void =
  ## Set production-ready SQLite pragmas for durability and performance.
  ##
  ## - busy_timeout: Wait up to 5 seconds for locks instead of failing immediately
  ## - synchronous=NORMAL: Safe with WAL mode, ~2x faster than FULL
  ## - cache_size: 64MB page cache (default 2MB is too conservative)
  ## - mmap_size: 256MB memory-mapped I/O for faster reads
  ## - temp_store: Keep temporary tables/indices in memory
  ## - wal_autocheckpoint: 10000 pages to reduce checkpoint stalls during writes
  ##
  ## These pragmas improve behavior under concurrent access and write performance.

  # Set busy timeout to 5 seconds (5000ms)
  # This prevents immediate "database is locked" errors under contention
  ?env.execPragma("PRAGMA busy_timeout = 5000;")

  # Set synchronous to NORMAL (safe with WAL mode)
  # WAL + NORMAL provides durability guarantees while improving write performance
  # - Transactions are durable after commit (written to WAL)
  # - Only risk: recent transactions may roll back on OS crash (never corrupted)
  ?env.execPragma("PRAGMA synchronous = NORMAL;")

  # 64MB page cache (default is ~2MB)
  # Absorbs batch writes in memory, reduces WAL reads
  ?env.execPragma("PRAGMA cache_size = -65536;")

  # Keep temporary tables and indices in memory
  ?env.execPragma("PRAGMA temp_store = MEMORY;")

  # Memory-mapped I/O for dbs of up to 500GB
  ?env.execPragma("PRAGMA mmap_size = 536870912000;")

  # Checkpoint every 10000 pages instead of default 1000
  # Reduces checkpoint stalls during large batch writes
  ?env.execPragma("PRAGMA wal_autocheckpoint = 10000;")

  success()
