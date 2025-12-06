import std/os
import std/strutils

import pkg/chronos
import pkg/asynctest/chronos/unittest2
import pkg/stew/byteutils

import pkg/sqlite3_abi
import pkg/kvstore/key
import pkg/kvstore/sql/sqlitedsdb
import pkg/kvstore/sql/sqliteutils
import pkg/kvstore/sql/sqliteds

suite "Test Open SQLite Datastore DB":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    filename = "test_store" & DbExt
    dbPathAbs = basePathAbs / filename

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

  teardownAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "Should create and open store DB":
    var dsDb = SQLiteDsDb
      .open(path = dbPathAbs, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()
    defer:
      dsDb.close()

    check:
      fileExists(dbPathAbs)

  test "Should open existing DB":
    var dsDb = SQLiteDsDb
      .open(path = dbPathAbs, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    defer:
      dsDb.close()

      check:
        fileExists(dbPathAbs)

  test "Should open existing DB in read only mode":
    check:
      fileExists(dbPathAbs)

    var dsDb = SQLiteDsDb.open(path = dbPathAbs, flags = SQLITE_OPEN_READONLY).tryGet()

    defer:
      dsDb.close()

  test "Should fail open non existent DB in read only mode":
    removeDir(basePathAbs)
    check:
      not fileExists(dbPathAbs)
      SQLiteDsDb.open(path = dbPathAbs).isErr

suite "Test SQLite Datastore DB operations":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    filename = "test_store" & DbExt
    dbPathAbs = basePathAbs / filename

    key = Key.init("test/key").tryGet()
    key1 = Key.init("test/key1").tryGet()
    data = "some data".toBytes
    otherData = "some other data".toBytes

  var
    dsDb: SQLiteDsDb
    readOnlyDb: SQLiteDsDb

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    dsDb = SQLiteDsDb
      .open(path = dbPathAbs, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    readOnlyDb =
      SQLiteDsDb.open(path = dbPathAbs, flags = SQLITE_OPEN_READONLY).tryGet()

  teardownAll:
    dsDb.close()
    readOnlyDb.close()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "Should insert key - calls callback with version 0":
    proc onRow(s: RawStmtPtr) =
      var
        value = dataCol(s, GetSingleStmtDataCol)()
        token = versionCol(s, GetSingleStmtVersionCol)()

      check:
        value == data
        token == 1'i64

    discard dsDb.upsertStmt.query((key.id, data, 0'i64, timestamp()), onRow).tryGet()

  test "Should update key":
    proc onRow(s: RawStmtPtr) =
      var
        value = dataCol(s, GetSingleStmtDataCol)()
        token = versionCol(s, GetSingleStmtVersionCol)()

      check:
        value == otherData
        token == 2'i64

    discard
      dsDb.upsertStmt.query((key.id, otherData, 1'i64, timestamp()), onRow).tryGet()

  test "Should fail update key with wrong version - calls callback with existing version 1":
    proc onRow(s: RawStmtPtr) =
      check false # should not be called

    discard dsDb.upsertStmt.query((key.id, data, 0'i64, timestamp()), onRow).tryGet()

  test "Should select single key":
    var
      bytes: seq[byte]
      version: int64

    proc onData(s: RawStmtPtr) =
      bytes = dataCol(s, GetSingleStmtDataCol)()
      version = versionCol(s, GetSingleStmtVersionCol)()

    discard dsDb.getSingleStmt.query((key.id), onData).tryGet()

    check:
      bytes == otherData
      version == 2'i64

  test "Should select multiple keys":
    var
      keys = newSeqOfCap[Key](2)
      datas = newSeqOfCap[seq[byte]](2)
      versions = newSeqOfCap[int](2)

    # add another record
    discard dsDb.upsertStmt
      .query(
        (key1.id, data, 0'i64, timestamp()),
        proc(s: RawStmtPtr) =
          discard,
      )
      .tryGet()

    proc onData(s: RawStmtPtr) {.raises: [].} =
      let
        id = idCol(s, GetManyStmtIdCol)()
        data = dataCol(s, GetManyStmtDataCol)()
        version = versionCol(s, GetManyStmtVersionCol)()

      keys.add(Key.init(id).get)
      datas.add(data)
      versions.add(version.int)

    let queryStr = makeGetManyStmStr([key.id, key1.id]).tryGet()
    discard dsDb.env.query(queryStr, onData).tryGet()

    check:
      keys == @[key, key1]
      datas == @[otherData, data]
      versions == @[2, 1]

  test "Should delete keys":
    let queryStr = makeDeleteManyStmStr([(key.id, 2'u64), (key1.id, 1'u64)]).tryGet()

    check:
      dsDb.env.exec(queryStr).isOk()
      dsDb.checkChanges().tryGet() == 2

  test "Should not contain key":
    var exists = false

    proc onData(s: RawStmtPtr) =
      exists = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

    check:
      dsDb.containsStmt.query((key.id), onData).isOk()
      not exists
