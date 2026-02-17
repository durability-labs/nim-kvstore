import std/os

import pkg/chronos
import pkg/asynctest/chronos/unittest2
import pkg/stew/byteutils

import pkg/sqlite3_abi
import pkg/kvstore/key
import pkg/kvstore/sql/sqlitedsdb
import pkg/kvstore/sql/sqliteutils

# =============================================================================
# Helpers
# =============================================================================

proc readSingle(
    dsDb: SQLiteDsDb, keyId: string
): tuple[found: bool, data: seq[byte], version: int64] =
  var
    found = false
    data: seq[byte]
    version: int64

  proc onRow(s: RawStmtPtr) =
    found = true
    data = dataCol(s, GetSingleStmtDataCol)()
    version = versionCol(s, GetSingleStmtVersionCol)()

  discard dsDb.getSingleStmt.query((keyId), onRow).tryGet()
  (found, data, version)

proc exists(dsDb: SQLiteDsDb, keyId: string): bool =
  var res = false

  proc onRow(s: RawStmtPtr) =
    res = sqlite3_column_int64(s, ContainsStmtExistsCol.cint).bool

  discard dsDb.containsStmt.query((keyId), onRow).tryGet()
  res

proc upsert(
    dsDb: SQLiteDsDb, records: openArray[(string, seq[byte], int64, int64)]
): seq[string] =
  ## Execute raw batch upsert, return list of affected key ids.
  let queryStr = makeBatchUpsertQuery(records.len)
  var affected: seq[string]

  proc onRow(s: RawStmtPtr) =
    affected.add($sqlite3_column_text_not_null(s, BatchUpsertReturnIdCol.cint))

  discard dsDb.env.queryWithUpsertRecords(queryStr, @records, onRow).tryGet()
  affected

# =============================================================================
# Open / Close
# =============================================================================

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
      discard dsDb.close()

    check:
      fileExists(dbPathAbs)

  test "Should open existing DB":
    var dsDb = SQLiteDsDb
      .open(path = dbPathAbs, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    defer:
      discard dsDb.close()

      check:
        fileExists(dbPathAbs)

  test "Should open existing DB in read only mode":
    check:
      fileExists(dbPathAbs)

    var dsDb = SQLiteDsDb.open(path = dbPathAbs, flags = SQLITE_OPEN_READONLY).tryGet()

    defer:
      discard dsDb.close()

  test "Should fail open non existent DB in read only mode":
    removeDir(basePathAbs)
    check:
      not fileExists(dbPathAbs)
      SQLiteDsDb.open(path = dbPathAbs).isErr

# =============================================================================
# Query Builder Correctness
# =============================================================================

suite "Test query builders":
  # Expected SQL strings built by hand to match the fmt templates in sqlitedsdb.nim.
  # These catch any accidental change to the SQL structure.

  test "makeBatchUpsertQuery count=0 returns empty string":
    check makeBatchUpsertQuery(0) == ""

  test "makeBatchUpsertQuery count=1":
    let expected =
      "    WITH v(id, data, version, ts) AS (\n" & "      VALUES (?, ?, ?, ?)\n" &
      "    )\n" & "    INSERT INTO Store (id, data, version, timestamp)\n" &
      "    SELECT id, data, version, ts\n" & "    FROM v\n" & "    WHERE v.version = 1\n" &
      "       OR EXISTS (SELECT 1 FROM Store s WHERE s.id = v.id)\n" &
      "    ON CONFLICT(id) DO UPDATE SET\n" & "      data = excluded.data,\n" &
      "      version = Store.version + 1,\n" & "      timestamp = excluded.timestamp\n" &
      "    WHERE Store.version = excluded.version - 1\n" & "    RETURNING id\n" & "  "
    check makeBatchUpsertQuery(1) == expected

  test "makeBatchUpsertQuery count=3":
    let expected =
      "    WITH v(id, data, version, ts) AS (\n" &
      "      VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)\n" & "    )\n" &
      "    INSERT INTO Store (id, data, version, timestamp)\n" &
      "    SELECT id, data, version, ts\n" & "    FROM v\n" & "    WHERE v.version = 1\n" &
      "       OR EXISTS (SELECT 1 FROM Store s WHERE s.id = v.id)\n" &
      "    ON CONFLICT(id) DO UPDATE SET\n" & "      data = excluded.data,\n" &
      "      version = Store.version + 1,\n" & "      timestamp = excluded.timestamp\n" &
      "    WHERE Store.version = excluded.version - 1\n" & "    RETURNING id\n" & "  "
    check makeBatchUpsertQuery(3) == expected

  test "makeGetManyParamQuery count=0 returns empty string":
    check makeGetManyParamQuery(0) == ""

  test "makeGetManyParamQuery count=1":
    let expected =
      "    SELECT id, data, version FROM Store\n" & "    WHERE id IN (?)\n" & "  "
    check makeGetManyParamQuery(1) == expected

  test "makeGetManyParamQuery count=3":
    let expected =
      "    SELECT id, data, version FROM Store\n" & "    WHERE id IN (?, ?, ?)\n" & "  "
    check makeGetManyParamQuery(3) == expected

  test "makeHasManyParamQuery count=0 returns empty string":
    check makeHasManyParamQuery(0) == ""

  test "makeHasManyParamQuery count=1":
    let expected = "    SELECT id FROM Store\n" & "    WHERE id IN (?)\n" & "  "
    check makeHasManyParamQuery(1) == expected

  test "makeHasManyParamQuery count=3":
    let expected = "    SELECT id FROM Store\n" & "    WHERE id IN (?, ?, ?)\n" & "  "
    check makeHasManyParamQuery(3) == expected

  test "makeDeleteManyParamQuery count=0 returns empty string":
    check makeDeleteManyParamQuery(0) == ""

  test "makeDeleteManyParamQuery count=1":
    let expected =
      "    DELETE FROM Store\n" & "    WHERE (id, version) IN ((?, ?))\n" & "  "
    check makeDeleteManyParamQuery(1) == expected

  test "makeDeleteManyParamQuery count=3":
    let expected =
      "    DELETE FROM Store\n" & "    WHERE (id, version) IN ((?, ?), (?, ?), (?, ?))\n" &
      "  "
    check makeDeleteManyParamQuery(3) == expected

# =============================================================================
# Batch Upsert CAS Semantics (the 5 cases)
# =============================================================================

suite "Test batch upsert - CAS insert scenarios":
  var dsDb: SQLiteDsDb

  let
    key1 = Key.init("ins/key1").tryGet()
    key2 = Key.init("ins/key2").tryGet()
    key3 = Key.init("ins/key3").tryGet()
    data1 = "data1".toBytes
    data2 = "data2".toBytes

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

  teardownAll:
    discard dsDb.close()

  test "Case 1: insert new key (token=0, version=1) succeeds":
    let affected = dsDb.upsert([(key1.id, data1, 1'i64, 1000'i64)])

    check affected == @[key1.id]

    let (found, d, v) = dsDb.readSingle(key1.id)
    check:
      found
      d == data1
      v == 1'i64

  test "Case 1: insert multiple new keys in one batch":
    let affected = dsDb.upsert(
      [(key2.id, data1, 1'i64, 1000'i64), (key3.id, data2, 1'i64, 1000'i64)]
    )

    check:
      affected.len == 2
      key2.id in affected
      key3.id in affected

    let
      (f2, d2, v2) = dsDb.readSingle(key2.id)
      (f3, d3, v3) = dsDb.readSingle(key3.id)
    check:
      f2 and d2 == data1 and v2 == 1'i64
      f3 and d3 == data2 and v3 == 1'i64

  test "Case 2: insert existing key (token=0, version=1) is silently skipped":
    # key1 already exists with version=1
    let affected =
      dsDb.upsert([(key1.id, "should not be written".toBytes, 1'i64, 9999'i64)])

    check affected.len == 0 # ON CONFLICT DO NOTHING path

    # Data unchanged
    let (_, d, v) = dsDb.readSingle(key1.id)
    check:
      d == data1
      v == 1'i64

  test "Insert with empty data stores NULL and reads back as empty seq":
    let
      emptyKey = Key.init("ins/empty").tryGet()
      affected = dsDb.upsert([(emptyKey.id, newSeq[byte](), 1'i64, 1000'i64)])

    check affected == @[emptyKey.id]

    let (found, d, v) = dsDb.readSingle(emptyKey.id)
    check:
      found
      d.len == 0
      v == 1'i64

suite "Test batch upsert - CAS update scenarios":
  var dsDb: SQLiteDsDb

  let
    key1 = Key.init("upd/key1").tryGet()
    data1 = "original".toBytes
    data2 = "updated".toBytes

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    # Insert key1 with version=1
    let affected = dsDb.upsert([(key1.id, data1, 1'i64, 1000'i64)])
    require(affected.len == 1)

  teardownAll:
    discard dsDb.close()

  test "Case 3: update existing key with correct token succeeds":
    # Current version=1, so update needs version=2 (token=1)
    let affected = dsDb.upsert([(key1.id, data2, 2'i64, 2000'i64)])

    check affected == @[key1.id]

    let (_, d, v) = dsDb.readSingle(key1.id)
    check:
      d == data2
      v == 2'i64

  test "Case 4: update existing key with wrong token is skipped":
    # Current version=2, correct update version would be 3
    # Use version=99 which won't match WHERE Store.version = 98
    let affected = dsDb.upsert([(key1.id, "wrong".toBytes, 99'i64, 3000'i64)])

    check affected.len == 0

    # Data unchanged
    let (_, d, v) = dsDb.readSingle(key1.id)
    check:
      d == data2
      v == 2'i64

  test "Case 4: update with stale token (already incremented) is skipped":
    # Current version=2, using version=2 means token=1 which is stale
    # WHERE Store.version = 1 won't match because Store.version is now 2
    let affected = dsDb.upsert([(key1.id, "stale".toBytes, 2'i64, 3000'i64)])

    check affected.len == 0

    let (_, d, v) = dsDb.readSingle(key1.id)
    check:
      d == data2
      v == 2'i64

  test "Case 5: update non-existing key is filtered out by EXISTS":
    let
      ghost = Key.init("upd/nonexistent").tryGet()
      affected = dsDb.upsert([(ghost.id, data1, 5'i64, 3000'i64)])

    check affected.len == 0

    let (found, _, _) = dsDb.readSingle(ghost.id)
    check not found

suite "Test batch upsert - mixed batches":
  var dsDb: SQLiteDsDb

  let
    keyA = Key.init("mix/a").tryGet()
    keyB = Key.init("mix/b").tryGet()
    keyC = Key.init("mix/c").tryGet()
    keyD = Key.init("mix/d").tryGet()
    data = "data".toBytes
    updated = "updated".toBytes

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    # Pre-insert keyA (version=1) and keyB (version=1)
    let affected =
      dsDb.upsert([(keyA.id, data, 1'i64, 1000'i64), (keyB.id, data, 1'i64, 1000'i64)])
    require(affected.len == 2)

  teardownAll:
    discard dsDb.close()

  test "Mixed: correct update + new insert + wrong-token update":
    let affected = dsDb.upsert(
      [
        (keyA.id, updated, 2'i64, 2000'i64), # Update keyA: correct (version 1->2)
        (keyC.id, data, 1'i64, 2000'i64), # Insert keyC: new key
        (keyB.id, updated, 99'i64, 2000'i64), # Update keyB: WRONG token
      ]
    )

    check:
      affected.len == 2
      keyA.id in affected
      keyC.id in affected
      keyB.id notin affected

    let
      (_, dA, vA) = dsDb.readSingle(keyA.id)
      (_, dB, vB) = dsDb.readSingle(keyB.id)
      (fC, dC, vC) = dsDb.readSingle(keyC.id)
    check:
      dA == updated and vA == 2'i64
      dB == data and vB == 1'i64 # Unchanged
      fC and dC == data and vC == 1'i64

  test "Mixed: insert existing + update nonexistent + new insert":
    let affected = dsDb.upsert(
      [
        (keyA.id, "dup".toBytes, 1'i64, 3000'i64), # Insert existing: skipped
        (keyD.id, data, 1'i64, 3000'i64), # Insert keyD: new
        ("mix/ghost", data, 5'i64, 3000'i64), # Update nonexistent: filtered
      ]
    )

    check:
      affected.len == 1
      keyD.id in affected

  test "Sequential updates increment version correctly":
    # keyA is at version=2 from previous test
    let
      aff3 = dsDb.upsert([(keyA.id, "v3".toBytes, 3'i64, 4000'i64)])
      aff4 = dsDb.upsert([(keyA.id, "v4".toBytes, 4'i64, 5000'i64)])
      aff5 = dsDb.upsert([(keyA.id, "v5".toBytes, 5'i64, 6000'i64)])

    check:
      aff3 == @[keyA.id]
      aff4 == @[keyA.id]
      aff5 == @[keyA.id]

    let (_, d, v) = dsDb.readSingle(keyA.id)
    check:
      d == "v5".toBytes
      v == 5'i64

# =============================================================================
# getSingleStmt
# =============================================================================

suite "Test getSingleStmt":
  var dsDb: SQLiteDsDb

  let
    key1 = Key.init("getsingle/key1").tryGet()
    data1 = "hello".toBytes

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    let affected = dsDb.upsert([(key1.id, data1, 1'i64, 1000'i64)])
    require(affected.len == 1)

  teardownAll:
    discard dsDb.close()

  test "Returns data and version for existing key":
    let (found, d, v) = dsDb.readSingle(key1.id)
    check:
      found
      d == data1
      v == 1'i64

  test "Returns no row for non-existing key":
    let (found, _, _) = dsDb.readSingle("getsingle/nonexistent")
    check not found

  test "Returns updated data after upsert":
    let newData = "updated".toBytes
    discard dsDb.upsert([(key1.id, newData, 2'i64, 2000'i64)])

    let (found, d, v) = dsDb.readSingle(key1.id)
    check:
      found
      d == newData
      v == 2'i64

# =============================================================================
# containsStmt
# =============================================================================

suite "Test containsStmt":
  var dsDb: SQLiteDsDb

  let key1 = Key.init("contains/key1").tryGet()

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    let affected = dsDb.upsert([(key1.id, "data".toBytes, 1'i64, 1000'i64)])
    require(affected.len == 1)

  teardownAll:
    discard dsDb.close()

  test "Returns true for existing key":
    check dsDb.exists(key1.id)

  test "Returns false for non-existing key":
    check not dsDb.exists("contains/nonexistent")

# =============================================================================
# queryWithStrings + makeGetManyParamQuery
# =============================================================================

suite "Test batch get (makeGetManyParamQuery + queryWithStrings)":
  var dsDb: SQLiteDsDb

  let
    key1 = Key.init("bget/key1").tryGet()
    key2 = Key.init("bget/key2").tryGet()
    key3 = Key.init("bget/key3").tryGet()
    data1 = "one".toBytes
    data2 = "two".toBytes
    data3 = "three".toBytes

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    let affected = dsDb.upsert(
      [
        (key1.id, data1, 1'i64, 1000'i64),
        (key2.id, data2, 1'i64, 1000'i64),
        (key3.id, data3, 1'i64, 1000'i64),
      ]
    )
    require(affected.len == 3)

  teardownAll:
    discard dsDb.close()

  test "Fetches all requested keys with correct data and versions":
    var
      ids: seq[string]
      datas: seq[seq[byte]]
      versions: seq[int64]

    proc onRow(s: RawStmtPtr) =
      ids.add(idCol(s, GetManyStmtIdCol)())
      datas.add(dataCol(s, GetManyStmtDataCol)())
      versions.add(versionCol(s, GetManyStmtVersionCol)())

    let queryStr = makeGetManyParamQuery(3)
    discard
      dsDb.env.queryWithStrings(queryStr, [key1.id, key2.id, key3.id], onRow).tryGet()

    check:
      ids.len == 3
      key1.id in ids
      key2.id in ids
      key3.id in ids

  test "Returns only existing keys when some are missing":
    var ids: seq[string]

    proc onRow(s: RawStmtPtr) =
      ids.add(idCol(s, GetManyStmtIdCol)())

    let queryStr = makeGetManyParamQuery(3)
    discard dsDb.env
      .queryWithStrings(queryStr, [key1.id, "bget/nonexistent", key3.id], onRow)
      .tryGet()

    check:
      ids.len == 2
      key1.id in ids
      key3.id in ids

  test "Returns empty result for all non-existing keys":
    var rowCount = 0

    proc onRow(s: RawStmtPtr) =
      inc rowCount

    let queryStr = makeGetManyParamQuery(2)
    discard dsDb.env
      .queryWithStrings(queryStr, ["bget/ghost1", "bget/ghost2"], onRow)
      .tryGet()

    check rowCount == 0

  test "Single key fetch works":
    var
      id: string
      version: int64

    proc onRow(s: RawStmtPtr) =
      id = idCol(s, GetManyStmtIdCol)()
      version = versionCol(s, GetManyStmtVersionCol)()

    let queryStr = makeGetManyParamQuery(1)
    discard dsDb.env.queryWithStrings(queryStr, [key2.id], onRow).tryGet()

    check:
      id == key2.id
      version == 1'i64

# =============================================================================
# queryWithStrings + makeHasManyParamQuery
# =============================================================================

suite "Test batch has (makeHasManyParamQuery + queryWithStrings)":
  var dsDb: SQLiteDsDb

  let
    key1 = Key.init("bhas/key1").tryGet()
    key2 = Key.init("bhas/key2").tryGet()

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    let affected = dsDb.upsert(
      [(key1.id, "x".toBytes, 1'i64, 1000'i64), (key2.id, "y".toBytes, 1'i64, 1000'i64)]
    )
    require(affected.len == 2)

  teardownAll:
    discard dsDb.close()

  test "Returns ids of existing keys":
    var foundIds: seq[string]

    proc onRow(s: RawStmtPtr) =
      foundIds.add(idCol(s, HasManyStmtIdCol)())

    let queryStr = makeHasManyParamQuery(2)
    discard dsDb.env.queryWithStrings(queryStr, [key1.id, key2.id], onRow).tryGet()

    check:
      foundIds.len == 2
      key1.id in foundIds
      key2.id in foundIds

  test "Filters out non-existing keys":
    var foundIds: seq[string]

    proc onRow(s: RawStmtPtr) =
      foundIds.add(idCol(s, HasManyStmtIdCol)())

    let queryStr = makeHasManyParamQuery(3)
    discard dsDb.env
      .queryWithStrings(queryStr, [key1.id, "bhas/ghost", key2.id], onRow)
      .tryGet()

    check:
      foundIds.len == 2
      "bhas/ghost" notin foundIds

  test "Returns empty for all non-existing":
    var rowCount = 0

    proc onRow(s: RawStmtPtr) =
      inc rowCount

    let queryStr = makeHasManyParamQuery(1)
    discard dsDb.env.queryWithStrings(queryStr, ["bhas/nothing"], onRow).tryGet()

    check rowCount == 0

# =============================================================================
# deleteStmt (single key)
# =============================================================================

suite "Test deleteStmt (single key)":
  var dsDb: SQLiteDsDb

  let
    key1 = Key.init("sdel/key1").tryGet()
    key2 = Key.init("sdel/key2").tryGet()

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    let affected = dsDb.upsert(
      [(key1.id, "a".toBytes, 1'i64, 1000'i64), (key2.id, "b".toBytes, 1'i64, 1000'i64)]
    )
    require(affected.len == 2)

  teardownAll:
    discard dsDb.close()

  test "Deletes key with correct version":
    proc onRow(s: RawStmtPtr) =
      discard

    discard dsDb.deleteStmt.query((key1.id, 1'i64), onRow).tryGet()

    check:
      dsDb.checkChanges().tryGet() == 1
      not dsDb.exists(key1.id)

  test "Does not delete key with wrong version":
    proc onRow(s: RawStmtPtr) =
      discard

    discard dsDb.deleteStmt.query((key2.id, 99'i64), onRow).tryGet()

    check:
      dsDb.checkChanges().tryGet() == 0
      dsDb.exists(key2.id)

  test "Does not delete non-existing key":
    proc onRow(s: RawStmtPtr) =
      discard

    discard dsDb.deleteStmt.query(("sdel/ghost", 1'i64), onRow).tryGet()

    check dsDb.checkChanges().tryGet() == 0

# =============================================================================
# queryWithIdVersionPairs + makeDeleteManyParamQuery
# =============================================================================

suite "Test batch delete (makeDeleteManyParamQuery + queryWithIdVersionPairs)":
  var dsDb: SQLiteDsDb

  let
    key1 = Key.init("bdel/key1").tryGet()
    key2 = Key.init("bdel/key2").tryGet()
    key3 = Key.init("bdel/key3").tryGet()

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

    let affected = dsDb.upsert(
      [
        (key1.id, "a".toBytes, 1'i64, 1000'i64),
        (key2.id, "b".toBytes, 1'i64, 1000'i64),
        (key3.id, "c".toBytes, 1'i64, 1000'i64),
      ]
    )
    require(affected.len == 3)

  teardownAll:
    discard dsDb.close()

  test "Deletes multiple keys with correct versions":
    proc onRow(s: RawStmtPtr) =
      discard

    let queryStr = makeDeleteManyParamQuery(2)
    discard dsDb.env
      .queryWithIdVersionPairs(queryStr, [(key1.id, 1'i64), (key2.id, 1'i64)], onRow)
      .tryGet()

    check:
      dsDb.checkChanges().tryGet() == 2
      not dsDb.exists(key1.id)
      not dsDb.exists(key2.id)
      dsDb.exists(key3.id) # Untouched

  test "Skips keys with wrong versions in batch":
    # key3 still exists with version=1
    proc onRow(s: RawStmtPtr) =
      discard

    let queryStr = makeDeleteManyParamQuery(1)
    discard
      dsDb.env.queryWithIdVersionPairs(queryStr, [(key3.id, 99'i64)], onRow).tryGet()

    check:
      dsDb.checkChanges().tryGet() == 0
      dsDb.exists(key3.id) # Still there

  test "Skips non-existing keys in batch":
    proc onRow(s: RawStmtPtr) =
      discard

    let queryStr = makeDeleteManyParamQuery(2)
    discard dsDb.env
      .queryWithIdVersionPairs(
        queryStr, [("bdel/ghost1", 1'i64), ("bdel/ghost2", 1'i64)], onRow
      )
      .tryGet()

    check dsDb.checkChanges().tryGet() == 0

# =============================================================================
# moveStmt
# =============================================================================

suite "Test moveStmt":
  var dsDb: SQLiteDsDb

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

  teardownAll:
    discard dsDb.close()

  test "Moves children matching prefix glob":
    # Insert old/a, old/b
    let affected = dsDb.upsert(
      [
        ("old/a", "aaa".toBytes, 1'i64, 1000'i64),
        ("old/b", "bbb".toBytes, 1'i64, 1000'i64),
      ]
    )
    require(affected.len == 2)

    var movedKeys: seq[string]

    proc onRow(s: RawStmtPtr) =
      movedKeys.add($sqlite3_column_text_not_null(s, 0.cint))

    # substrOffset = len("old") + 1 = 4
    discard dsDb.moveStmt.query(("new", 4'i64, "old/*", "old"), onRow).tryGet()

    check:
      movedKeys.len == 2
      "new/a" in movedKeys
      "new/b" in movedKeys

    # Old keys gone, new keys exist
    check:
      not dsDb.exists("old/a")
      not dsDb.exists("old/b")
      dsDb.exists("new/a")
      dsDb.exists("new/b")

  test "Moves exact prefix key itself":
    discard dsDb.upsert([("exact", "data".toBytes, 1'i64, 1000'i64)])

    var movedKeys: seq[string]

    proc onRow(s: RawStmtPtr) =
      movedKeys.add($sqlite3_column_text_not_null(s, 0.cint))

    # substrOffset = len("exact") + 1 = 6
    discard dsDb.moveStmt.query(("renamed", 6'i64, "exact/*", "exact"), onRow).tryGet()

    check:
      movedKeys.len == 1
      "renamed" in movedKeys
      not dsDb.exists("exact")
      dsDb.exists("renamed")

  test "Preserves data and version after move":
    discard dsDb.upsert([("mv/src", "important".toBytes, 1'i64, 1000'i64)])

    proc onRow(s: RawStmtPtr) =
      discard

    # substrOffset = len("mv/src") + 1 = 7
    discard dsDb.moveStmt.query(("mv/dst", 7'i64, "mv/src/*", "mv/src"), onRow).tryGet()

    let (found, d, v) = dsDb.readSingle("mv/dst")
    check:
      found
      d == "important".toBytes
      v == 1'i64

  test "No-op when prefix matches nothing":
    var movedCount = 0

    proc onRow(s: RawStmtPtr) =
      inc movedCount

    discard dsDb.moveStmt
      .query(("dst", 12'i64, "nonexistent/*", "nonexistent"), onRow)
      .tryGet()

    check movedCount == 0

# =============================================================================
# checkChanges
# =============================================================================

suite "Test checkChanges":
  var dsDb: SQLiteDsDb

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

  teardownAll:
    discard dsDb.close()

  test "Reports correct count after single insert":
    discard dsDb.upsert([("chg/a", "x".toBytes, 1'i64, 1000'i64)])
    check dsDb.checkChanges().tryGet() == 1

  test "Reports correct count after batch insert":
    discard dsDb.upsert(
      [("chg/b", "x".toBytes, 1'i64, 1000'i64), ("chg/c", "x".toBytes, 1'i64, 1000'i64)]
    )
    check dsDb.checkChanges().tryGet() == 2

  test "Reports 0 after skipped insert (existing key, token=0)":
    discard dsDb.upsert([("chg/a", "y".toBytes, 1'i64, 2000'i64)])
    check dsDb.checkChanges().tryGet() == 0

  test "Reports correct count after update":
    discard dsDb.upsert([("chg/a", "updated".toBytes, 2'i64, 3000'i64)])
    check dsDb.checkChanges().tryGet() == 1

  test "Reports correct count after delete":
    proc onRow(s: RawStmtPtr) =
      discard

    discard dsDb.deleteStmt.query(("chg/a", 2'i64), onRow).tryGet()
    check dsDb.checkChanges().tryGet() == 1

# =============================================================================
# Transaction statements (beginStmt / endStmt / rollbackStmt)
# =============================================================================

suite "Test transaction statements":
  var dsDb: SQLiteDsDb

  setupAll:
    dsDb = SQLiteDsDb
      .open(path = SqliteMemory, flags = SQLITE_OPEN_READWRITE or SQLITE_OPEN_CREATE)
      .tryGet()

  teardownAll:
    discard dsDb.close()

  test "Begin + insert + end commits data":
    dsDb.beginStmt.exec().tryGet()
    discard dsDb.upsert([("txn/committed", "data".toBytes, 1'i64, 1000'i64)])
    dsDb.endStmt.exec().tryGet()

    check dsDb.exists("txn/committed")

  test "Begin + insert + rollback discards data":
    dsDb.beginStmt.exec().tryGet()
    discard dsDb.upsert([("txn/rolledback", "data".toBytes, 1'i64, 1000'i64)])
    dsDb.rollbackStmt.exec().tryGet()

    check not dsDb.exists("txn/rolledback")
