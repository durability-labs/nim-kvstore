import std/options
import std/os
import std/sequtils
from std/algorithm import sort, reversed
import std/tables

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/questionable

import pkg/kvstore

import ../kvcommontests
import ../querycommontests

suite "Test Basic SQLiteKVStore":

  let
    ds = SQLiteKVStore.new(Memory).tryGet()
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  teardownAll:
    (await ds.close()).tryGet()

  basicStoreTests(ds, key, bytes, otherBytes)
  helperTests(ds, key)

suite "Test Read Only SQLiteKVStore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    filename = "test_store" & DbExt
    dbPathAbs = basePathAbs / filename
    key = Key.init("a:b/c/d:e").tryGet()
    bytes = "some bytes".toBytes

  var
    dsDb: SQLiteKVStore
    readOnlyDb: SQLiteKVStore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    dsDb = SQLiteKVStore.new(path = dbPathAbs).tryGet()
    readOnlyDb = SQLiteKVStore.new(path = dbPathAbs, readOnly = true).tryGet()

  teardownAll:
    (await dsDb.close()).tryGet()
    (await readOnlyDb.close()).tryGet()

    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "put":
    check:
      (await readOnlyDb.put(key, bytes)).isErr

    (await dsDb.put(key, bytes)).tryGet()

  var record: RawRecord
  test "get":
    record = (await readOnlyDb.get(key)).tryGet()

    check:
      record.val == bytes
      (await dsDb.get(key)).tryGet().val == bytes

  test "delete":
    check:
      (await readOnlyDb.delete(record)).isErr

    (await dsDb.delete(record)).tryGet()

  test "contains":
    check:
      not (await readOnlyDb.has(key)).tryGet()
      not (await dsDb.has(key)).tryGet()

suite "Test Query":
  var ds: SQLiteKVStore

  setup:
    ds = SQLiteKVStore.new(Memory).tryGet()

  teardown:
    (await ds.close()).tryGet

  queryTests(ds, testLimitsAndOffsets = true, testSortOrder = true)

  test "Query should return records with tokens":
    let key = Key.init("/test/query/token").tryGet()
    (await ds.put(key, @[1'u8, 2, 3])).tryGet()

    let q = Query.init(Key.init("/test/query").tryGet())
    let iter = (await ds.query(q)).tryGet()

    defer:
      iter.dispose()

    for item in iter:
      let maybeRecord = (await item).tryGet()
      if record =? maybeRecord:
        check record.token > 0  # Token should be non-zero after put

  test "Query with value=true should return records with tokens":
    let key = Key.init("/test/query/token/value").tryGet()
    let data = @[4'u8, 5, 6]
    (await ds.put(key, data)).tryGet()

    let q = Query.init(Key.init("/test/query/token").tryGet(), value = true)
    let iter = (await ds.query(q)).tryGet()

    defer:
      iter.dispose()

    for item in iter:
      let maybeRecord = (await item).tryGet()
      if record =? maybeRecord:
        check:
          record.token > 0  # Token should be non-zero after put
          record.val == data  # Value should be returned
