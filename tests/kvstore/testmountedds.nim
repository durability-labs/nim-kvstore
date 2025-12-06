import std/os
import std/tables
import std/sequtils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils

import pkg/kvstore
import pkg/kvstore/mountedds
import pkg/kvstore/sql
import pkg/kvstore/fsds

import ./kvcommontests

suite "Test Basic Mounted KVStore":
  let
    root = "tests" / "test_data"
    path = currentSourcePath() # get this file's name
    rootAbs = path.parentDir / root

    namespaceLeaf = Key.init("segment").tryGet
    sqlKey = Key.init("/root/sql").tryGet
    fsKey = Key.init("/root/fs").tryGet

    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    sql: SQLiteKVStore
    fs: FSKVStore
    mountedDs: MountedKVStore

  setupAll:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))
    createDir(rootAbs)

    sql = SQLiteKVStore.new(Memory).tryGet
    fs = FSKVStore.new(rootAbs, depth = 10).tryGet
    mountedDs = MountedKVStore.new(
      {sqlKey: KVStore(sql), fsKey: KVStore(fs)}.toTable
    ).tryGet

  teardownAll:
    removeDir(rootAbs)
    require(not dirExists(rootAbs))

  suite "Mounted sql":
    let namespace = sqlKey / namespaceLeaf
    basicStoreTests(mountedDs, namespace, bytes, otherBytes)
    helperTests(mountedDs, namespace)

  suite "Mounted fs":
    let namespace = fsKey / namespaceLeaf
    basicStoreTests(mountedDs, namespace, bytes, otherBytes)
    helperTests(mountedDs, namespace)

  test "Bulk operations across mounts":
    let
      sqlKeyA = (sqlKey / namespaceLeaf / "bulk-sql").tryGet
      fsKeyB = (fsKey / namespaceLeaf / "bulk-fs").tryGet
      initial = @[RawRecord.init(sqlKeyA, @[1.byte]), RawRecord.init(fsKeyB, @[2.byte])]

    # initial insert should succeed for both backends
    check (await mountedDs.put(initial)).tryGet.len == 0

    let fetched = (await mountedDs.get(@[sqlKeyA, fsKeyB])).tryGet
    check fetched.len == 2

    let sqlRecord = fetched.filterIt(it.key == sqlKeyA)[0]
    let fsRecord = fetched.filterIt(it.key == fsKeyB)[0]

    # using an outdated token must register a conflict for the sql record
    let staleSql = RawRecord.init(sqlRecord.key, sqlRecord.val, sqlRecord.token + 1)
    let conflicts = (await mountedDs.put(@[staleSql])).tryGet
    check conflicts.len == 1
    check conflicts[0] == sqlKeyA

    let skipped = (await mountedDs.delete(@[sqlRecord, fsRecord])).tryGet
    check skipped.len == 0 # both should be deleted successfully

suite "Test Mounted KVStore":
  test "Should mount datastore":
    let
      ds = SQLiteKVStore.new(Memory).tryGet
      mounted = MountedKVStore.new().tryGet
      key = Key.init("/sql").tryGet

    mounted.mount(key, ds).tryGet

    check mounted.stores.len == 1
    mounted.stores.withValue(key, store):
      check store.key == key
      check store.store == ds

  test "Should find with exact key":
    let
      ds = SQLiteKVStore.new(Memory).tryGet
      key = Key.init("/sql").tryGet
      mounted = MountedKVStore.new({key: KVStore(ds)}.toTable).tryGet
      store = mounted.findStore(key).tryGet

    check store.key == key
    check store.store == ds

  test "Should find with child key":
    let
      ds = SQLiteKVStore.new(Memory).tryGet
      key = Key.init("/sql").tryGet
      childKey = Key.init("/sql/child/key").tryGet
      mounted = MountedKVStore.new({key: KVStore(ds)}.toTable).tryGet
      store = mounted.findStore(childKey).tryGet

    check store.key == key
    check store.store == ds

  test "Should error on missing key":
    let
      ds = SQLiteKVStore.new(Memory).tryGet
      key = Key.init("/sql").tryGet
      childKey = Key.init("/nomatchkey/child/key").tryGet
      mounted = MountedKVStore.new({key: KVStore(ds)}.toTable).tryGet

    expect KVStoreKeyNotFound:
      discard mounted.findStore(childKey).tryGet

  test "Should find nested stores":
    let
      ds1 = SQLiteKVStore.new(Memory).tryGet
      ds2 = SQLiteKVStore.new(Memory).tryGet
      key1 = Key.init("/sql").tryGet
      key2 = Key.init("/sql/nested").tryGet

      nestedKey1 = Key.init("/sql/anotherkey").tryGet
      nestedKey2 = Key.init("/sql/nested/key").tryGet

      mounted = MountedKVStore.new(
        {key1: KVStore(ds1), key2: KVStore(ds2)}.toTable
      ).tryGet

      store1 = mounted.findStore(nestedKey1).tryGet
      store2 = mounted.findStore(nestedKey2).tryGet

    check store1.key == key1
    check store1.store == ds1

    check store2.key == key2
    check store2.store == ds2

  test "Should find with field:value key":
    let
      ds = SQLiteKVStore.new(Memory).tryGet
      key = Key.init("/sql").tryGet
      findKey1 = Key.init("/sql:name1").tryGet
      findKey2 = Key.init("/sql:name2").tryGet
      mounted = MountedKVStore.new({key: KVStore(ds)}.toTable).tryGet

    for k in @[findKey1, findKey2]:
      let store = mounted.findStore(k).tryGet

      check store.key == key
      check store.store == ds
