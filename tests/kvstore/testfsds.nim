import std/options
import std/sequtils
import std/os
from std/algorithm import sort, reversed
import std/tables

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/taskpools

import pkg/kvstore

import ./kvcommontests
import ./typedcommontests
import ./querycommontests

suite "Test Basic FSKVStore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var
    tp: Taskpool
    fsStore: FSKVStore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    tp = Taskpool.new(numThreads = 4)
    fsStore = FSKVStore.new(root = basePathAbs, tp = tp, depth = 16).tryGet()

  teardownAll:
    require((await fsStore.close()).isOk)
    tp.shutdown()
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  basicStoreTests(fsStore, key, bytes, otherBytes)
  typedHelperTests(fsStore, key)
  atomicBatchTests(fsStore, key, supportsAtomic = false)

suite "Test Misc FSKVStore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    bytes = "some bytes".toBytes

  var tp: Taskpool

  setupAll:
    tp = Taskpool.new(numThreads = 4)

  teardownAll:
    tp.shutdown()

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

  teardown:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "Test validDepth()":
    let
      fs = FSKVStore.new(root = "/", tp = tp, depth = 3).tryGet()
      invalid = Key.init("/a/b/c/d").tryGet()
      valid = Key.init("/a/b/c").tryGet()

    check:
      not fs.validDepth(invalid)
      fs.validDepth(valid)

  test "Test invalid key (path) depth":
    let
      fs = FSKVStore.new(root = basePathAbs, tp = tp, depth = 3).tryGet()
      key = Key.init("/a/b/c/d").tryGet()

    check:
      (await fs.put(RawRecord.init(key, bytes))).isErr
      (await fs.get(key)).isErr
      (await fs.delete(KeyRecord.init(key))).isErr
      (await fs.has(key)).isErr

  test "Test valid key (path) depth":
    let
      fs = FSKVStore.new(root = basePathAbs, tp = tp, depth = 3).tryGet()
      key = Key.init("/a/b/c").tryGet()

    require (await fs.put(key, bytes)).isOk
    let stored = (await fs.get(key)).tryGet()
    check:
      (await fs.get(key)).tryGet() == stored

    require (await fs.delete(stored)).isOk
    check:
      not (await fs.has(key)).tryGet()

  test "Test key cannot write outside of root":
    let
      fs = FSKVStore.new(root = basePathAbs, tp = tp, depth = 3).tryGet()
      key = Key.init("/a/../../c").tryGet()

    check:
      (await fs.put(RawRecord.init(key, bytes))).isErr
      (await fs.get(key)).isErr
      (await fs.delete(RawRecord.init(key, EmptyBytes))).isErr
      (await fs.has(key)).isErr

  test "Test key cannot convert to invalid path":
    let fs = FSKVStore.new(root = basePathAbs, tp = tp).tryGet()

    for c in invalidFilenameChars:
      if c == ':':
        continue
      if c == '/':
        continue

      let key = Key.init("/" & c).tryGet()

      check:
        (await fs.put(RawRecord.init(key, bytes))).isErr
        (await fs.get(key)).isErr
        (await fs.delete(RawRecord.init(key, EmptyBytes))).isErr
        (await fs.has(key)).isErr

suite "Test Query":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath

  var
    tp: Taskpool
    ds: FSKVStore

  setupAll:
    tp = Taskpool.new(numThreads = 4)

  teardownAll:
    tp.shutdown()
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    ds = FSKVStore.new(root = basePathAbs, tp = tp, depth = 5).tryGet()

  teardown:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  queryTests(ds, testLimitsAndOffsets = false, testSortOrder = false)

  test "Query should exclude siblings outside the namespace":
    let
      rootKey = Key.init("/ns").tryGet()
      leftKey = Key.init(rootKey, Key.init("/left").tryGet()).tryGet()
      leftChild = Key.init(leftKey, Key.init("/child").tryGet()).tryGet()
      rightKey = Key.init(rootKey, Key.init("/right").tryGet()).tryGet()

    (await ds.put(RawRecord.init(leftKey, "left".toBytes))).tryGet()
    (await ds.put(RawRecord.init(leftChild, "left child".toBytes))).tryGet()
    (await ds.put(RawRecord.init(rightKey, "right".toBytes))).tryGet()

    let iter = (await ds.query(Query.init(leftKey))).tryGet()
    let res = (await iter.fetchAll()).tryGet

    check:
      res.len == 2
      res.filterIt(it.key == leftKey).len == 1
      res.filterIt(it.key == leftChild).len == 1
      res.filterIt(it.key == rightKey).len == 0
