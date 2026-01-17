import std/options
import std/sequtils
import std/os
from std/algorithm import sort, reversed
import std/tables

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils

import pkg/kvstore

import ./kvcommontests
import ./querycommontests

suite "Test Basic FSKVStore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/a/b").tryGet()
    bytes = "some bytes".toBytes
    otherBytes = "some other bytes".toBytes

  var fsStore: FSKVStore

  setupAll:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    fsStore = FSKVStore.new(root = basePathAbs, depth = 16).tryGet()

  teardownAll:
    require((await fsStore.close()).isOk)
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  basicStoreTests(fsStore, key, bytes, otherBytes)
  helperTests(fsStore, key)
  atomicBatchTests(fsStore, key, supportsAtomic = false)

suite "Test Misc FSKVStore":
  let
    path = currentSourcePath() # get this file's name
    basePath = "tests_data"
    basePathAbs = path.parentDir / basePath
    bytes = "some bytes".toBytes

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

  teardown:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  test "Test validDepth()":
    let
      fs = FSKVStore.new(root = "/", depth = 3).tryGet()
      invalid = Key.init("/a/b/c/d").tryGet()
      valid = Key.init("/a/b/c").tryGet()

    check:
      not fs.validDepth(invalid)
      fs.validDepth(valid)

  test "Test invalid key (path) depth":
    let
      fs = FSKVStore.new(root = basePathAbs, depth = 3).tryGet()
      key = Key.init("/a/b/c/d").tryGet()

    check:
      (await fs.put(RawRecord.init(key, bytes))).isErr
      (await fs.get(key)).isErr
      (await fs.delete(KeyRecord.init(key))).isErr
      (await fs.has(key)).isErr

  test "Test valid key (path) depth":
    let
      fs = FSKVStore.new(root = basePathAbs, depth = 3).tryGet()
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
      fs = FSKVStore.new(root = basePathAbs, depth = 3).tryGet()
      key = Key.init("/a/../../c").tryGet()

    check:
      (await fs.put(RawRecord.init(key, bytes))).isErr
      (await fs.get(key)).isErr
      (await fs.delete(RawRecord.init(key, EmptyBytes))).isErr
      (await fs.has(key)).isErr

  test "Test key cannot convert to invalid path":
    let fs = FSKVStore.new(root = basePathAbs).tryGet()

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

  var ds: FSKVStore

  setup:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))
    createDir(basePathAbs)

    ds = FSKVStore.new(root = basePathAbs, depth = 5).tryGet()

  teardown:
    removeDir(basePathAbs)
    require(not dirExists(basePathAbs))

  teardownAll:
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
    let res = (await allFinished(toSeq(iter)))
      .mapIt(it.read.tryGet)
      .filterIt(it.isSome)
      .mapIt(it.get)

    check:
      res.len == 2
      res.filterIt(it.key == leftKey).len == 1
      res.filterIt(it.key == leftChild).len == 1
      res.filterIt(it.key == rightKey).len == 0
