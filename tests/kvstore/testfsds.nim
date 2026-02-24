import std/options
import std/sequtils
import std/os
import std/strutils
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
import ./closecommontests
import ./threadingcommontests
import ./cancellationcommontests

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
  hasBatchTests(fsStore, key)

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
      (await fs.put(RawKVRecord.init(key, bytes))).isErr
      (await fs.get(key)).isErr
      (await fs.delete(KeyKVRecord.init(key))).isErr
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
      (await fs.put(RawKVRecord.init(key, bytes))).isErr
      (await fs.get(key)).isErr
      (await fs.delete(RawKVRecord.init(key, EmptyBytes))).isErr
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
        (await fs.put(RawKVRecord.init(key, bytes))).isErr
        (await fs.get(key)).isErr
        (await fs.delete(RawKVRecord.init(key, EmptyBytes))).isErr
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

    (await ds.put(RawKVRecord.init(leftKey, "left".toBytes))).tryGet()
    (await ds.put(RawKVRecord.init(leftChild, "left child".toBytes))).tryGet()
    (await ds.put(RawKVRecord.init(rightKey, "right".toBytes))).tryGet()

    let iter = (await ds.query(Query.init(leftKey))).tryGet()
    defer:
      (await iter.dispose()).tryGet()

    let res = (await iter.fetchAll()).tryGet

    check:
      res.len == 2
      res.filterIt(it.key == leftKey).len == 1
      res.filterIt(it.key == leftChild).len == 1
      res.filterIt(it.key == rightKey).len == 0

suite "Test Close and Dispose":
  let
    path = currentSourcePath()
    basePath = "tests_data_close"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/close/test").tryGet()

  var
    tp: Taskpool
    ds: FSKVStore

  setupAll:
    removeDir(basePathAbs)
    createDir(basePathAbs)
    tp = Taskpool.new(numThreads = 4)
    ds = FSKVStore.new(root = basePathAbs, tp = tp, depth = 16).tryGet()

  teardownAll:
    discard await ds.close()
    tp.shutdown()
    removeDir(basePathAbs)

  iteratorDisposeTests(ds, key)

suite "Test Iterator Tracking":
  let
    path = currentSourcePath()
    basePath = "tests_data_tracking"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/tracking/test").tryGet()

  var
    tp: Taskpool
    factoryCounter: int

  setupAll:
    tp = Taskpool.new(numThreads = 4)
    factoryCounter = 0

  teardownAll:
    tp.shutdown()
    removeDir(basePathAbs)

  proc fsFactory(): Future[KVStore] {.
      async: (raises: [CancelledError, CatchableError])
  .} =
    # Each call creates a fresh store in a unique subdirectory
    let subDir = basePathAbs / $factoryCounter
    factoryCounter.inc
    removeDir(subDir)
    createDir(subDir)
    FSKVStore.new(root = subDir, tp = tp, depth = 16).tryGet()

  iteratorTrackingTests(fsFactory, key)
  closeAndDisposeTests(fsFactory, key)
  concurrentCloseTests(fsFactory, key)

suite "Test Threading":
  let
    path = currentSourcePath()
    basePath = "tests_data_threading"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/threading/test").tryGet()

  var
    tp: Taskpool
    factoryCounter: int

  setupAll:
    tp = Taskpool.new(numThreads = 4)
    factoryCounter = 0

  teardownAll:
    tp.shutdown()
    removeDir(basePathAbs)

  proc fsThreadingFactory(): Future[KVStore] {.
      async: (raises: [CancelledError, CatchableError])
  .} =
    let subDir = basePathAbs / $factoryCounter
    factoryCounter.inc
    removeDir(subDir)
    createDir(subDir)
    FSKVStore.new(root = subDir, tp = tp, depth = 16).tryGet()

  threadingTests(fsThreadingFactory, key)

suite "Test Cancellation":
  let
    path = currentSourcePath()
    basePath = "tests_data_cancel"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/cancel/test").tryGet()

  var
    tp: Taskpool
    factoryCounter: int

  setupAll:
    tp = Taskpool.new(numThreads = 4)
    factoryCounter = 0

  teardownAll:
    tp.shutdown()
    removeDir(basePathAbs)

  proc fsCancellationFactory(): Future[KVStore] {.
      async: (raises: [CancelledError, CatchableError])
  .} =
    let subDir = basePathAbs / $factoryCounter
    factoryCounter.inc
    removeDir(subDir)
    createDir(subDir)
    FSKVStore.new(root = subDir, tp = tp, depth = 16).tryGet()

  cancellationTests(fsCancellationFactory, key)

suite "Test FsWriteConfig":
  let
    path = currentSourcePath()
    basePath = "tests_data_writeconfig"
    basePathAbs = path.parentDir / basePath
    key = Key.init("/wc/test").tryGet()
    bytes = "write config test".toBytes
    otherBytes = "updated value".toBytes

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

  test "Should use default config (fsync on) when no params given":
    let fs = FSKVStore.new(root = basePathAbs, tp = tp, depth = 16).tryGet()
    (await fs.put(key, bytes)).tryGet()
    let record = (await fs.get(key)).tryGet()
    check record.val == bytes
    (await fs.close()).tryGet()

  test "Should write and read with fsync disabled":
    let fs = FSKVStore
      .new(root = basePathAbs, tp = tp, depth = 16, fsyncFile = false, fsyncDir = false)
      .tryGet()
    (await fs.put(key, bytes)).tryGet()
    let record = (await fs.get(key)).tryGet()
    check:
      record.val == bytes
      record.token == 1
    (await fs.close()).tryGet()

  test "Should write and read with all durability off":
    let fs = FSKVStore
      .new(
        root = basePathAbs,
        tp = tp,
        depth = 16,
        directIO = false,
        fsyncFile = false,
        fsyncDir = false,
      )
      .tryGet()
    (await fs.put(key, bytes)).tryGet()
    let record = (await fs.get(key)).tryGet()
    check:
      record.val == bytes
      record.token == 1
    (await fs.close()).tryGet()

  test "Should update records correctly with fsync disabled":
    let fs = FSKVStore
      .new(root = basePathAbs, tp = tp, depth = 16, fsyncFile = false, fsyncDir = false)
      .tryGet()
    (await fs.put(key, bytes)).tryGet()
    var r1 = (await fs.get(key)).tryGet()
    check r1.token == 1
    r1.val = otherBytes
    (await fs.put(r1)).tryGet()
    let r2 = (await fs.get(key)).tryGet()
    check:
      r2.val == otherBytes
      r2.token == 2
    (await fs.close()).tryGet()

  test "Should delete records correctly with fsync disabled":
    let fs = FSKVStore
      .new(root = basePathAbs, tp = tp, depth = 16, fsyncFile = false, fsyncDir = false)
      .tryGet()
    (await fs.put(key, bytes)).tryGet()
    let record = (await fs.get(key)).tryGet()
    (await fs.delete(KeyKVRecord.init(key, record.token))).tryGet()
    check:
      not (await fs.has(key)).tryGet()
    (await fs.close()).tryGet()

  test "Should not leave temp files after successful put":
    let fs = FSKVStore
      .new(root = basePathAbs, tp = tp, depth = 16, fsyncFile = false, fsyncDir = false)
      .tryGet()
    # Write several records
    for i in 0 ..< 10:
      let k = Key.init("/tmp/test/" & $i).tryGet()
      (await fs.put(k, bytes)).tryGet()

    # Walk the entire tree looking for .tmp files
    var tmpFiles: seq[string]
    for f in walkDirRec(basePathAbs):
      if f.endsWith(".tmp"):
        tmpFiles.add(f)
    check tmpFiles.len == 0
    (await fs.close()).tryGet()

  test "Should not leave temp files after successful update":
    let fs = FSKVStore
      .new(root = basePathAbs, tp = tp, depth = 16, fsyncFile = false, fsyncDir = false)
      .tryGet()
    (await fs.put(key, bytes)).tryGet()
    let r1 = (await fs.get(key)).tryGet()
    # Update multiple times
    var rec = r1
    for i in 0 ..< 5:
      rec.val = ("update " & $i).toBytes
      (await fs.put(rec)).tryGet()
      rec = (await fs.get(key)).tryGet()

    var tmpFiles: seq[string]
    for f in walkDirRec(basePathAbs):
      if f.endsWith(".tmp"):
        tmpFiles.add(f)
    check tmpFiles.len == 0
    (await fs.close()).tryGet()

  test "Should handle batch put with config":
    let fs = FSKVStore
      .new(root = basePathAbs, tp = tp, depth = 16, fsyncFile = false, fsyncDir = false)
      .tryGet()
    var records: seq[RawKVRecord]
    for i in 0 ..< 20:
      records.add(
        RawKVRecord.init(Key.init("/batch/" & $i).tryGet(), ("value" & $i).toBytes)
      )

    let skipped = (await fs.put(records)).tryGet()
    check skipped.len == 0

    for r in records:
      let fetched = (await fs.get(r.key)).tryGet()
      check fetched.val == r.val

    # No temp files left
    var tmpFiles: seq[string]
    for f in walkDirRec(basePathAbs):
      if f.endsWith(".tmp"):
        tmpFiles.add(f)
    check tmpFiles.len == 0
    (await fs.close()).tryGet()
