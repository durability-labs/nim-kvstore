{.push raises: [].}

import std/os
import std/tables
import std/strutils
import std/options
import std/random
import std/times

import pkg/chronos
import pkg/questionable
import pkg/questionable/results
import pkg/stew/endians2

import ./key
import ./query
import ./rawkvstore

const
  TokenBytes = sizeof(uint64)
  FileExt* = "dsobj"
  EmptyBytes* = newSeq[byte](0)

export rawkvstore

type FSKVStore* = ref object of KVStore
  ## Filesystem-backed kvstore that stores records as files.
  ##
  ## Tokens are stored as uint64 in little-endian format. Unlike SQLiteKVStore
  ## which is limited to int64 range due to SQLite INTEGER type, FSKVStore
  ## supports the full uint64 range.
  root*: string
  ignoreProtected: bool
  depth: int
  locks: TableRef[Key, AsyncLock]

randomize() # TODO: We should probably use a stronger rng here?

proc moveFile(src, dst: string): ?!void {.raises: [].} =
  try:
    os.moveFile(src, dst)
    success()
  except Exception as exc:
    failure newBackendError("unable to move '" & src & "' to '" & dst & "': " & exc.msg)

proc validDepth*(self: FSKVStore, key: Key): bool =
  key.len <= self.depth

proc isRootSubdir*(self: FSKVStore, path: string): bool =
  path.startsWith(self.root)

proc path*(self: FSKVStore, key: Key): ?!string =
  ## Return filename corresponding to the key
  ## or failure if the key doesn't correspond to a valid filename
  ##

  if not self.validDepth(key):
    return failure newBackendError("Path has invalid depth!")

  var segments: seq[string]
  for ns in key:
    let basename = ns.value.extractFilename
    if basename == "" or not basename.isValidFilename:
      return failure newBackendError("Filename contains invalid chars!")

    if ns.field == "":
      segments.add(ns.value)
    else:
      let basename = ns.field.extractFilename
      if basename == "" or not basename.isValidFilename:
        return failure newBackendError("Filename contains invalid chars!")

      # `:` are replaced with `/`
      segments.add(ns.field / ns.value)

  let absolute = ?((self.root / segments.joinPath()).absolutePath().catch())
  let fullname = absolute.addFileExt(FileExt)

  if not self.isRootSubdir(fullname):
    return failure newBackendError("Path is outside of `root` directory!")

  return success fullname

proc readVersioned*(
    self: FSKVStore, path: string, key: Key, data = true
): ?!RawRecord =
  var file: File

  defer:
    file.close

  if not file.open(path):
    return failure newBackendError("unable to open file: " & path)

  let size = ?catch (file.getFileSize)
  if size < TokenBytes:
    return failure newCorruptionError("File too small for record: " & path)

  var header: array[TokenBytes, byte]
  if ?catch (file.readBuffer(addr header[0], TokenBytes)) != TokenBytes:
    return failure newBackendError("unable to read token header")

  let token = uint64.fromBytesLE(header)
  let payloadLen = size - TokenBytes

  let value =
    if data:
      var value = newSeq[byte](payloadLen)
      if payloadLen > 0 and ?catch (file.readBytes(value, 0, payloadLen)) != payloadLen:
        return failure newBackendError("unable to read payload")
      value
    else:
      EmptyBytes

  return success RawRecord.init(key, value, token)

proc writeVersioned*(
    self: FSKVStore, path: string, token: uint64, value: seq[byte]
): ?!void =
  ?catch(createDir(parentDir(path)))

  let tmp = path & ".tmp-" & $epochTime() & "-" & $rand(1_000_000)

  var file: File
  defer:
    ?catch(removeFile(tmp))

  try:
    if not file.open(tmp, fmWrite):
      return failure newBackendError("unable to open temporary file '" & tmp & "'")

    let header = token.toBytesLE()
    if ?catch(file.writeBuffer(addr header[0], TokenBytes)) != TokenBytes:
      return failure newBackendError("Failed writing token")

    if value.len > 0:
      if ?catch (file.writeBytes(value, 0, value.len)) != value.len:
        return failure newBackendError("Failed writing data")

    file.flushFile()
  finally:
    file.close

  ?moveFile(tmp, path)

  return success()

method has*(
    self: FSKVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  return self.path(key) .? fileExists()

proc acquire*(
    self: FSKVStore, key: Key
): Future[AsyncLock] {.async: (raises: [CancelledError]).} =
  var lock = self.locks.mgetOrPut(key, newAsyncLock())
  await lock.acquire()
  lock

proc release*(self: FSKVStore, key: Key, lock: AsyncLock) {.raises: [].} =
  if lock.locked:
    try:
      lock.release()
    except CatchableError as err:
      raiseAssert(err.msg) # shouldn't happen
    finally:
      if not lock.locked:
        self.locks.del(key)

template withLock(self: FSKVStore, key: Key, body: untyped) =
  let lock = await self.acquire(key)
  try:
    body
  finally:
    self.release(key, lock)

method get*(
    self: FSKVStore, key: Key
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
  let path = ?self.path(key)

  if not path.fileExists():
    return failure newException(KVStoreKeyNotFound, "Key doesn't exist")

  return self.readVersioned(path, key)

method get*(
    self: FSKVStore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  var records: seq[RawRecord]
  for key in keys:
    let path = ?self.path(key)

    if not path.fileExists():
      continue

    let record = ?self.readVersioned(path, key)

    records.add(record)

  return success records

method put*(
    self: FSKVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  var conflicts: seq[Key]

  for record in records:
    let path = ?self.path(record.key)

    self.withLock(record.key):
      var conflict = false
      if not path.fileExists():
        if record.token != 0:
          conflict = true
        else:
          ?self.writeVersioned(path, 1'u64, record.val)
      else:
        let current = ?self.readVersioned(path, record.key)

        if current.token != record.token:
          conflict = true
        else:
          ?self.writeVersioned(path, current.token + 1, record.val)

      if conflict:
        conflicts.add(record.key)

  return success conflicts

method delete*(
    self: FSKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  var skipped: seq[Key]

  for record in records:
    let path = ?self.path(record.key)

    self.withLock(record.key):
      if not path.fileExists():
        skipped.add(record.key)
        continue

      let current = ?self.readVersioned(path, record.key)

      if current.token != record.token:
        skipped.add(record.key)
        continue

      ?catch(removeFile(path))

  return success skipped

proc dirWalker(path: string): (iterator (): string {.raises: [Defect], gcsafe.}) =
  return
    iterator (): string =
      try:
        for p in path.walkDirRec(yieldFilter = {pcFile}, relative = true):
          yield p
      except CatchableError as exc:
        raise newException(Defect, exc.msg)

method close*(self: FSKVStore): Future[?!void] {.async: (raises: [CancelledError]).} =
  return success()

method query*(
    self: FSKVStore, query: Query
): Future[?!QueryIterRaw] {.async: (raises: [CancelledError]).} =
  let path = ?self.path(query.key)

  let basePath =
    # it there is a file in the directory
    # with the same name then list the contents
    # of the directory, otherwise recurse
    # into subdirectories
    if path.fileExists:
      path.parentDir
    else:
      path.changeFileExt("")

  let walker = dirWalker(basePath)
  proc next(): Future[?!(?RawRecord)] {.async: (raises: [CancelledError]).} =
    while true:
      let path = walker()

      if finished(walker):
        return success RawRecord.none

      var keyPath = basePath

      keyPath.removePrefix(self.root)
      keyPath = keyPath / path.changeFileExt("")
      keyPath = keyPath.replace("\\", "/")

      let key = ?Key.init(keyPath)
      if key != query.key and not query.key.ancestor(key):
        continue

      return success (
        ?self.readVersioned(?catch((basePath / path).absolutePath), key, query.value)
      ).some

  proc finished(): bool =
    walker.finished

  proc dispose() =
    discard # No resources to cleanup for directory walker

  return success QueryIter.new(next, finished, dispose)

proc new*(
    T: type FSKVStore,
    root: string,
    depth = 2,
    caseSensitive = true,
    ignoreProtected = false,
): ?!T =
  let root =
    ?(
      block:
        if root.isAbsolute: root
        else:
          getCurrentDir() / root
    ).catch

  if not dirExists(root):
    return failure "directory does not exist: " & root

  success T(
    root: root,
    ignoreProtected: ignoreProtected,
    depth: depth,
    locks: newTable[Key, AsyncLock](),
  )
