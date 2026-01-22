{.push raises: [].}

## Typed KVStore Support
##
## Provides automatic conversion between user-defined types and seq[byte]
## using encoder/decoder procs.
##
## Basic usage:
## .. code-block:: Nim
##   import pkg/kvstore
##   import pkg/kvstore/typedkv
##   import pkg/stew/byteutils
##   import pkg/questionable/results
##
##   type Person = object
##     age: int
##     name: string
##
##   proc encode(p: Person): seq[byte] =
##     ($p.age & ":" & p.name).toBytes()
##
##   proc decode(T: type Person, bytes: seq[byte]): ?!T =
##     let values = string.fromBytes(bytes).split(':', maxsplit = 1)
##     success(Person(age: parseInt(values[0]), name: values[1]))
##
##   let p1 = Person(name: "john", age: 21)
##   let record1 = Record[Person].init(key, p1)
##   discard await ds.put(record1)
##   let record2 = (await ds.get[Person](key)).tryGet()
##   assert record2.val == p1

import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./types
import ./query
import ./helpers

export types, query, key, helpers

# =============================================================================
# Typed Get/Put/Delete
# =============================================================================

proc get*[T](
    self: KVStore, keys: seq[Key]
): Future[?!seq[Record[T]]] {.async: (raises: [CancelledError]).} =
  ## Get a list of records specified by the keys
  ##

  let raw = ?(await helpers.get(self, keys))
  success raw.mapIt(?toRecord[T](it))

proc get*[T](
    self: KVStore, key: Key
): Future[?!Record[T]] {.async: (raises: [CancelledError]).} =
  ## Get a single record
  ##

  toRecord[T]((?(await helpers.get(self, key))))

proc put*[T](
    self: KVStore, records: seq[Record[T]]
): Future[?!seq[Key]] {.async: (raw: true, raises: [CancelledError]).} =
  ## Insert or update a group of records
  ##
  ## The sequence will contain records that couldn't be inserted/updated
  ## due to conflicts.
  ##

  helpers.put(self, records.mapIt(it.toRaw))

proc put*[T](
    self: KVStore, record: Record[T]
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  ## Insert or update a single record
  ##

  helpers.put(self, record.toRaw)

proc put*[T](
    self: KVStore, key: Key, value: T
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  helpers.put(self, Record[T].init(key = key, val = value).toRaw)

proc delete*[T](
    self: KVStore, records: seq[Record[T]]
): Future[?!seq[Key]] {.async: (raw: true, raises: [CancelledError]).} =
  ## Delete records - extracts key+token only, value is ignored (no encode/decode)
  helpers.delete(self, records.toKeyRecord)

proc delete*[T](
    self: KVStore, record: Record[T]
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  ## Delete single record - value is ignored (no encode/decode)
  helpers.delete(self, record.toKeyRecord)

# =============================================================================
# Typed Query
# =============================================================================

proc query*[T](
    self: KVStore, q: Query
): Future[?!QueryIter[T]] {.async: (raises: [CancelledError]).} =
  let dsIter = ?(await helpers.query(self, q))

  proc next(): Future[?!(?Record[T])] {.async: (raises: [CancelledError]).} =
    let rawOpt = ?(await dsIter.next())
    without raw =? rawOpt:
      return success Record[T].none

    let decoded = ?(toRecord[T](raw))
    success decoded.some

  proc isFinished(): bool =
    dsIter.finished

  proc dispose(): Future[?!void] {.
      async: (raises: [CancelledError], raw: true), gcsafe
  .} =
    dsIter.dispose()

  success QueryIter[T].new(next = next, finished = isFinished, dispose = dispose)

# =============================================================================
# Typed Retry Helpers
# =============================================================================

proc tryPut*[T](
    self: KVStore,
    records: seq[Record[T]],
    maxRetries = 3,
    middleware: Middleware[Record[T]] = nil,
): Future[?!seq[Record[T]]] {.async: (raises: [CancelledError]).} =
  ## Bulk put with retry on conflicts for typed records
  ## Returns a list containing failed records, or empty list on success
  ##
  ## If middleware is provided, calls it with failed records to resolve conflicts
  ##

  if records.len == 0:
    return success(newSeq[Record[T]]())

  # Convert to raw for base operation
  let rawRecords = records.mapIt(it.toRaw)

  # Create raw middleware wrapper if typed middleware provided
  var rawMiddleware: RawMiddleware = nil
  if not middleware.isNil:
    rawMiddleware = proc(
        failed: seq[RawRecord]
    ): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
      # Call typed middleware
      let resolved = ?(await middleware(failed.mapIt(?toRecord[T](it))))

      # Convert back to raw
      success resolved.mapIt(it.toRaw)

  # Call raw tryPut
  let failedRaw = ?(await helpers.tryPut(self, rawRecords, maxRetries, rawMiddleware))

  # Convert failed records back to typed
  return success(failedRaw.mapIt(?toRecord[T](it)))

proc tryPut*[T](
    self: KVStore,
    record: Record[T],
    maxRetries = 3,
    middleware: Middleware[Record[T]] = nil,
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record wrapper for tryPut with typed records
  ##

  let results = ?(await self.tryPut(@[record], maxRetries, middleware))
  if results.len > 0:
    return failure newException(KVConflictError, "Unable to put record due to conflict")

  return success()

proc tryDelete*[T](
    self: KVStore,
    records: seq[Record[T]],
    maxRetries = 3,
    middleware: Middleware[Record[T]] = nil,
): Future[?!seq[Record[T]]] {.async: (raises: [CancelledError]).} =
  ## Bulk delete with retry on conflicts for typed records
  ## Returns a list containing failed records, or empty list on success
  ##
  ## If middleware is provided, calls it with failed records to resolve conflicts
  ##
  if records.len == 0:
    return success(newSeq[Record[T]]())

  # Convert to raw for base operation
  let rawRecords = records.mapIt(it.toRaw)

  # Create raw middleware wrapper if typed middleware provided
  var rawMiddleware: RawMiddleware = nil
  if not middleware.isNil:
    rawMiddleware = proc(
        failed: seq[RawRecord]
    ): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
      # Call typed middleware
      let resolved = ?(await middleware(failed.mapIt(?toRecord[T](it))))

      # Convert back to raw
      success resolved.mapIt(it.toRaw)

  # Call raw tryDelete
  let failedRaw =
    ?(await helpers.tryDelete(self, rawRecords, maxRetries, rawMiddleware))

  # Convert failed records back to typed
  return success(failedRaw.mapIt(?toRecord[T](it)))

proc tryDelete*[T](
    self: KVStore,
    record: Record[T],
    maxRetries = 3,
    middleware: Middleware[Record[T]] = nil,
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record tryDelete
  let results = ?(await self.tryDelete(@[record], maxRetries, middleware))
  if results.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")
  return success()

proc getOrPut*[T](
    self: KVStore, key: Key, producer: ValueProducer[T], maxRetries = 3
): Future[?!Record[T]] {.async: (raises: [CancelledError]).} =
  ## Get existing typed record or lazily insert using producer
  ## Producer is only called if key is missing
  ## Errors if backend fails or retries exhausted
  ##

  # Try to get first
  let existing = await get[T](self, key)
  if existing.isOk:
    return existing

  let err = existing.error
  if not (err of KVStoreKeyNotFound):
    return failure(err)

  # Key doesn't exist - produce value and try to insert
  let value = ?(await producer())
  let record = Record[T].init(key, value)
  ?(await self.tryPut(record, maxRetries))

  # Fetch the record to get the actual token
  return await get[T](self, key)
