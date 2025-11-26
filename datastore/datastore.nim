{.push raises: [].}

import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./types
import ./query

export types

proc init*(_: type RawRecord, key: Key, val: seq[byte], token = 0'u64): RawRecord =
  RawRecord(key: key, val: val, token: token)

proc newBackendError*(msg: string): ref DatastoreBackendError =
  newException(DatastoreBackendError, msg)

proc newCorruptionError*(msg: string): ref DatastoreCorruption =
  newException(DatastoreCorruption, msg)

proc newMaxRetriesError*(
    msg: string = "Max retries reached"
): ref DatastoreMaxRetriesError =
  newException(DatastoreMaxRetriesError, msg)

proc withValue*[T](record: Record[T], value: T): Record[T] =
  ## Returns a copy of `record` with `value` replaced.
  Record[T].init(record.key, value, record.token)

proc withToken*[T](record: Record[T], token: uint64): Record[T] =
  ## Returns a copy of `record` with `token` replaced.
  Record[T].init(record.key, record.val, token)

method has*(
    self: Datastore, key: Key
): Future[?!bool] {.base, gcsafe, async: (raises: [CancelledError]).} =
  raiseAssert("Not implemented!")

method get*(
    self: Datastore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Get a list of records specified by the keys
  ##

  raiseAssert("Not implemented!")

method get*(
    self: Datastore, key: Key
): Future[?!RawRecord] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Get a single record
  ##

  raiseAssert("Not implemented!")

method put*(
    self: Datastore, records: seq[RawRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Insert or update a group of records
  ##
  ## The sequence will contain records that couldn't be inserted/updated
  ## due to conflicts.
  ##

  raiseAssert("Not implemented!")

proc put*(
    self: Datastore, record: RawRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Insert or update a single record
  ##
  ## The result contains the record that couldn't be inserted/updated
  ##

  let res = ?(await self.put(@[record]))
  if res.len > 0:
    return failure newException(DatastoreError, "Unable to put record due to conflict")

  return success()

method delete*(
    self: Datastore, records: seq[RawRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Delete a list of records
  ##
  ## Returns a sequence of keys that were skipped due to conflicts.
  ##
  raiseAssert("Not implemented!")

proc delete*(
    self: Datastore, record: RawRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Delete a single record
  ##
  ## Errors if conflict occurs
  ##

  let skipped = ?(await self.delete(@[record]))
  if skipped.len > 0:
    return
      failure newException(DatastoreError, "Unable to delete record due to conflict")

  success()

method close*(
    self: Datastore
): Future[?!void] {.base, async: (raises: [CancelledError]).} =
  raiseAssert("Not implemented!")

method query*(
    self: Datastore, query: Query
): Future[?!QueryIterRaw] {.base, gcsafe, async: (raises: [CancelledError]).} =
  raiseAssert("Not implemented!")

proc tryPut*(
    self: Datastore,
    records: seq[RawRecord],
    maxRetries = 3,
    middleware: RawMiddleware = nil,
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  ## Bulk put with retry on conflicts
  ## Returns a list containing failed records, or empty list on success
  ##
  ## If middleware is provided, calls it with failed records to resolve conflicts
  ## If middleware is nil, uses default: refetch tokens and retry with same values
  ##

  if records.len == 0:
    return success(newSeq[RawRecord]())

  var
    remaining = maxRetries
    records = records

  while true:
    if remaining == 0:
      return failure newMaxRetriesError("tryPut max retries reached")

    let keys = ?(await self.put(records))
    records = records.filterIt(it.key in keys)
    if records.len == 0:
      break

    # Prepare next attempt using middleware
    if not middleware.isNil:
      records = ?(await middleware(records))

    if records.len == 0:
      # Middleware gave up or all keys disappeared
      break

    dec remaining

  return success records

proc tryPut*(
    self: Datastore, record: RawRecord, maxRetries = 3, middleware: RawMiddleware = nil
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record wrapper for tryPut
  ##

  let results = ?(await self.tryPut(@[record], maxRetries, middleware))
  if results.len > 0:
    return failure newException(DatastoreError, "Unable to put record due to conflict")

  return success()

proc tryDelete*(
    self: Datastore,
    records: seq[RawRecord],
    maxRetries = 3,
    middleware: RawMiddleware = nil,
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  ## Bulk delete with retry on conflicts
  ## Returns a list containing failed records, or empty list on success
  ##
  ## If middleware is provided, calls it with failed records to resolve conflicts
  ## If middleware is nil, uses default: refetch tokens and retry with same values
  ##

  if records.len == 0:
    return success(newSeq[RawRecord]())

  var
    remaining = maxRetries
    records = records

  while true:
    if remaining == 0:
      return failure newMaxRetriesError("tryDelete max retries reached")

    let keys = ?(await self.delete(records))
    records = records.filterIt(it.key in keys)
    if records.len == 0:
      break

    # Prepare next attempt using middleware
    if not middleware.isNil:
      records = ?(await middleware(records))

    if records.len == 0:
      # Middleware gave up or all keys disappeared
      break

    dec remaining

  return success records

proc tryDelete*(
    self: Datastore, record: RawRecord, maxRetries = 3, middleware: RawMiddleware = nil
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record wrapper for tryDelete
  ##

  let results = ?(await self.tryDelete(@[record], maxRetries, middleware))
  if results.len > 0:
    return
      failure newException(DatastoreError, "Unable to delete record due to conflict")

  return success()

proc getOrPut*(
    self: Datastore, key: Key, producer: RawValueProducer, maxRetries = 3
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
  ## Get existing record or lazily insert using producer
  ## Producer is only called if key is missing
  ## Errors if backend fails or retries exhausted
  ##

  # Try to get first
  let existing = await self.get(key)
  if existing.isOk:
    return existing

  let err = existing.error
  if not (err of DatastoreKeyNotFound):
    return failure(err)

  # Key doesn't exist - produce value and try to insert
  let value = ?(await producer())
  ?(await self.tryPut(RawRecord.init(key, value), maxRetries))

  # Successfully inserted, return the record
  return success RawRecord.init(key, value)
