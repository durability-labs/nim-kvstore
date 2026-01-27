{.push raises: [].}

import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./types
import ./kvstore
import ./query

export kvstore
export query

# =============================================================================
# Single Record Convenience Wrappers
# =============================================================================

proc has*(
    self: KVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  ## Check if a single key exists.
  ## Convenience wrapper around batch has().
  let existing = ?(await self.has(@[key]))
  success(key in existing)

proc get*(
    self: KVStore, key: Key
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
  ## Get a single record
  ##

  let records = ?(await self.get(@[key]))
  if records.len == 0:
    return failure newException(KVStoreKeyNotFound, "Key not found: " & $key)

  return success records[0]

proc put*(
    self: KVStore, record: RawRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Insert or update a single record
  ##
  ## Errors if conflict occurs
  ##

  let res = ?(await self.put(@[record]))
  if res.len > 0:
    return failure newException(KVConflictError, "Unable to put record due to conflict")

  return success()

proc put*(
    self: KVStore, key: Key, value: seq[byte]
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  ## Convenience: insert or update raw bytes at key (token=0, insert-only semantics)
  self.put(RawRecord.init(key, value))

proc delete*(
    self: KVStore, record: KeyRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Delete a single record
  ##
  ## Errors if conflict occurs
  ##

  let skipped = ?(await self.delete(@[record]))
  if skipped.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")

  success()

# RawRecord convenience overloads - just extract key+token, no conversion
proc delete*(
    self: KVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raw: true, raises: [CancelledError]).} =
  self.delete(records.toKeyRecord)

proc delete*(
    self: KVStore, record: RawRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  let skipped = ?(await self.delete(@[record]))
  if skipped.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")
  success()

# =============================================================================
# Atomic Single Record Wrappers
# =============================================================================

proc putAtomic*(
    self: KVStore, record: RawRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single record is trivially atomic - convenience wrapper.
  let res = ?(await self.putAtomic(@[record]))
  if res.len > 0:
    return failure newException(KVConflictError, "Unable to put record due to conflict")
  return success()

proc deleteAtomic*(
    self: KVStore, record: KeyRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single record delete - convenience wrapper.
  let skipped = ?(await self.deleteAtomic(@[record]))
  if skipped.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")
  success()

# RawRecord overloads for deleteAtomic
proc deleteAtomic*(
    self: KVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raw: true, raises: [CancelledError]).} =
  self.deleteAtomic(records.toKeyRecord)

proc deleteAtomic*(
    self: KVStore, record: RawRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  let skipped = ?(await self.deleteAtomic(@[record]))
  if skipped.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")
  success()

# =============================================================================
# Retry Helpers
# =============================================================================

proc tryPut*(
    self: KVStore, records: seq[RawRecord], maxRetries = 3, middleware: RawMiddleware
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  ## Bulk put with retry on conflicts
  ## Returns a list containing failed records, or empty list on success
  ##
  ## If middleware is provided, calls it with failed records to resolve conflicts.
  ##

  if records.len == 0:
    return success(newSeq[RawRecord]())

  var
    remaining = maxRetries
    records = records

  while true:
    if remaining == 0:
      return failure newException(KVStoreMaxRetriesError, "tryPut max retries reached")

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
    self: KVStore, record: RawRecord, maxRetries = 3, middleware: RawMiddleware = nil
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record wrapper for tryPut
  ##

  let results = ?(await self.tryPut(@[record], maxRetries, middleware))
  if results.len > 0:
    return failure newException(KVConflictError, "Unable to put record due to conflict")

  return success()

proc tryDelete*(
    self: KVStore, records: seq[KeyRecord], maxRetries = 3, middleware: KeyMiddleware
): Future[?!seq[KeyRecord]] {.async: (raises: [CancelledError]).} =
  ## Bulk delete with retry on conflicts
  ## Returns a list containing failed records, or empty list on success
  ##
  ## If middleware is provided, calls it with failed records to resolve conflicts.
  ##

  if records.len == 0:
    return success(newSeq[KeyRecord]())

  var
    remaining = maxRetries
    records = records

  while true:
    if remaining == 0:
      return
        failure newException(KVStoreMaxRetriesError, "tryDelete max retries reached")

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
    self: KVStore, record: KeyRecord, maxRetries = 3, middleware: KeyMiddleware = nil
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record wrapper for tryDelete
  ##

  let results = ?(await self.tryDelete(@[record], maxRetries, middleware))
  if results.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")

  return success()

# RawRecord tryDelete - middleware works with RawRecord, no conversion
proc tryDelete*(
    self: KVStore, records: seq[RawRecord], maxRetries = 3, middleware: RawMiddleware
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  ## Bulk delete with retry - value passes through untouched (no encode/decode)
  if records.len == 0:
    return success(newSeq[RawRecord]())

  var
    remaining = maxRetries
    records = records

  while true:
    if remaining == 0:
      return
        failure newException(KVStoreMaxRetriesError, "tryDelete max retries reached")

    # Convert to KeyRecord ONLY for the delete call
    let keys = ?(await self.delete(records.toKeyRecord))
    records = records.filterIt(it.key in keys)
    if records.len == 0:
      break

    # Middleware receives RawRecords directly - no conversion
    if not middleware.isNil:
      records = ?(await middleware(records))

    if records.len == 0:
      break

    dec remaining

  return success records

proc tryDelete*(
    self: KVStore, record: RawRecord, maxRetries = 3, middleware: RawMiddleware = nil
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record tryDelete - value is ignored (no encode/decode)
  let results = ?(await self.tryDelete(@[record], maxRetries, middleware))
  if results.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")
  return success()

proc getOrPut*(
    self: KVStore, key: Key, producer: RawValueProducer, maxRetries = 3
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
  if not (err of KVStoreKeyNotFound):
    return failure(err)

  # Key doesn't exist - produce value and try to insert
  let value = ?(await producer())
  ?(await self.tryPut(RawRecord.init(key, value), maxRetries))

  # Fetch the record to get the actual token
  return await self.get(key)

# =============================================================================
# Atomic Retry Helpers
# =============================================================================

proc tryPutAtomic*(
    self: KVStore,
    records: seq[RawRecord],
    maxRetries = 3,
    middleware: RawAtomicMiddleware = nil,
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Atomic batch put with retry on conflicts.
  ##
  ## Unlike tryPut (partial commit), this uses all-or-nothing semantics:
  ## - Each iteration attempts ALL records
  ## - On conflict, NOTHING is committed
  ## - Middleware receives full batch + conflict keys to update tokens
  ##
  ## Returns success() if all records committed, failure otherwise.

  if records.len == 0:
    return success()

  if not self.supportsAtomicBatch():
    return failure newException(
      KVStoreBackendError, "Atomic batch not supported by this backend"
    )

  var
    remaining = maxRetries
    current = records

  while remaining > 0:
    let conflicts = ?(await self.putAtomic(current))
    if conflicts.len == 0:
      return success()

    # Prepare next attempt using middleware
    if middleware.isNil:
      return failure newException(
        KVStoreError,
        "Atomic put failed: conflicts on " & $conflicts.len &
          " keys, no middleware to resolve",
      )

    current = ?(await middleware(current, conflicts))
    if current.len == 0:
      return failure newException(KVStoreError, "Middleware returned empty batch")

    dec remaining

  return
    failure newException(KVStoreMaxRetriesError, "tryPutAtomic max retries reached")

proc tryPutAtomic*(
    self: KVStore,
    record: RawRecord,
    maxRetries = 3,
    middleware: RawAtomicMiddleware = nil,
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  ## Single-record wrapper for tryPutAtomic
  self.tryPutAtomic(@[record], maxRetries, middleware)

proc tryDeleteAtomic*(
    self: KVStore,
    records: seq[KeyRecord],
    maxRetries = 3,
    middleware: KeyAtomicMiddleware = nil,
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Atomic batch delete with retry on conflicts.
  ## Same semantics as tryPutAtomic().

  if records.len == 0:
    return success()

  if not self.supportsAtomicBatch():
    return failure newException(
      KVStoreBackendError, "Atomic batch not supported by this backend"
    )

  var
    remaining = maxRetries
    current = records

  while remaining > 0:
    let conflicts = ?(await self.deleteAtomic(current))
    if conflicts.len == 0:
      return success()

    # Prepare next attempt using middleware
    if middleware.isNil:
      return failure newException(
        KVStoreError,
        "Atomic delete failed: conflicts on " & $conflicts.len &
          " keys, no middleware to resolve",
      )

    current = ?(await middleware(current, conflicts))
    if current.len == 0:
      return failure newException(KVStoreError, "Middleware returned empty batch")

    dec remaining

  return
    failure newException(KVStoreMaxRetriesError, "tryDeleteAtomic max retries reached")

proc tryDeleteAtomic*(
    self: KVStore,
    record: KeyRecord,
    maxRetries = 3,
    middleware: KeyAtomicMiddleware = nil,
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  ## Single-record wrapper for tryDeleteAtomic
  self.tryDeleteAtomic(@[record], maxRetries, middleware)

# =============================================================================
# Query Iterator Helpers
# =============================================================================

proc fetchAll*[T](
    iter: QueryIter[T]
): Future[?!seq[Record[T]]] {.async: (raises: [CancelledError]).} =
  ## Collect all records from an iterator into a seq.
  ##
  ## This is the correct way to collect results from an async iterator.
  ## Unlike `toSeq(iter)`, this properly awaits each `next()` call,
  ## ensuring correct ordering and avoiding infinite loops.
  ##
  ## Example:
  ##   let iter = (await ds.query(q)).tryGet
  ##   let records = (await iter.fetchAll()).tryGet
  ##
  var res: seq[Record[T]]
  while not iter.finished:
    let recordOpt = ?await iter.next()
    if record =? recordOpt:
      res.add(record)
    else:
      break # End of stream
  return success(res)
