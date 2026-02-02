{.push raises: [].}

import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./types
import ./kvstore
import ./query

# =============================================================================
# Single KVRecord Convenience Wrappers
# =============================================================================

proc has*(
    self: KVStore, keys: seq[Key]
): Future[?!seq[Key]] {.async: (raises: [CancelledError], raw: true).} =
  ## Check which keys exist.
  ## Returns a seq of keys that exist in the store.
  self.hasImpl(keys)

proc has*(
    self: KVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  ## Check if a single key exists.
  ## Convenience wrapper around batch has().
  let existing = ?(await self.has(@[key]))
  success(key in existing)

proc get*(
    self: KVStore, keys: seq[Key], T: type = seq[byte]
): Future[?!seq[KVRecord[T]]] {.async: (raises: [CancelledError]).} =
  ## Get multiple records by keys.
  ## Returns only records that exist (missing keys are silently skipped).

  let records = ?(await self.getImpl(keys))
  when T is seq[byte]:
    success records
  else:
    success records.mapIt(?toRecord[T](it))

proc get*(
    self: KVStore, key: Key, T: type = seq[byte]
): Future[?!KVRecord[T]] {.async: (raises: [CancelledError]).} =
  ## Get a single record
  ##

  let records = ?(await get(self, @[key], T))
  if records.len == 0:
    return failure newException(KVStoreKeyNotFound, "Key not found: " & $key)

  success records[0]

proc put*[T](
    self: KVStore, records: seq[KVRecord[T]]
): Future[?!seq[Key]] {.async: (raises: [CancelledError], raw: true).} =
  let rawRecords =
    when T isnot seq[byte]:
      records.mapIt(it.toRaw)
    else:
      records

  self.putImpl(rawRecords)

proc put*[T](
    self: KVStore, record: KVRecord[T]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Insert or update a single record
  ##
  ## Errors if conflict occurs
  ##

  let skipped = ?(await self.put(@[record]))
  if skipped.len > 0:
    return failure newException(KVConflictError, "Unable to put record due to conflict")

  return success()

proc put*[T](
    self: KVStore, key: Key, val: T
): Future[?!void] {.async: (raises: [CancelledError], raw: true).} =
  ## Insert or update a single record
  ##
  ## Errors if conflict occurs
  ##

  self.put(KVRecord[T].init(key, val))

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

  let skipped = ?(await self.deleteImpl(@[record]))
  if skipped.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")

  success()

# RawRecord convenience overloads - just extract key+token, no conversion
proc delete*[T](
    self: KVStore, records: seq[KVRecord[T]]
): Future[?!seq[Key]] {.async: (raw: true, raises: [CancelledError]).} =
  let keyRecords = when T is void: records else: records.toKeyRecord

  self.deleteImpl(keyRecords)

proc delete*[T](
    self: KVStore, record: KVRecord[T]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  let skipped = ?(await self.delete(@[record]))
  if skipped.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")
  success()

proc contains*(
    self: KVStore, key: Key
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Check if a key exists in the store
  ## Errors are treated as "not found" to support `key in store` syntax.
  (await has(self, key)).valueOr:
    false

# =============================================================================
# Atomic Single KVRecord Wrappers
# =============================================================================

proc putAtomic*[T](
    self: KVStore, records: seq[KVRecord[T]]
): Future[?!seq[Key]] {.async: (raises: [CancelledError], raw: true).} =
  ## Atomic batch put - all succeed or all fail
  let rawRecords =
    when T isnot seq[byte]:
      records.mapIt(it.toRaw)
    else:
      records

  self.putAtomicImpl(rawRecords)

proc putAtomic*[T](
    self: KVStore, record: KVRecord[T]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single record is trivially atomic - convenience wrapper.
  let skipped = ?(await self.putAtomic(@[record]))
  if skipped.len > 0:
    return failure newException(KVConflictError, "Unable to put record due to conflict")
  return success()

proc putAtomic*[T](
    self: KVStore, key: Key, val: T
): Future[?!void] {.async: (raises: [CancelledError], raw: true).} =
  ## Convenience: atomically insert or update a typed value at key
  self.putAtomic(KVRecord[T].init(key, val))

proc putAtomic*(
    self: KVStore, key: Key, value: seq[byte]
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  ## Convenience: atomically insert or update raw bytes at key (token=0, insert-only semantics)
  self.putAtomic(RawRecord.init(key, value))

proc deleteAtomic*(
    self: KVStore, record: KeyRecord
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single record delete - convenience wrapper.
  let skipped = ?(await self.deleteAtomicImpl(@[record]))
  if skipped.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")
  success()

# RawRecord overloads for deleteAtomic
proc deleteAtomic*[T](
    self: KVStore, records: seq[KVRecord[T]]
): Future[?!seq[Key]] {.async: (raw: true, raises: [CancelledError]).} =
  let keyRecords = when T is void: records else: records.toKeyRecord
  self.deleteAtomicImpl(keyRecords)

proc deleteAtomic*[T](
    self: KVStore, record: KVRecord[T]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  let skipped = ?(await self.deleteAtomic(@[record]))
  if skipped.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")

  success()

# =============================================================================
# Retry Helpers
# =============================================================================

proc tryPut*[T](
    self: KVStore,
    records: seq[KVRecord[T]],
    maxRetries = 3,
    middleware: Middleware[T] = nil,
): Future[?!seq[KVRecord[T]]] {.async: (raises: [CancelledError]).} =
  ## Bulk put with retry on conflicts
  ## Returns a list containing failed records, or empty list on success
  ##
  ## If middleware is provided, calls it with failed records to resolve conflicts.
  ##

  if records.len == 0:
    return success(newSeq[KVRecord[T]]())

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

proc tryPut*[T](
    self: KVStore, record: KVRecord[T], maxRetries = 3, middleware: Middleware[T] = nil
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record wrapper for tryPut
  ##

  let results = ?(await self.tryPut(@[record], maxRetries, middleware))
  if results.len > 0:
    return failure newException(KVConflictError, "Unable to put record due to conflict")

  return success()

proc tryDelete*(
    self: KVStore,
    records: seq[KeyRecord],
    maxRetries = 3,
    middleware: KeyMiddleware = nil,
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
proc tryDelete*[T](
    self: KVStore, records: seq[KVRecord[T]], maxRetries = 3, middleware: Middleware[T]
): Future[?!seq[KVRecord[T]]] {.async: (raises: [CancelledError]).} =
  ## Bulk delete with retry
  if records.len == 0:
    return success(newSeq[KVRecord[T]]())

  var
    remaining = maxRetries
    records = records

  while true:
    if remaining == 0:
      return
        failure newException(KVStoreMaxRetriesError, "tryDelete max retries reached")

    # Convert to KeyRecord ONLY for the delete call
    let keys = ?(await self.delete(records))
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

proc tryDelete*[T](
    self: KVStore, record: KVRecord[T], maxRetries = 3, middleware: Middleware[T] = nil
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Single-record tryDelete - value is ignored (no encode/decode)
  let results = ?(await self.tryDelete(@[record], maxRetries, middleware))
  if results.len > 0:
    return
      failure newException(KVConflictError, "Unable to delete record due to conflict")
  return success()

proc getOrPut*[T](
    self: KVStore, key: Key, producer: ValueProducer[T], maxRetries = 3
): Future[?!KVRecord[T]] {.async: (raises: [CancelledError]).} =
  ## Get existing record or lazily insert using producer
  ## Producer is only called if key is missing
  ## Errors if backend fails or retries exhausted
  ##

  # Try to get first
  let existing = await self.get(key, T)
  if existing.isOk:
    return existing

  let err = existing.error
  if not (err of KVStoreKeyNotFound):
    return failure(err)

  # Key doesn't exist - produce value and try to insert
  let value = ?(await producer())
  ?(await self.tryPut(KVRecord[T].init(key, value), maxRetries))

  # Fetch the record to get the actual token
  return await self.get(key, T)

# =============================================================================
# Atomic Retry Helpers
# =============================================================================

proc tryPutAtomic*[T](
    self: KVStore,
    records: seq[KVRecord[T]],
    maxRetries = 3,
    middleware: AtomicMiddleware[T] = nil,
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

proc tryPutAtomic*[T](
    self: KVStore,
    record: KVRecord[T],
    maxRetries = 3,
    middleware: AtomicMiddleware[T] = nil,
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
# Query Helpers
# =============================================================================

proc query*(
    self: KVStore, q: Query, T: type = seq[byte]
): Future[?!QueryIter[T]] {.async: (raises: [CancelledError]).} =
  when T is seq[byte]:
    await self.queryImpl(q)
  else:
    let dsIter = ?(await self.queryImpl(q))
    proc next(): Future[?!(?KVRecord[T])] {.async: (raises: [CancelledError]).} =
      let rawOpt = ?(await dsIter.next())
      without raw =? rawOpt:
        return success KVRecord[T].none

      let decoded = ?(toRecord[T](raw))
      success decoded.some

    proc isFinished(): bool =
      dsIter.finished

    proc isDisposed(): bool =
      dsIter.disposed

    proc dispose(): Future[?!void] {.async: (raises: [], raw: true), gcsafe.} =
      dsIter.dispose()

    success QueryIter[T].new(
      next = next, finished = isFinished, disposed = isDisposed, dispose = dispose
    )

proc fetchAll*[T](
    iter: QueryIter[T]
): Future[?!seq[KVRecord[T]]] {.async: (raises: [CancelledError]).} =
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
  var res: seq[KVRecord[T]]
  while not iter.finished:
    let recordOpt = ?await iter.next()
    if record =? recordOpt:
      res.add(record)
    else:
      break # End of stream
  return success(res)

proc close*(self: KVStore): Future[?!void] {.async: (raises: [], raw: true).} =
  self.closeImpl()
