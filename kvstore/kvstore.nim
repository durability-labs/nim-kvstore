{.push raises: [].}

import pkg/chronos
import pkg/questionable/results

import ./key
import ./types
import ./query

export types

# =============================================================================
# Base Interface Methods
# =============================================================================

method has*(
    self: KVStore, key: Key
): Future[?!bool] {.base, gcsafe, async: (raises: [CancelledError]).} =
  raiseAssert("Not implemented!")

method get*(
    self: KVStore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Get a list of records specified by the keys
  ##

  raiseAssert("Not implemented!")

method put*(
    self: KVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Insert or update a group of records
  ##
  ## The sequence will contain records that couldn't be inserted/updated
  ## due to conflicts.
  ##

  raiseAssert("Not implemented!")

method delete*(
    self: KVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Delete a list of records
  ##
  ## Returns a sequence of keys that were skipped due to conflicts.
  ##
  raiseAssert("Not implemented!")

method close*(self: KVStore): Future[?!void] {.base, async: (raises: []).} =
  raiseAssert("Not implemented!")

method query*(
    self: KVStore, query: Query
): Future[?!QueryIterRaw] {.base, gcsafe, async: (raises: [CancelledError]).} =
  raiseAssert("Not implemented!")

# =============================================================================
# Atomic Batch API
# =============================================================================

method supportsAtomicBatch*(self: KVStore): bool {.base, gcsafe.} =
  ## Returns true if this backend supports atomic batch operations.
  ##
  ## Backends that return true guarantee putAtomic/deleteAtomic will either:
  ## - Commit ALL records atomically (empty conflict list)
  ## - Commit NONE and report conflicts (non-empty conflict list)
  false

method putAtomic*(
    self: KVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Insert or update records atomically (all-or-nothing).
  ##
  ## If ANY record has a CAS conflict, NO records are committed.
  ##
  ## Returns:
  ##   - Success with empty seq: All records committed atomically
  ##   - Success with non-empty seq: These keys had conflicts; NOTHING committed
  ##   - Failure: Backend error or atomic not supported
  return failure newException(
    KVStoreBackendError, "Atomic batch not supported by this backend"
  )

method deleteAtomic*(
    self: KVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Delete records atomically (all-or-nothing).
  ## Same semantics as putAtomic().
  return failure newException(
    KVStoreBackendError, "Atomic batch not supported by this backend"
  )
