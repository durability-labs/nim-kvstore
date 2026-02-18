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

method hasImpl*(
    self: KVStore, keys: seq[Key]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Check existence of multiple keys.
  ## Returns the subset of input keys that exist in the store.
  ##
  ## Semantics:
  ## - Result preserves input order (first-seen order)
  ## - Duplicate keys in input are deduplicated (first occurrence wins)
  raiseAssert("Not implemented!")

method getImpl*(
    self: KVStore, keys: seq[Key]
): Future[?!seq[RawKVRecord]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Get a list of records specified by the keys
  ##

  raiseAssert("Not implemented!")

method putImpl*(
    self: KVStore, records: seq[RawKVRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Insert or update a group of records
  ##
  ## The sequence will contain records that couldn't be inserted/updated
  ## due to conflicts.
  ##

  raiseAssert("Not implemented!")

method deleteImpl*(
    self: KVStore, records: seq[KeyKVRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Delete a list of records
  ##
  ## Returns a sequence of keys that were skipped due to conflicts.
  ##
  raiseAssert("Not implemented!")

method closeImpl*(self: KVStore): Future[?!void] {.base, async: (raises: []).} =
  raiseAssert("Not implemented!")

method queryImpl*(
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

method putAtomicImpl*(
    self: KVStore, records: seq[RawKVRecord]
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

method deleteAtomicImpl*(
    self: KVStore, records: seq[KeyKVRecord]
): Future[?!seq[Key]] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Delete records atomically (all-or-nothing).
  ## Same semantics as putAtomic().
  return failure newException(
    KVStoreBackendError, "Atomic batch not supported by this backend"
  )

# =============================================================================
# Move (Key-Prefix Rename)
# =============================================================================

method moveKeysAtomicImpl*(
    self: KVStore, oldPrefix, newPrefix: Key
): Future[?!void] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Move all records from oldPrefix/* to newPrefix/* atomically.
  ##
  ## Either all keys are moved or none are (full rollback).
  ## Returns KVConflictError if any destination key already exists.
  ##
  return failure newException(
    KVStoreBackendError, "Atomic move not supported by this backend"
  )

method moveKeysAtomicImpl*(
    self: KVStore, moves: seq[(Key, Key)]
): Future[?!void] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Move multiple prefix pairs atomically in a single transaction.
  ##
  ## All pairs are moved or none are (full rollback).
  ## Returns KVConflictError if any destination key already exists.
  ##
  return failure newException(
    KVStoreBackendError, "Atomic move not supported by this backend"
  )

# =============================================================================
# Drop Prefix (Destructive Bulk Delete)
# =============================================================================

method dropPrefixImpl*(
    self: KVStore, prefix: Key
): Future[?!void] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Delete all records under prefix/* and the prefix key itself, atomically.
  ##
  ## Idempotent: no-op if no matching keys exist.
  ##
  return failure newException(
    KVStoreBackendError, "dropPrefix not supported by this backend"
  )

method dropPrefixImpl*(
    self: KVStore, prefixes: seq[Key]
): Future[?!void] {.base, gcsafe, async: (raises: [CancelledError]).} =
  ## Drop multiple prefixes atomically in a single transaction.
  ##
  ## Idempotent: no-op if no matching keys exist.
  ##
  return failure newException(
    KVStoreBackendError, "dropPrefix not supported by this backend"
  )
