{.push raises: [].}

import std/sequtils

import pkg/chronos
import pkg/questionable/results

import ./key

type
  KVStore* = ref object of RootObj

  # Core types
  KVRecord*[T] = object
    token*: uint64
    key*: Key
    when T isnot void:
      val*: T

  Middleware*[T] = proc(failed: seq[KVRecord[T]]): Future[?!seq[KVRecord[T]]] {.
    gcsafe, async: (raises: [CancelledError])
  .}
  ValueProducer*[T] = proc(): Future[?!T] {.gcsafe, async: (raises: [CancelledError]).}

  # Atomic batch middleware receives ALL records + conflict keys
  # This allows it to refresh tokens for the entire batch, not just failed records
  AtomicMiddleware*[T] = proc(
    allRecords: seq[KVRecord[T]], conflicts: seq[Key]
  ): Future[?!seq[KVRecord[T]]] {.gcsafe, async: (raises: [CancelledError]).}

  RawRecord* = KVRecord[seq[byte]]
  KeyRecord* = KVRecord[void]
  KeyMiddleware* = Middleware[void]
  KeyAtomicMiddleware* = AtomicMiddleware[void]

  # Error types
  KVStoreError* = object of CatchableError
  KVConflictError* = object of KVStoreError
  KVStoreMaxRetriesError* = object of KVStoreError
  KVStoreBackendError* = object of KVStoreError
  KVStoreKeyNotFound* = object of KVStoreBackendError
  KVStoreCorruption* = object of KVStoreBackendError

# =============================================================================
# KVRecord Constructors
# =============================================================================

proc init*[T](_: type KVRecord[T], key: Key, val: T, token = 0'u64): KVRecord[T] =
  KVRecord[T](key: key, val: val, token: token)

proc init*[void](_: type KVRecord[void], key: Key, token = 0'u64): KVRecord[void] =
  KVRecord[void](key: key, token: token)

# Encoder/decoder requirements
template requireDecoder*(T: typedesc): untyped =
  when not (compiles (let _: ?!T = T.decode(newSeq[byte]()))):
    {.
      error:
        "provide a decoder: `proc decode(T: type " & $T & ", bytes: seq[byte]): ?!T`"
    .}

template requireEncoder*(T: typedesc): untyped =
  when not (compiles (let _: seq[byte] = encode(default(T)))):
    {.error: "provide an encoder: `proc encode(a: " & $T & "): seq[byte]`".}

proc toRaw*[T](record: KVRecord[T]): RawRecord =
  when T is seq[byte]:
    record
  elif T is not void:
    requireEncoder(T)
    mixin encode
    RawRecord.init(record.key, encode(record.val), record.token)
  else:
    RawRecord.init(record.key, newSeq[byte](), record.token)

template toRecord*[T](record: RawRecord): ?!KVRecord[T] =
  when T is seq[byte]:
    success record
  elif T is not void:
    requireDecoder(T)
    mixin decode
    success KVRecord[T].init(record.key, ?T.decode(record.val), record.token)
  else:
    success KeyRecord.init(record.key, record.token)

template toRecord*(T: type, record: RawRecord): ?!KVRecord[T] =
  toRecord[T](record)

# KeyRecord extraction - for operations that only need key+token
template toKeyRecord*[T](record: KVRecord[T]): KeyRecord =
  when T is void:
    record
  else:
    KeyRecord(key: record.key, token: record.token)

template toKeyRecord*[T](records: seq[KVRecord[T]]): seq[KeyRecord] =
  when T is void:
    records
  else:
    records.mapIt(KeyRecord(key: it.key, token: it.token))
