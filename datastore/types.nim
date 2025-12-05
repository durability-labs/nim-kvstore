{.push raises: [].}

import std/sequtils

import pkg/chronos
import pkg/questionable/results

import ./key

type
  Datastore* = ref object of RootObj

  # Core types
  Record*[T] = object
    token*: uint64
    key*: Key
    when T is not type void:
      val*: T

  Middleware*[T] =
    proc(failed: seq[T]): Future[?!seq[T]] {.gcsafe, async: (raises: [CancelledError]).}
  ValueProducer*[T] = proc(): Future[?!T] {.gcsafe, async: (raises: [CancelledError]).}

  RawRecord* = Record[seq[byte]]
  KeyRecord* = Record[void]
  KeyMiddleware* = Middleware[KeyRecord]
  RawMiddleware* = Middleware[RawRecord]
  RawValueProducer* = ValueProducer[seq[byte]]

  # Error types
  DatastoreError* = object of CatchableError
  DatastoreMaxRetriesError* = object of DatastoreError
  DatastoreBackendError* = object of DatastoreError
  DatastoreKeyNotFound* = object of DatastoreBackendError
  DatastoreCorruption* = object of DatastoreBackendError

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

proc toRaw*[T](record: Record[T]): RawRecord =
  when T is seq[byte]:
    RawRecord.init(record.key, record.val, record.token)
  elif T is not void:
    RawRecord.init(record.key, encode(record.val), record.token)
  else:
    RawRecord.init(record.key, newSeq[byte](), record.token)

template toRecord*[T](record: RawRecord): ?!Record[T] =
  mixin decode
  when T is seq[byte]:
    success Record[T].init(record.key, record.val, record.token)
  elif T is not void:
    let value = ?T.decode(record.val)
    success Record[T].init(record.key, value, record.token)
  else:
    success KeyRecord.init(record.key, record.token)

# KeyRecord extraction - for operations that only need key+token
template toKeyRecord*[T](record: Record[T]): KeyRecord =
  KeyRecord(key: record.key, token: record.token)

template toKeyRecord*[T](records: seq[Record[T]]): seq[KeyRecord] =
  records.mapIt(KeyRecord(key: it.key, token: it.token))
