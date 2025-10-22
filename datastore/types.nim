{.push raises: [].}

import pkg/chronos
import pkg/questionable/results

import ./key

type
  Datastore* = ref object of RootObj

  # Core types
  Record*[T] = object
    token*: uint64
    key*: Key
    val*: T

  Middleware*[T] = proc(failed: seq[T]): Future[seq[T]] {.gcsafe, async: (raises: [CancelledError]).}
  ValueProducer*[T] = proc(): Future[T] {.gcsafe, async: (raises: [CancelledError]).}

  RawRecord* = Record[seq[byte]]
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
  when not (compiles do:
    let _: ?!T = T.decode(newSeq[byte]())):
    {.error: "provide a decoder: `proc decode(T: type " & $T & ", bytes: seq[byte]): ?!T`".}

template requireEncoder*(T: typedesc): untyped =
  when not (compiles do:
    let _: seq[byte] = encode(default(T))):
    {.error: "provide an encoder: `proc encode(a: " & $T & "): seq[byte]`".}

proc toRaw*[T](record: Record[T]): RawRecord =
  when T is seq[byte]:
    RawRecord.init(record.key, record.val, record.token)
  else:
    requireEncoder(T)
    RawRecord.init(record.key, encode(record.val), record.token)

proc toRecord*[T](record: RawRecord): ?!Record[T] =
  when T is seq[byte]:
    success Record[T].init(record.key, record.val, record.token)
  else:
    requireDecoder(T)
    let value = ?T.decode(record.val)
    success Record[T].init(record.key, value, record.token)
