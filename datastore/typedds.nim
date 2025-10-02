{.push raises: [].}

import std/options
import std/macros

import pkg/questionable
import pkg/questionable/results
import pkg/chronos
import pkg/chronos/futures

import ./datastore
import ./types
import ./key

## Wrapper for Datastore with basic functionality of automatically converting
## stored values from some user defined type `T` to `seq[byte]` and vice-versa.
##
## To use this API you need to provide decoder and encoder procs.
##
## Basic usage
## ==================
## .. code-block:: Nim
##   import pkg/stew/byteutils
##   import pkg/questionable/results
##
##   let
##     tds = TypeDatastore.init(ds)
##     key = Key.init("p").tryGet()
##
##   type Person = object
##     age: int
##     name: string
##
##   proc encode(p: Person): seq[byte] =
##     ($p.age & ":" & p.name).toBytes()
##   proc decode(T: type Person, bytes: seq[byte]): ?!T =
##     let values = string.fromBytes(bytes).split(':', maxsplit = 1)
##     success(Person(age: parseInt(values[0]), name: values[1]))
##
##   let p1 = Person(name: "john", age: 21)
##   (await tds.put(key, p1)).tryGet()
##   let p2 = (await get[Person](tds, key)).tryGet()
##
##   assert p1 == p2

type
  TypedDatastore* = ref object of RootObj
    ds*: Datastore

  Modify*[T] = proc(v: ?T): Future[?T] {.raises: [CancelledError], gcsafe, closure.}
  ModifyGet*[T, U] = proc(v: ?T): Future[(?T, U)] {.raises: [CancelledError], gcsafe, closure.}

  QueryResponse*[T] = tuple[key: ?Key, value: ?!T]
  GetNext*[T] = proc(): Future[?!QueryResponse[T]] {.async: (raises: [CancelledError]), gcsafe, closure.}
  QueryIter*[T] = ref object
    nextImpl: GetNext[T]
    finishedImpl: IterFinished
    disposeImpl: IterDispose

export types, key, IterDispose, Key, Query, SortOrder, QueryEndedError

# Helpers
template requireDecoder*(T: typedesc): untyped =
  when not (compiles do:
    let _: ?!T = T.decode(newSeq[byte]())):
    {.error: "provide a decoder: `proc decode(T: type " & $T & ", bytes: seq[byte]): ?!T`".}

template requireEncoder*(T: typedesc): untyped =
  when not (compiles do:
    let _: seq[byte] = encode(default(T))):
    {.error: "provide an encoder: `proc encode(a: " & $T & "): seq[byte]`".}

# Original Datastore API
proc has*(self: TypedDatastore, key: Key): Future[?!bool] {.async: (raises: [CancelledError]).} =
  await self.ds.has(key)

proc contains*(self: TypedDatastore, key: Key): Future[bool] {.async: (raises: [CancelledError]).} =
  return (await self.ds.has(key)) |? false

proc delete*(self: TypedDatastore, key: Key): Future[?!void] {.async: (raises: [CancelledError]).} =
  await self.ds.delete(key)

proc delete*(self: TypedDatastore, keys: seq[Key]): Future[?!void] {.async: (raises: [CancelledError]).} =
  await self.ds.delete(keys)

proc close*(self: TypedDatastore): Future[?!void] {.async.} =
  await self.ds.close()

# TypedDatastore API
proc init*(T: type TypedDatastore, ds: Datastore): T =
  TypedDatastore(ds: ds)

proc put*[T](self: TypedDatastore, key: Key, t: T): Future[?!void] {.async: (raises: [CancelledError]).} =
  requireEncoder(T)

  await self.ds.put(key, t.encode)

proc get*[T](self: TypedDatastore, key: Key): Future[?!T] {.async: (raises: [CancelledError]).} =
  requireDecoder(T)

  without bytes =? await self.ds.get(key), error:
    return failure(error)
  return T.decode(bytes)

proc modify*[T](self: TypedDatastore, key: Key, fn: Modify[T]): Future[?!void] {.async: (raises: [CancelledError]).} =
  requireDecoder(T)
  requireEncoder(T)

  proc wrappedFn(maybeBytes: ?seq[byte]): Future[?seq[byte]] {.async.} =
    var
      maybeNextT: ?T
    if bytes =? maybeBytes:
      without t =? T.decode(bytes), error:
        raise error
      maybeNextT = await fn(t.some)
    else:
      maybeNextT = await fn(T.none)

    if nextT =? maybeNextT:
      return nextT.encode().some
    else:
      return seq[byte].none

  await self.ds.modify(key, wrappedFn)

proc modifyGet*[T, U](self: TypedDatastore, key: Key, fn: ModifyGet[T, U]): Future[?!U] {.async: (raises: [CancelledError]).} =
  requireDecoder(T)
  requireEncoder(T)
  requireEncoder(U)
  requireDecoder(U)

  proc wrappedFn(maybeBytes: ?seq[byte]): Future[(Option[seq[byte]], seq[byte])] {.async.} =
    var
      maybeNextT: ?T
      aux: U
    if bytes =? maybeBytes:
      without t =? T.decode(bytes), error:
        raise error

      (maybeNextT, aux) = await fn(t.some)
    else:
      (maybeNextT, aux) = await fn(T.none)

    if nextT =? maybeNextT:
      let b: seq[byte] = nextT.encode()
      return (b.some, aux.encode())
    else:
      return (seq[byte].none, aux.encode())

  without auxBytes =? await self.ds.modifyGet(key, wrappedFn), error:
    return failure(error)


  return U.decode(auxBytes)

proc query*[T](self: TypedDatastore, q: Query): Future[?!QueryIter[T]] {.async: (raises: [CancelledError]).} =
  requireDecoder(T)

  without dsIter =? await self.ds.query(q), error:
    let childErr = newException(CatchableError, "Error executing query with key " & $q.key, parentException = error)
    return failure(childErr)

  proc next: Future[?!QueryResponse[T]] {.async: (raises: [CancelledError]).} =
    without pair =? await dsIter.next(), error:
      return failure(error)

    return success((key: pair.key, value: T.decode(pair.data)))

  proc isFinished: bool =
    dsIter.finished

  proc dispose =
    dsIter.dispose()

  return success QueryIter[T](
    nextImpl: next,
    finishedImpl: isFinished,
    disposeImpl: dispose
  )

proc next*[T](iter: QueryIter[T]): Future[?!QueryResponse[T]] {.async: (raises: [CancelledError]).} =
  await iter.nextImpl()

proc finished*[T](iter: QueryIter[T]): bool =
  iter.finishedImpl()

proc dispose*[T](iter: QueryIter[T]) =
  iter.disposeImpl()
