import std/sequtils

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./datastore/key
import ./datastore/datastore as rawds
import ./datastore/fsds
import ./datastore/sql
import ./datastore/mountedds
import ./datastore/tieredds
import ./datastore/types
import ./datastore/query

export datastore, fsds, mountedds, tieredds, sql, types, query, key

## Typed Datastore Support
##
## Provides automatic conversion between user-defined types and seq[byte]
## using encoder/decoder procs.
##
## Basic usage:
## .. code-block:: Nim
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

# Typed operations
# Note: Converters work for single records, but not for seq[Record[T]] -> seq[RawRecord]
# So we provide explicit typed wrappers

# Typed Record constructor
proc init*[T](_: type Record[T], key: Key, val: T, token = 0'u64): Record[T] =
  Record[T](key: key, val: val, token: token)

# Typed datastore API
proc get*[T](
    self: Datastore, keys: seq[Key]
): Future[?!seq[Record[T]]] {.async: (raises: [CancelledError]).} =
  ## Get a list of records specified by the keys
  ##

  let raw = ?(await rawds.get(self, keys))
  success raw.mapIt(?toRecord[T](it))

proc get*[T](
    self: Datastore, key: Key
): Future[?!Record[T]] {.async: (raises: [CancelledError]).} =
  ## Get a single record
  ##

  toRecord[T]((?(await rawds.get(self, key))))

proc put*[T](
    self: Datastore, records: seq[Record[T]]
): Future[?!seq[Key]] {.async: (raw: true, raises: [CancelledError]).} =
  ## Insert or update a group of records
  ##
  ## The sequence will contain records that couldn't be inserted/updated
  ## due to conflicts.
  ##

  rawds.put(self, records.mapIt(it.toRaw))

proc put*[T](
    self: Datastore, record: Record[T]
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  ## Insert or update a single record
  ##
  ## The result contains the record that couldn't be inserted/updated
  ##

  rawds.put(self, record.toRaw)

proc put*[T](
    self: Datastore, key: Key, value: T
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  rawds.put(self, Record.init(key, value).toRaw)

proc delete*[T](
    self: Datastore, records: seq[Record[T]]
): Future[?!seq[Key]] {.async: (raw: true, raises: [CancelledError]).} =
  ## Delete a list of records
  ##
  ## Returns a sequence of keys that were skipped due to conflicts.
  ##

  rawds.delete(self, records.mapIt(it.toRaw))

proc delete*[T](
    self: Datastore, record: Record[T]
): Future[?!void] {.async: (raw: true, raises: [CancelledError]).} =
  ## Delete a single record
  ##
  ## Errors if conflict occurs
  ##

  rawds.delete(self, record.toRaw)

proc contains*(
    self: Datastore, key: Key
): Future[bool] {.async: (raises: [CancelledError]).} =
  return (await rawds.has(self, key)) |? false

proc query*[T](
    self: Datastore, q: Query
): Future[?!QueryIter[T]] {.async: (raises: [CancelledError]).} =
  let dsIter = ?(await rawds.query(self, q))

  proc next(): Future[?!(?Record[T])] {.async: (raises: [CancelledError]).} =
    let rawOpt = ?(await dsIter.next())
    if rawOpt.isNone:
      return success Record[T].none

    let decoded = ?(toRecord[T](rawOpt.get()))
    success decoded.some

  proc isFinished(): bool =
    dsIter.finished

  proc dispose() =
    dsIter.dispose()

  success QueryIter[T].new(next = next, finished = isFinished, dispose = dispose)
