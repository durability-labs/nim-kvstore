import std/options
import std/sugar
import std/tables
import std/strutils

import pkg/asynctest/chronos/unittest2
import pkg/chronos
import pkg/stew/byteutils
import pkg/stew/endians2
import pkg/questionable
import pkg/questionable/results

import pkg/datastore

proc encode(i: int): seq[byte] =
  @(cast[uint64](i).toBytesBE)

proc decode(T: type int, bytes: seq[byte]): ?!T =
  if bytes.len >= sizeof(uint64):
    success(cast[int](uint64.fromBytesBE(bytes)))
  else:
    failure("not enough bytes to decode int")

proc encode(s: string): seq[byte] =
  s.toBytes()

proc decode(T: type string, bytes: seq[byte]): ?!T =
  success(string.fromBytes(bytes))

proc typedDsTests*(ds: Datastore, key: Key) =

  var record: Record[int]
  test "should put a value":
    (await ds.put(key, 11)).tryGet()
    record = (await ds.get[:int](key)).tryGet()

  test "should get the value":
    record = ((await get[int](ds, key)).tryGet())
    check 11 == record.val

  test "put overwrites existing value":
    record.val = 34
    (await ds.put(record)).tryGet
    record = (await get[int](ds, key)).tryGet()
    check 34 == record.val

  test "delete removes value":
    (await ds.delete(record)).tryGet
    check not (await ds.has(key)).tryGet()

  test "get reports missing":
    let missing = await get[int](ds, key)
    check missing.isErr and missing.error of DatastoreKeyNotFound

proc typedDsQueryTests*(ds: Datastore) =
  discard

  test "should query values":
    let
      source = {"a": 11, "b": 22, "c": 33, "d": 44}.toTable
      Root = Key.init("/querytest").tryGet()

    for k, v in source:
      let key = (Root / k).tryGet()
      (await ds.put(key, v)).tryGet()

    let iter = (await query[int](ds, Query.init(Root))).tryGet()

    var results = initTable[string, int]()

    while not iter.finished:
      let item = (await iter.next()).tryGet()

      without record =? item:
        continue

      let
        key = record.key
        value = record.val

      check:
        key.value notin results

      results[key.value] = value

    check:
      results == source
