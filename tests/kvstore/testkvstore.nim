import std/options

import pkg/asynctest/chronos/unittest2
import pkg/chronos

import pkg/kvstore

suite "KVStore (base)":
  let
    key = Key.init("a").get
    ds = KVStore()

  let record = KVRecord.init(key, @[1.byte])

  test "put":
    expect AssertionDefect:
      (await ds.put(record)).tryGet

  test "delete":
    expect AssertionDefect:
      (await ds.delete(KeyRecord.init(key, record.token))).tryGet

  test "contains":
    expect AssertionDefect:
      discard (await ds.has(key)).tryGet

  test "get":
    expect AssertionDefect:
      var rec = (await ds.get(key)).tryGet

  test "query":
    expect AssertionDefect:
      let iter = (await query(ds, Query.init(key))).tryGet
      for n in iter:
        discard
