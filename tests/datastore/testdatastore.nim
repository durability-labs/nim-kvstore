import std/options

import pkg/asynctest/chronos/unittest2
import pkg/chronos

import pkg/datastore

suite "Datastore (base)":
  let
    key = Key.init("a").get
    ds = Datastore()

  let record = Record.init(key, @[1.byte])

  test "put":
    expect Defect: (await ds.put(record)).tryGet

  test "delete":
    expect Defect: discard (await ds.delete(record))

  test "contains":
    expect Defect: discard (await ds.has(key)).tryGet

  test "get":
    expect Defect:
      var rec: Record[seq[byte]] = (await ds.get[:seq[byte]](key)).tryGet

  test "query":
    expect Defect:
      let iter = (await ds.query[:seq[byte]](Query.init(key))).tryGet
      for n in iter: discard
