import pkg/datastore

template queryTests*(
    ds: Datastore, testLimitsAndOffsets = true, testSortOrder = true
) {.dirty.} =
  var
    key1: Key
    key2: Key
    key3: Key
    val1: seq[byte]
    val2: seq[byte]
    val3: seq[byte]

  setupAll:
    key1 = Key.init("/a").tryGet
    key2 = Key.init("/a/b").tryGet
    key3 = Key.init("/a/b/c").tryGet
    val1 = "value for 1".toBytes
    val2 = "value for 2".toBytes
    val3 = "value for 3".toBytes

  test "Key should query all keys and all it's children":
    let q = Query.init(key1)

    (await ds.put(RawRecord.init(key1, val1))).tryGet()
    (await ds.put(RawRecord.init(key2, val2))).tryGet()
    (await ds.put(RawRecord.init(key3, val3))).tryGet()

    let
      iter = (await ds.query(q)).tryGet
      res = (await allFinished(toSeq(iter)))
        .mapIt(it.read.tryGet)
        .filterIt(it.isSome)
        .mapIt(it.get)

    check:
      res.len == 3
      res[0].key == key1
      res[0].val == val1

      res[1].key == key2
      res[1].val == val2

      res[2].key == key3
      res[2].val == val3

  test "Key should query all keys without values":
    let q = Query.init(key1, value = false)

    (await ds.put(RawRecord.init(key1, val1))).tryGet()
    (await ds.put(RawRecord.init(key2, val2))).tryGet()
    (await ds.put(RawRecord.init(key3, val3))).tryGet()

    let
      iter = (await ds.query(q)).tryGet
      res = (await allFinished(toSeq(iter)))
        .mapIt(it.read.tryGet)
        .filterIt(it.isSome)
        .mapIt(it.get)

    check:
      res.len == 3
      res[0].key == key1
      res[0].val.len == 0

      res[1].key == key2
      res[1].val.len == 0

      res[2].key == key3
      res[2].val.len == 0

  test "Key should not query parent":
    let q = Query.init(key2)

    (await ds.put(RawRecord.init(key1, val1))).tryGet()
    (await ds.put(RawRecord.init(key2, val2))).tryGet()
    (await ds.put(RawRecord.init(key3, val3))).tryGet()

    let
      iter = (await ds.query(q)).tryGet
      res = (await allFinished(toSeq(iter)))
        .mapIt(it.read.tryGet)
        .filterIt(it.isSome)
        .mapIt(it.get)

    check:
      res.len == 2
      res[0].key == key2
      res[0].val == val2

      res[1].key == key3
      res[1].val == val3

  test "Key should all list all keys at the same level":
    let
      queryKey = Key.init("/a").tryGet
      q = Query.init(queryKey)

    (await ds.put(RawRecord.init(key1, val1))).tryGet()
    (await ds.put(RawRecord.init(key2, val2))).tryGet()
    (await ds.put(RawRecord.init(key3, val3))).tryGet()

    let iter = (await ds.query(q)).tryGet

    var res = (await allFinished(toSeq(iter)))
      .mapIt(it.read.tryGet)
      .filterIt(it.isSome)
      .mapIt(it.get)

    res.sort do(a, b: RawRecord) -> int:
      cmp(a.key.id, b.key.id)

    check:
      res.len == 3
      res[0].key == key1
      res[0].val == val1

      res[1].key == key2
      res[1].val == val2

      res[2].key == key3
      res[2].val == val3

  test "Query iterator should be disposed when it goes out of scope":
    when defined(gcOrc) or defined(gcArc):
      let q = Query.init(key1)

      (await ds.put(RawRecord.init(key1, val1))).tryGet()
      (await ds.put(RawRecord.init(key2, val2))).tryGet()
      (await ds.put(RawRecord.init(key3, val3))).tryGet()

      proc openIterator() {.async.} =
        let iter = (await ds.query(q)).tryGet
        discard await iter.next()

      await openIterator()

      (await ds.close()).tryGet() # crashes if iter not disposed
    else:
      skip()

  if testLimitsAndOffsets:
    test "Should apply limit":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, limit = 10)

      for i in 0 ..< 100:
        let
          key = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        (await ds.put(RawRecord.init(key, val))).tryGet()

      let
        iter = (await ds.query(q)).tryGet
        res = (await allFinished(toSeq(iter)))
          .mapIt(it.read.tryGet)
          .filterIt(it.isSome)
          .mapIt(it.get)

      check:
        res.len == 10

    test "Should not apply offset":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, offset = 90)

      for i in 0 ..< 100:
        let
          key = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        (await ds.put(RawRecord.init(key, val))).tryGet()

      let
        iter = (await ds.query(q)).tryGet
        res = (await allFinished(toSeq(iter)))
          .mapIt(it.read.tryGet)
          .filterIt(it.isSome)
          .mapIt(it.get)

      check:
        res.len == 10

    test "Should not apply offset and limit":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, offset = 95, limit = 5)

      for i in 0 ..< 100:
        let
          key = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        (await ds.put(RawRecord.init(key, val))).tryGet()

      let
        iter = (await ds.query(q)).tryGet
        res = (await allFinished(toSeq(iter)))
          .mapIt(it.read.tryGet)
          .filterIt(it.isSome)
          .mapIt(it.get)

      check:
        res.len == 5

      for i in 0 ..< res.high:
        let
          val = ("val " & $(i + 95)).toBytes
          key = Key.init(key, Key.init("/" & $(i + 95)).tryGet).tryGet

        check:
          res[i].key == key
          res[i].val == val

  if testSortOrder:
    test "Should apply sort order - descending":
      let
        key = Key.init("/a").tryGet
        q = Query.init(key, sort = SortOrder.Descending)

      var kvs: seq[RawRecord]
      for i in 0 ..< 100:
        let
          k = Key.init(key, Key.init("/" & $i).tryGet).tryGet
          val = ("val " & $i).toBytes

        kvs.add(RawRecord.init(k, val))
        (await ds.put(RawRecord.init(k, val))).tryGet()

      # lexicographic sort, as it comes from the backend
      kvs.sort do(a, b: RawRecord) -> int:
        cmp(a.key.id, b.key.id)

      kvs = kvs.reversed
      let
        iter = (await ds.query(q)).tryGet
        res = (await allFinished(toSeq(iter)))
          .mapIt(it.read.tryGet)
          .filterIt(it.isSome)
          .mapIt(it.get)

      check:
        res.len == 100

      for i, r in res[1 ..^ 1]:
        check:
          res[i].key == kvs[i].key
          res[i].val == kvs[i].val
