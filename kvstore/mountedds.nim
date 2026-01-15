{.push raises: [].}

import std/tables

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./rawkvstore

type
  MountedStore* = object
    store*: KVStore
    key*: Key

  MountedKVStore* = ref object of KVStore
    stores*: Table[Key, MountedStore]

method mount*(
    self: MountedKVStore, key: Key, store: KVStore
): ?!void {.base, gcsafe.} =
  ## Mount a store on a namespace - namespaces are only `/`
  ##

  if key in self.stores:
    return failure("Key already has store mounted!")

  self.stores[key] = MountedStore(store: store, key: key)

  return success()

func findStore*(self: MountedKVStore, key: Key): ?!MountedStore =
  ## Find a store mounted under a particular key
  ##

  for (k, v) in self.stores.pairs:
    var mounted = key

    while mounted.len > 0:
      if ?k.path == ?mounted.path:
        return success v

      if mounted.parent.isErr:
        break

      mounted = mounted.parent.get

  failure newException(KVStoreKeyNotFound, "No store found for key")

proc dispatch(
    self: MountedKVStore, key: Key
): ?!tuple[store: MountedStore, relative: Key] =
  ## Helper to retrieve the store and corresponding relative key
  ##

  let mounted = ?self.findStore(key)

  return success (store: mounted, relative: ?key.relative(mounted.key))

method has*(
    self: MountedKVStore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  let mounted = ?self.dispatch(key)

  return (await mounted.store.store.has(mounted.relative))

method get*(
    self: MountedKVStore, key: Key
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
  let mounted = ?self.dispatch(key)
  let child = ?(await mounted.store.store.get(mounted.relative))

  let globalKey = mounted.store.key / child.key
  return success RawRecord.init(globalKey, child.val, child.token)

method get*(
    self: MountedKVStore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  type GetBatch = object
    store: MountedStore
    keys: seq[Key]

  var batches = initTable[Key, GetBatch]()

  for key in keys:
    let dispatched = ?self.dispatch(key)
    var batch = batches.mgetOrPut(
      dispatched.store.key, GetBatch(store: dispatched.store, keys: @[])
    )
    batch.keys.add(dispatched.relative)
    batches[dispatched.store.key] = batch

  var collected: seq[RawRecord]
  for batch in batches.values:
    let results = ?(await batch.store.store.get(batch.keys))
    for record in results:
      let globalKey = batch.store.key / record.key
      collected.add(RawRecord.init(globalKey, record.val, record.token))

  return success collected

method put*(
    self: MountedKVStore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  type PutBatch = object
    store: MountedStore
    records: seq[RawRecord]

  var batches = initTable[Key, PutBatch]()

  for record in records:
    let dispatched = ?self.dispatch(record.key)
    var batch = batches.mgetOrPut(
      dispatched.store.key, PutBatch(store: dispatched.store, records: @[])
    )
    batch.records.add(RawRecord.init(dispatched.relative, record.val, record.token))
    batches[dispatched.store.key] = batch

  var conflicts: seq[Key]
  for batch in batches.values:
    let skipped = ?(await batch.store.store.put(batch.records))
    for relKey in skipped:
      conflicts.add(batch.store.key / relKey)

  return success conflicts

method delete*(
    self: MountedKVStore, records: seq[KeyRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  type DeleteBatch = object
    store: MountedStore
    records: seq[KeyRecord]

  var batches = initTable[Key, DeleteBatch]()

  for record in records:
    let dispatched = ?self.dispatch(record.key)
    var batch = batches.mgetOrPut(
      dispatched.store.key, DeleteBatch(store: dispatched.store, records: @[])
    )
    batch.records.add(KeyRecord.init(dispatched.relative, record.token))
    batches[dispatched.store.key] = batch

  var skipped: seq[Key]
  for batch in batches.values:
    let relSkipped = ?(await batch.store.store.delete(batch.records))
    for relKey in relSkipped:
      skipped.add(batch.store.key / relKey)

  return success skipped

method close*(
    self: MountedKVStore
): Future[?!void] {.async: (raises: [CancelledError]).} =
  for s in self.stores.values:
    if err =? (await s.store.close()).errorOption:
      return failure err
  return success()

func new*(
    T: type MountedKVStore,
    stores: Table[Key, KVStore] = initTable[Key, KVStore](),
): ?!T =
  var self = T()
  for (k, v) in stores.pairs:
    self.stores[?k.path] = MountedStore(store: v, key: k)

  success self
