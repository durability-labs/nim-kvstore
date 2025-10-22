{.push raises: [].}

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./key
import ./datastore

type TieredDatastore* = ref object of Datastore
  stores: seq[Datastore]

proc new*(T: type TieredDatastore, stores: varargs[Datastore]): ?!T =
  if stores.len == 0:
    failure "stores must contain at least one Datastore"
  else:
    success T(stores: @stores)

proc stores*(self: TieredDatastore): seq[Datastore] =
  self.stores

method has*(
    self: TieredDatastore, key: Key
): Future[?!bool] {.async: (raises: [CancelledError]).} =
  for store in self.stores:
    without res =? (await store.has(key)), err:
      return failure(err)

    if res:
      return success true

  return success false

method get*(
    self: TieredDatastore, key: Key
): Future[?!RawRecord] {.async: (raises: [CancelledError]).} =
  for idx, store in self.stores.pairs():
    let fetched = await store.get(key)
    if fetched.isOk:
      let record = fetched.get()

      # Backfill to upper tiers (best effort)
      if idx > 0:
        for backIdx in 0 ..< idx:
          # Use put directly - ignore conflicts during backfill
          discard await self.stores[backIdx].put(@[record])

      return success record

    let err = fetched.error
    if err of DatastoreKeyNotFound:
      continue

    return failure err

  return failure newException(DatastoreKeyNotFound, "Key doesn't exist")

method get*(
    self: TieredDatastore, keys: seq[Key]
): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
  ## Get multiple records, backfilling to upper tiers as we go

  var results: seq[RawRecord]
  var remaining = keys

  for idx, store in self.stores.pairs():
    if remaining.len == 0:
      break

    let fetched = await store.get(remaining)
    if fetched.isErr:
      return failure fetched.error

    let records = fetched.get()

    # Backfill to upper tiers if this isn't the first tier (best effort)
    if idx > 0 and records.len > 0:
      for backIdx in 0 ..< idx:
        discard await self.stores[backIdx].put(records)

    results.add(records)

    # Remove found keys from remaining
    var foundKeys: seq[Key]
    for record in records:
      foundKeys.add(record.key)

    var stillRemaining: seq[Key]
    for key in remaining:
      if key notin foundKeys:
        stillRemaining.add(key)
    remaining = stillRemaining

  return success results

method put*(
    self: TieredDatastore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## Put records to all tiers
  ## Returns keys that were skipped due to conflicts in ANY tier

  if records.len == 0:
    return success(newSeq[Key]())

  var allSkipped: seq[Key]

  for store in self.stores:
    let skipped = ?(await store.put(records))
    # Collect any keys that failed in this tier
    for key in skipped:
      if key notin allSkipped:
        allSkipped.add(key)

  return success allSkipped

method delete*(
    self: TieredDatastore, records: seq[RawRecord]
): Future[?!seq[Key]] {.async: (raises: [CancelledError]).} =
  ## Delete records from all tiers
  ## Returns keys that were skipped due to conflicts in ANY tier

  if records.len == 0:
    return success(newSeq[Key]())

  var allSkipped: seq[Key]

  for store in self.stores:
    let skipped = ?(await store.delete(records))
    # Collect any keys that failed in this tier
    for key in skipped:
      if key notin allSkipped:
        allSkipped.add(key)

  return success allSkipped

method close*(
    self: TieredDatastore
): Future[?!void] {.async: (raises: [CancelledError]).} =
  return success()
