{.push raises: [].}

## Lock-free, non-GC prepared statement cache.
##
## Designed for per-SQLiteDsDb-instance caching of dynamically-prepared
## statements keyed by record count (small int). Uses allocShared/deallocShared
## so the GC never sees this memory -- safe to embed in ptr-shared objects.
##
## Linear scan is intentional: typical cardinality is <10 distinct batch sizes,
## so a flat array beats hashing and is fully cache-line friendly.

import ./sqliteutils

const DefaultStmtCacheCap* = 8

type
  StmtCacheEntry* = object
    count*: int
    stmt*: RawStmtPtr

  StmtCache* = object
    entries: ptr UncheckedArray[StmtCacheEntry]
    len: int
    cap: int

proc init*(cache: var StmtCache, initialCap: int = DefaultStmtCacheCap) =
  ## Initialize cache. Must be called before use.
  cache.len = 0
  cache.cap = initialCap
  cache.entries = cast[ptr UncheckedArray[StmtCacheEntry]](allocShared0(
    initialCap * sizeof(StmtCacheEntry)
  ))

proc get*(cache: StmtCache, count: int): RawStmtPtr =
  ## Look up a cached statement by record count.
  ## Returns nil if not found.
  for i in 0 ..< cache.len:
    if cache.entries[i].count == count:
      return cache.entries[i].stmt
  return nil

proc put*(cache: var StmtCache, count: int, stmt: RawStmtPtr) =
  ## Cache a prepared statement for the given record count.
  ## Grows the backing array if needed.
  if cache.len == cache.cap:
    let newCap = cache.cap * 2
    cache.entries = cast[ptr UncheckedArray[StmtCacheEntry]](reallocShared0(
      cache.entries, cache.cap * sizeof(StmtCacheEntry), newCap * sizeof(StmtCacheEntry)
    ))
    cache.cap = newCap
  cache.entries[cache.len] = StmtCacheEntry(count: count, stmt: stmt)
  cache.len += 1

proc deinit*(cache: var StmtCache) =
  ## Finalize all cached prepared statements, then free shared memory.
  if not cache.entries.isNil:
    for i in 0 ..< cache.len:
      cache.entries[i].stmt.dispose
    deallocShared(cache.entries)
    cache.entries = nil
  cache.len = 0
  cache.cap = 0
