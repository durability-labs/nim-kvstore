# nim-kvstore

Simple, unified API for multiple key-value stores with optimistic concurrency control.

Inspired by the Python library [datastore](https://github.com/datastore/datastore).

## Features

- **Unified API** - Same interface across different storage backends
- **Optimistic Concurrency Control** - Token-based CAS (Compare-And-Swap) semantics prevent lost updates
- **Typed Records** - Automatic serialization/deserialization with custom encoder/decoder procs
- **Async/Await** - Built on Chronos for async operations
- **Multiple Backends** - SQLite (in-memory or file), filesystem, and mounted kvstores

## Installation

```bash
nimble install kvstore
```

## Quick Start

```nim
import pkg/chronos
import pkg/kvstore
import pkg/stew/byteutils

proc main() {.async.} =
  # Create an in-memory SQLite kvstore
  let ds = SQLiteKVStore.new(SqliteMemory).tryGet()

  # Create a key
  let key = Key.init("/users/alice").tryGet()

  # Store data
  (await ds.put(key, "Hello, World!".toBytes())).tryGet()

  # Retrieve data
  let record = (await ds.get(key)).tryGet()
  echo "Value: ", string.fromBytes(record.val)
  echo "Token: ", record.token  # Version token for CAS

  # Update with CAS - use the token from the previous get
  let updated = RawRecord.init(key, "Updated!".toBytes(), record.token)
  (await ds.put(updated)).tryGet()

  # Close the store
  (await ds.close()).tryGet()

waitFor main()
```

## Core Concepts

### Records and Tokens

Every record in nim-kvstore has three components:

```nim
type Record*[T] = object
  key*: Key        # Unique identifier
  val*: T          # The stored value
  token*: uint64   # Version token for optimistic concurrency
```

The **token** is central to the CAS semantics:

- Token `0` means "insert only if key doesn't exist"
- Any other token means "update only if current token matches"

**Important:** Tokens are opaque values and should not be manipulated directly. Always use the token returned from `get()` operations when performing updates or deletes. Different backends may implement token generation differently (incrementing integers, timestamps, UUIDs, etc.), so never assume a specific token format or attempt arithmetic on tokens.

### Optimistic Concurrency Control

nim-kvstore uses optimistic concurrency to prevent lost updates in concurrent environments:

```nim
# Two clients read the same record
let record1 = (await ds.get(key)).tryGet()  # token = 5
let record2 = (await ds.get(key)).tryGet()  # token = 5

# Client 1 updates successfully
let update1 = RawRecord.init(key, newValue1, record1.token)
(await ds.put(update1)).tryGet()  # Success! Token is now 6

# Client 2's update fails - stale token
let update2 = RawRecord.init(key, newValue2, record2.token)
let result = await ds.put(update2)
# result contains the key in skipped list - conflict detected!
```

### Bulk Operations

Bulk operations return a list of keys that were skipped due to conflicts:

```nim
let records = @[
  RawRecord.init(key1, value1, token1),
  RawRecord.init(key2, value2, token2),
  RawRecord.init(key3, value3, token3),
]

let skipped = (await ds.put(records)).tryGet()
# skipped contains keys where token didn't match
```

## API Reference

### Core Operations

| Method | Description |
| ------ | ----------- |
| `has(key)` | Check if key exists |
| `get(key)` | Get single record |
| `get(keys)` | Get multiple records |
| `put(record)` | Insert/update single record (errors on conflict) |
| `put(records)` | Insert/update multiple records (returns skipped keys) |
| `delete(record)` | Delete single record (errors on conflict) |
| `delete(records)` | Delete multiple records (returns skipped keys) |
| `query(query)` | Query records by key prefix |
| `close()` | Close the store |

### Helper Operations

| Method | Description |
| ------ | ----------- |
| `tryPut(records, maxRetries, middleware)` | Bulk put with retry on conflicts |
| `tryDelete(records, maxRetries, middleware)` | Bulk delete with retry on conflicts |
| `getOrPut(key, producer, maxRetries)` | Get existing or lazily create |
| `contains(key)` | Alias for `has` returning bool |

### Middleware for Conflict Resolution

The `tryPut` and `tryDelete` helpers accept a middleware function to resolve conflicts:

```nim
# Middleware receives failed records and returns updated records to retry
let middleware = proc(failed: seq[RawRecord]): Future[?!seq[RawRecord]] {.async.} =
  # Refetch current tokens
  let fresh = (await ds.get(failed.mapIt(it.key))).tryGet()

  # Update records with fresh tokens
  var updated: seq[RawRecord]
  for i, record in failed:
    for f in fresh:
      if f.key == record.key:
        updated.add(RawRecord.init(record.key, record.val, f.token))
        break

  success(updated)

let result = await ds.tryPut(records, maxRetries = 3, middleware = middleware)
```

## Typed Records

nim-kvstore supports automatic type conversion with custom encoder/decoder procs:

```nim
import pkg/stew/byteutils
import pkg/questionable/results

type Person = object
  name: string
  age: int

# Define encoder
proc encode(p: Person): seq[byte] =
  (p.name & ":" & $p.age).toBytes()

# Define decoder
proc decode(T: type Person, bytes: seq[byte]): ?!T =
  let parts = string.fromBytes(bytes).split(':')
  success(Person(name: parts[0], age: parseInt(parts[1])))

# Use typed API
let key = Key.init("/people/alice").tryGet()
let person = Person(name: "Alice", age: 30)

# Store typed record
(await ds.put(key, person)).tryGet()

# Retrieve typed record
let record = (await ds.get[Person](key)).tryGet()
echo record.val.name  # "Alice"
echo record.val.age   # 30
```

## Storage Backends

### SQLiteKVStore

SQLite-backed storage supporting both in-memory and file-based databases.

```nim
# In-memory database
let memDs = SQLiteKVStore.new(SqliteMemory).tryGet()

# File-based database
let fileDs = SQLiteKVStore.new("/path/to/db.sqlite").tryGet()

# Read-only mode
let readOnlyDs = SQLiteKVStore.new("/path/to/db.sqlite", readOnly = true).tryGet()
```

**Note:** SQLite uses `int64` for tokens, limiting the range to `0..high(int64)`.

### FSKVStore

Filesystem-backed storage where each record is a file.

```nim
let fsDs = FSKVStore.new(
  root = "/path/to/data",
  depth = 5  # Maximum key depth
).tryGet()
```

**Note:** FSKVStore uses `uint64` for tokens, supporting the full range.

### MountedKVStore

Combines multiple kvstores under different key prefixes:

```nim
let sqlDs = SQLiteKVStore.new(SqliteMemory).tryGet()
let fsDs = FSKVStore.new("/data").tryGet()

let mounted = MountedKVStore.new({
  Key.init("/cache").tryGet(): KVStore(sqlDs),
  Key.init("/files").tryGet(): KVStore(fsDs),
}.toTable).tryGet()

# Keys are routed to appropriate backend
await mounted.put(Key.init("/cache/item").tryGet(), data)  # Goes to SQLite
await mounted.put(Key.init("/files/doc").tryGet(), data)   # Goes to filesystem
```

## Error Types

```nim
KVStoreError              # Base error type
├── KVStoreMaxRetriesError  # tryPut/tryDelete exhausted retries
└── KVStoreBackendError     # Backend-specific errors
    ├── KVStoreKeyNotFound  # Key doesn't exist
    └── KVStoreCorruption   # Data corruption detected
```

## Query API

Query records by key prefix:

```nim
let query = Query.init(
  key = Key.init("/users").tryGet(),
  value = true,              # Include values in results
  sort = SortOrder.Ascending,
  offset = 0,
  limit = 100
)

let iter = (await ds.query(query)).tryGet()

while not iter.finished:
  let recordOpt = (await iter.next()).tryGet()
  if record =? recordOpt:
    echo record.key, ": ", record.val

iter.dispose()
```

## Stability

nim-kvstore is currently marked as experimental and may be subject to breaking changes across any version bump until it is marked as stable.

## Future Work

- **Token Provider API** - A pluggable interface for token generation, allowing backends to implement custom token strategies (incrementing integers, timestamps, vector clocks, etc.) while maintaining the opaque token semantics.

## License

nim-kvstore is licensed and distributed under either of:

- Apache License, Version 2.0: [LICENSE-APACHEv2](LICENSE-APACHEv2) or <https://opensource.org/licenses/Apache-2.0>
- MIT license: [LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>

at your option. The contents of this repository may not be copied, modified, or distributed except according to those terms.
