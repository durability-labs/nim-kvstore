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

### Keys and Namespaces

Keys are hierarchical paths used to identify records. A `Key` is composed of one or more `Namespace` segments.

#### Namespace

A `Namespace` is a single segment of a key, consisting of an optional `field` and a `value`:

```nim
type Namespace* = object
  field*: string   # Optional field/type identifier
  value*: string   # The namespace value
```

**Constants:**
- `Delimiter = ":"` - Separates field from value within a namespace
- `Separator = "/"` - Separates namespaces within a key

**Constructors:**

| Signature | Description |
|-----------|-------------|
| `Namespace.init(field, value: string): ?!Namespace` | Create from separate field and value |
| `Namespace.init(id: string): ?!Namespace` | Parse from string like `"field:value"` or `"value"` |

**Validation Rules:**
- Neither field nor value may contain `":"` or `"/"`
- An id string may contain at most one `":"`
- Whitespace is stripped from field and value

**Functions:**

| Function | Description |
|----------|-------------|
| `id(ns): string` | Returns `"field:value"` if field exists, else `"value"` |
| `hash(ns): Hash` | Hash based on `id` |
| `$(ns): string` | Same as `id` |

#### Key

A `Key` is a hierarchical path composed of `Namespace` segments:

```nim
type Key* = object
  namespaces*: seq[Namespace]
```

**Constructors:**

| Signature | Description |
|-----------|-------------|
| `Key.init(namespaces: varargs[Namespace]): ?!Key` | Create from Namespace objects |
| `Key.init(namespaces: varargs[string]): ?!Key` | Parse from path strings like `"/a:b/c/d"` |
| `Key.init(keys: varargs[Key]): ?!Key` | Concatenate multiple keys |

**Parsing Behavior:**
- Strings are split by `"/"` separator
- Empty segments (e.g., `"///a///b///"`) are filtered out
- Each segment is parsed as a `Namespace`

**Accessors:**

| Function | Description |
|----------|-------------|
| `list(key): seq[Namespace]` | Returns all namespaces |
| `key[x]` | Index into namespaces (supports slices) |
| `len(key): int` | Number of namespaces |
| `value(key): string` | Value of the last namespace |
| `field(key): string` | Field of the last namespace |
| `id(key): string` | Full path string, e.g., `"/a:b/c/d:e"` |

**Navigation:**

| Function | Description |
|----------|-------------|
| `root(key): bool` | True if key has only one namespace |
| `parent(key): ?!Key` | Key without last namespace (fails if root) |
| `path(key): ?!Key` | Parent with last namespace's field/value stripped |
| `reverse(key): Key` | Key with namespaces in reverse order |

**Building Keys:**

| Function | Description |
|----------|-------------|
| `child(key, namespaces: varargs[Namespace]): Key` | Append namespaces |
| `child(key, keys: varargs[Key]): Key` | Append keys |
| `child(key, ids: varargs[string]): ?!Key` | Append parsed strings |
| `key / ns` | Operator alias for child (Namespace) |
| `key / other` | Operator alias for child (Key) |
| `key / id` | Operator alias for child (string) |
| `Key.random(): string` | Generate random 24-char OID string |

**Relationships:**

| Function | Description |
|----------|-------------|
| `relative(key, parent): ?!Key` | Get key relative to parent |
| `ancestor(key, other): bool` | True if `other` is a descendant of `key` |
| `descendant(key, other): bool` | True if `key` is a descendant of `other` |

**Example Usage:**

```nim
# Create namespaces
let ns = Namespace.init("type", "user").tryGet()  # field="type", value="user"
let ns2 = Namespace.init("user").tryGet()          # field="", value="user"
let ns3 = Namespace.init("type:user").tryGet()     # field="type", value="user"

# Create keys
let key = Key.init("/users/alice/profile").tryGet()
let key2 = Key.init("users", "alice", "profile").tryGet()  # equivalent

# Navigate keys
let parent = key.parent.tryGet()        # /users/alice
let isRoot = parent.root                # false
let lastValue = key.value               # "profile"

# Build keys
let child = (key / "settings").tryGet()  # /users/alice/profile/settings
let combined = key / Key.init("a/b").tryGet()

# Check relationships
let isAncestor = Key.init("/users").tryGet().ancestor(key)  # true
let relative = key.relative(Key.init("/users").tryGet()).tryGet()  # alice/profile
```

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

defer:
  iter.dispose()

while not iter.finished:
  let recordOpt = (await iter.next()).tryGet()
  if record =? recordOpt:
    echo record.key, ": ", record.val

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
