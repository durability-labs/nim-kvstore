# nim-kvstore Guidelines for AI Agents

Guidelines for AI agents working on the nim-kvstore codebase.

## Build & Test Commands

```bash
# Run all tests
nimble test

# Run specific test file
nim c -r tests/kvstore/testfsds.nim
nim c -r tests/kvstore/testsql.nim
nim c -r tests/kvstore/testkey.nim

# Quick compile check (no run)
nim c kvstore/sql/sqliteds.nim

# Generate coverage report
nimble coverage
```

### Test File Locations

```
tests/
├── testall.nim                    # Main test runner
└── kvstore/
    ├── testkey.nim                # Key/Namespace tests
    ├── testkvstore.nim            # Base KVStore tests
    ├── testfsds.nim               # FSKVStore tests
    ├── testsql.nim                # SQLiteKVStore tests
    ├── kvcommontests.nim          # Shared test helpers
    ├── querycommontests.nim       # Query test helpers
    └── sql/
        ├── testsqliteds.nim
        └── testsqlitedsdb.nim
```

## Code Style

### Module Structure

```nim
{.push raises: [].}  # Module-level exception safety - REQUIRED

import std/sequtils  # Standard library

import pkg/chronos   # External packages use pkg/ prefix
import pkg/questionable
import pkg/questionable/results

import ./types       # Local imports use relative paths
import ../key
```

### Naming Conventions

- **ref types**: Use `new` constructor - `proc new*(T: type MyRef): T`
- **object types**: Use `init` constructor - `func init*(T: type MyObj): T`
- **Fallible constructors**: Return `?!T` - `proc new*(T: type MyRef): ?!T`

### Types Pattern

```nim
type
  KVStore* = ref object of RootObj

  Record*[T] = object
    token*: uint64
    key*: Key
    when T is not type void:
      val*: T

  RawRecord* = Record[seq[byte]]
  KeyRecord* = Record[void]
```

## Error Handling

**Never silently discard errors.** Use `?!T` (Result type) over exceptions.

### Async Methods

Always annotate with explicit raises:

```nim
# GOOD
method get*(self: KVStore, key: Key): Future[?!RawRecord] 
    {.base, gcsafe, async: (raises: [CancelledError]).} =
  raiseAssert("Not implemented!")

# BAD - missing raises annotation
method get*(self: KVStore, key: Key): Future[?!RawRecord] {.async.} =
  discard
```

### Result Unwrapping

```nim
# BEST: ? operator propagates errors
proc process(): Future[?!Result] {.async: (raises: [CancelledError]).} =
  let data = ?await fetchData()
  let parsed = ?parseData(data)
  success(parsed)

# GOOD: without pattern for custom error handling
without value =? someResult, err:
  return failure(newException(CustomError, "context: " & err.msg))

# Discard success value, propagate error
discard ?await innerOp()
```

### Return Patterns

```nim
# For ?!void
return success()
return failure(err)

# For ?!T
return success(value)
return failure(newException(Error, "msg"))
```

## CAS (Compare-And-Swap) Semantics

This is a key-value store with optimistic concurrency control via tokens:

| Token | Meaning | Behavior |
|-------|---------|----------|
| `0` | Insert only | Fails if key exists |
| `N > 0` | Update only | Fails if current token != N |

```nim
# Insert new record (token = 0)
let record = RawRecord.init(key, value, token = 0)
let skipped = ?await store.put(@[record])
# skipped contains keys that failed due to conflicts

# Update existing (use token from get)
let existing = ?await store.get(key)
let updated = RawRecord.init(key, newValue, existing.token)
?await store.put(updated)
```

## Nim Idioms

### Functional Style Preferred

```nim
# GOOD
let keys = records.mapIt(it.key)
let filtered = items.filterIt(it.token > 0)

# AVOID when functional is cleaner
var keys: seq[Key]
for r in records:
  keys.add(r.key)
```

### Avoid Explicit `result`

```nim
# GOOD
proc getValue(): int =
  42

# BAD
proc getValue(): int =
  result = 42
```

## Key Patterns

### Key Structure

Keys are hierarchical paths with Namespace segments:

```nim
let key = Key.init("/users/alice/profile").tryGet()
let parent = key.parent.tryGet()        # /users/alice
let child = (key / "settings").tryGet() # /users/alice/profile/settings
```

### Backend Implementations

| Backend | File | Description |
|---------|------|-------------|
| SQLiteKVStore | `kvstore/sql/sqliteds.nim` | SQLite-backed storage |
| FSKVStore | `kvstore/fsds.nim` | Filesystem-backed (one file per key) |

## Testing Guidelines

### Test Before Moving On

1. **Compile first**: Fix compile errors immediately
2. **Run relevant tests**: Focus on the module you're changing
3. **All tests must pass** before proceeding

### Test Incrementally

```bash
# Working on SQL backend
nim c -r tests/kvstore/testsql.nim

# Working on FS backend  
nim c -r tests/kvstore/testfsds.nim

# Before committing
nimble test
```

### Test Structure

```nim
import pkg/asynctest
import pkg/questionable

suite "MyModule":
  test "should do something":
    let store = SQLiteKVStore.new(SqliteMemory).tryGet()
    let key = Key.init("/test").tryGet()
    
    discard await store.put(RawRecord.init(key, "value".toBytes, 0))
    let record = (await store.get(key)).tryGet()
    
    check record.val == "value".toBytes
```

## Things to Avoid

1. **Don't suppress errors** with plain `discard` - use `discard ?` to propagate
2. **Don't skip `raises` annotations** on async procs
3. **Don't use `result =`** - return directly
4. **Don't move on with failing tests** - fix them first
5. **Don't use `as any` or `@ts-ignore` equivalents** (`{.cast(gcsafe).}` etc.)

## Reference Files

Good examples of codebase patterns:
- `kvstore/rawkvstore.nim` - Base interface, error handling
- `kvstore/types.nim` - Type definitions
- `kvstore/sql/sqliteds.nim` - Full backend implementation
- `kvstore/key/key.nim` - Key manipulation

## Commit Workflow

**Before committing any changes:**

1. **Run all tests**: `nimble test --threads:on`
2. **Invoke Oracle for review**: Always consult the Oracle agent to review changes before committing
3. **Address Oracle feedback**: Fix any issues identified
4. **Then commit**: Only after Oracle approval and tests pass

```
# Example Oracle review prompt:
"Review the changes I made to sqliteds.nim for the threading implementation. 
Check for: correctness, error handling, memory safety, and adherence to codebase patterns."
```

## Dependencies

Key dependencies (from `kvstore.nimble`):
- `chronos` - Async runtime
- `questionable` - Option/Result types with `?` operator
- `sqlite3_abi` - SQLite bindings
- `stew` - Utilities (endians, byte conversions)
- `taskpools` - Thread pool for true async operations
- `asynctest` / `unittest2` - Testing frameworks
