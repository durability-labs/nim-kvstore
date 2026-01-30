## nim-kvstore - Unified API for multiple key-value stores
##
## Basic usage with raw bytes:
## .. code-block:: Nim
##   import pkg/kvstore
##   import pkg/stew/byteutils
##
##   let ds = SQLiteKVStore.new(SqliteMemory).tryGet()
##   let key = Key.init("/users/alice").tryGet()
##
##   # Store data
##   (await ds.put(key, "Hello".toBytes())).tryGet()
##
##   # Retrieve data
##   let record = (await ds.get(key)).tryGet()
##   echo string.fromBytes(record.val)
##
## For typed records with automatic encoding/decoding, also import:
## .. code-block:: Nim

import pkg/chronos
import pkg/questionable/results

import ./kvstore/key
import ./kvstore/helpers
import ./kvstore/fsds
import ./kvstore/sql
import ./kvstore/types
import ./kvstore/query

export helpers, fsds, sql, types, query, key
