{.push raises: [].}

import std/algorithm
import std/hashes
import std/oids
import std/sequtils
import std/strutils
import std/strformat

import pkg/questionable
import pkg/questionable/results

import ./namespace

export hashes, namespace

type Key* = object
  namespaces*: seq[Namespace]
  cachedId*: string

func computeId(namespaces: seq[Namespace]): string =
  Separator & namespaces.mapIt(it.id).join(Separator)

func init*(T: type Key, namespaces: varargs[Namespace]): ?!T =
  let ns = @namespaces
  success T(namespaces: ns, cachedId: computeId(ns))

func init*(T: type Key, namespaces: varargs[string]): ?!T =
  var self = T()
  for s in namespaces:
    self.namespaces &= s.split(Separator).filterIt(it.len > 0).mapIt(
      ?Namespace.init(it)
    )

  self.cachedId = computeId(self.namespaces)
  return success self

func init*(T: type Key, keys: varargs[Key]): ?!T =
  let ns = keys.mapIt(it.namespaces).concat
  success T(namespaces: ns, cachedId: computeId(ns))

func list*(self: Key): seq[Namespace] =
  self.namespaces

proc random*(T: type Key): string =
  $genOid()

template `[]`*(key: Key, x: auto): auto =
  key.namespaces[x]

func len*(self: Key): int =
  self.namespaces.len

iterator items*(key: Key): Namespace =
  for k in key.namespaces:
    yield k

func reverse*(self: Key): Key =
  let ns = self.namespaces.reversed
  Key(namespaces: ns, cachedId: computeId(ns))

func value*(self: Key): string =
  if self.len > 0:
    return self[^1].value

func field*(self: Key): string =
  if self.len > 0:
    return self[^1].field

func id*(self: Key): string {.inline.} =
  self.cachedId

func root*(self: Key): bool =
  self.len == 1

func parent*(self: Key): ?!Key =
  if self.root:
    failure "key has no parent"
  else:
    let ns = self.namespaces[0 ..^ 2]
    success Key(namespaces: ns, cachedId: computeId(ns))

func path*(self: Key): ?!Key =
  let tail =
    if self[^1].field.len > 0:
      self[^1].field
    else:
      self[^1].value

  if self.root:
    return Key.init(tail)

  let ns = (?self.parent).namespaces & @[Namespace(value: tail)]
  return success Key(namespaces: ns, cachedId: computeId(ns))

func child*(self: Key, namespaces: varargs[Namespace]): Key =
  let ns = self.namespaces & @namespaces
  Key(namespaces: ns, cachedId: computeId(ns))

func `/`*(self: Key, ns: Namespace): Key =
  self.child(ns)

func child*(self: Key, keys: varargs[Key]): Key =
  let ns = self.namespaces & concat(keys.mapIt(it.namespaces))
  Key(namespaces: ns, cachedId: computeId(ns))

func `/`*(self, key: Key): Key =
  self.child(key)

func child*(self: Key, ids: varargs[string]): ?!Key =
  success self.child(ids.filterIt(it != "").mapIt(?Key.init(it)))

func `/`*(self: Key, id: string): ?!Key =
  self.child(id)

func relative*(self: Key, parent: Key): ?!Key =
  ## Get a key relative to parent from current key
  ##

  if self.len < parent.len:
    return failure "Not a parent of this key!"

  if self.namespaces[0 ..< parent.len] != parent.namespaces:
    return failure "Not a parent of this key!"

  if parent.len == self.len:
    let ns: seq[Namespace] = @[]
    return success Key(namespaces: ns, cachedId: computeId(ns))

  let ns = self.namespaces[parent.len ..< self.len]
  return success Key(namespaces: ns, cachedId: computeId(ns))

func ancestor*(self, other: Key): bool =
  if other.len <= self.len:
    false
  else:
    other.namespaces[0 ..< self.len] == self.namespaces

func descendant*(self, other: Key): bool =
  other.ancestor(self)

func hash*(key: Key): Hash {.inline.} =
  hash(key.id)

func `$`*(key: Key): string =
  key.id
