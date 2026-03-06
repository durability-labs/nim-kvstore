{.push raises: [].}

import std/tables

import pkg/chronos

import ../key

type
  RefCountedLock* = ref object
    ## AsyncLock with reference counting to safely track waiters.
    ## Only removed from table when refcount reaches zero.
    lock*: AsyncLock
    refCount*: int

  LockTable* = TableRef[Key, RefCountedLock]

proc newLockTable*(): LockTable =
  newTable[Key, RefCountedLock]()

proc acquire*(
    locks: LockTable, key: Key
): Future[RefCountedLock] {.async: (raises: [CancelledError]).} =
  ## Acquire a per-key lock. Refcount is incremented BEFORE await to track waiters.
  ## This prevents the race where a lock is deleted while tasks are waiting on it.
  var rcLock = locks.mgetOrPut(key, RefCountedLock(lock: newAsyncLock(), refCount: 0))
  rcLock.refCount += 1
  try:
    await rcLock.lock.acquire()
  except CancelledError as exc:
    rcLock.refCount -= 1
    if rcLock.refCount == 0:
      locks.del(key)
    raise exc
  rcLock

proc release*(locks: LockTable, key: Key, rcLock: RefCountedLock) {.raises: [].} =
  ## Release a per-key lock. Only removes from table when refcount reaches zero.
  if rcLock.lock.locked:
    try:
      rcLock.lock.release()
    except CatchableError as err:
      raiseAssert(err.msg)

  rcLock.refCount -= 1
  if rcLock.refCount == 0:
    locks.del(key)
