{.push raises: [].}

import pkg/chronos
import pkg/questionable
import pkg/questionable/results

import ./types
import ./key

type
  QueryEndedError* = object of KVStoreError

  SortOrder* {.pure.} = enum
    Ascending
    Descending

  Query* = object
    key*: Key # Key to be queried
    value*: bool # Flag to indicate if data should be returned
    limit*: int # Max items to return - not available in all backends
    offset*: int # Offset from which to start querying - not available in all backends
    sort*: SortOrder # Sort order - not available in all backends

  IterFinished* = proc(): bool {.closure, gcsafe, raises: [].}
  IterDispose* =
    proc(): Future[?!void] {.closure, async: (raises: [CancelledError]), gcsafe.}
  GetNext*[T] = proc(): Future[?!(?Record[T])] {.
    closure, async: (raises: [CancelledError]), gcsafe
  .}

  QueryIterObj[T] = object
    nextImpl: GetNext[T]
    finishedImpl: IterFinished
    disposeImpl: IterDispose
    disposed: bool

  QueryIter*[T] = ref QueryIterObj[T]

  GetNextRaw* = GetNext[seq[byte]]
  QueryIterRaw* = QueryIter[seq[byte]]

proc finished*[T](iter: QueryIter[T]): bool =
  iter.finishedImpl()

proc next*[T](
    iter: QueryIter[T]
): Future[?!(?Record[T])] {.async: (raises: [CancelledError]).} =
  await iter.nextImpl()

proc dispose*[T](
    iter: QueryIter[T]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Async dispose - properly waits for in-flight workers to complete.
  ## Should be preferred over sync dispose() when in async context.
  if not iter.disposed and iter.disposeImpl != nil:
    let res = await iter.disposeImpl()
    iter.disposed = true
    return res
  return success()

iterator items*[T](q: QueryIter[T]): Future[?!(?Record[T])] =
  while not q.finished:
    yield q.next()

proc defaultDispose*[T](): Future[?!void] {.async: (raises: [CancelledError]).} =
  return success()

proc new*[T](
    _: type QueryIter[T],
    next: GetNext[T],
    finished: IterFinished,
    dispose: IterDispose = defaultDispose[T],
): QueryIter[T] =
  QueryIter[T](nextImpl: next, finishedImpl: finished, disposeImpl: dispose)

proc init*(
    _: type Query,
    key: Key,
    value = true,
    sort = SortOrder.Ascending,
    offset = 0,
    limit = -1,
): Query =
  Query(key: key, value: value, sort: sort, offset: offset, limit: limit)
