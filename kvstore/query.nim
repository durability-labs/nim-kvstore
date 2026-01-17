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

  IterFinished* = proc(): bool {.gcsafe, closure, raises: [].}
  IterDispose* = proc() {.gcsafe, closure, raises: [].}
  IterDisposeAsync* = proc(): Future[?!void] {.
    async: (raises: [CancelledError]), gcsafe, closure
  .}
  GetNext*[T] = proc(): Future[?!(?Record[T])] {.
    async: (raises: [CancelledError]), gcsafe, closure
  .}

  QueryIterObj[T] = object
    nextImpl: GetNext[T]
    finishedImpl: IterFinished
    disposeImpl: IterDispose
    disposeAsyncImpl: IterDisposeAsync
    disposed: bool

  QueryIter*[T] = ref QueryIterObj[T]

  GetNextRaw* = GetNext[seq[byte]]
  QueryIterRaw* = QueryIter[seq[byte]]

proc `=destroy`[T](iter: var QueryIterObj[T]) =
  ## Destructor ensures dispose is called when iterator goes out of scope
  if not iter.disposed and iter.disposeImpl != nil:
    iter.disposeImpl()
    iter.disposed = true

proc finished*[T](iter: QueryIter[T]): bool =
  iter.finishedImpl()

proc next*[T](
    iter: QueryIter[T]
): Future[?!(?Record[T])] {.async: (raises: [CancelledError]).} =
  await iter.nextImpl()

proc dispose*[T](iter: QueryIter[T]) =
  ## Sync dispose - for backwards compatibility and destructor use.
  ## Prefer disposeAsync() when possible.
  if not iter.disposed:
    iter.disposeImpl()
    iter.disposed = true

proc disposeAsync*[T](
    iter: QueryIter[T]
): Future[?!void] {.async: (raises: [CancelledError]).} =
  ## Async dispose - properly waits for in-flight workers to complete.
  ## Should be preferred over sync dispose() when in async context.
  if not iter.disposed:
    if iter.disposeAsyncImpl != nil:
      let res = await iter.disposeAsyncImpl()
      iter.disposed = true
      return res
    else:
      # Fallback to sync dispose if no async impl
      iter.disposeImpl()
      iter.disposed = true
  return success()

iterator items*[T](q: QueryIter[T]): Future[?!(?Record[T])] =
  while not q.finished:
    yield q.next()

proc defaultDispose() =
  discard

proc defaultDisposeAsync(): Future[?!void] {.async: (raises: [CancelledError]).} =
  return success()

proc new*[T](
    _: type QueryIter[T],
    next: GetNext[T],
    finished: IterFinished,
    dispose: IterDispose = defaultDispose,
    disposeAsync: IterDisposeAsync = nil,
): QueryIter[T] =
  QueryIter[T](
    nextImpl: next, 
    finishedImpl: finished, 
    disposeImpl: dispose,
    disposeAsyncImpl: disposeAsync
  )

proc init*(
    _: type Query,
    key: Key,
    value = true,
    sort = SortOrder.Ascending,
    offset = 0,
    limit = -1,
): Query =
  Query(key: key, value: value, sort: sort, offset: offset, limit: limit)
