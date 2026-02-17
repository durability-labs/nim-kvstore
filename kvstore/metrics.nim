{.push raises: [].}

import pkg/metrics
export metrics

# =============================================================================
# API Layer Metrics (retry middleware in api.nim)
# These are backend-agnostic — they track the retry logic.
# =============================================================================

# tryPut (bulk, partial commit)
declarePublicCounter(kvstore_tryput_total, "kvstore tryPut calls")
declarePublicCounter(kvstore_tryput_retries_total, "kvstore tryPut retry iterations")
declarePublicCounter(
  kvstore_tryput_exhausted_total, "kvstore tryPut max retries exhausted"
)

# tryPutAtomic (all-or-nothing)
declarePublicCounter(kvstore_tryputatomic_total, "kvstore tryPutAtomic calls")
declarePublicCounter(
  kvstore_tryputatomic_retries_total, "kvstore tryPutAtomic retry iterations"
)
declarePublicCounter(
  kvstore_tryputatomic_exhausted_total, "kvstore tryPutAtomic max retries exhausted"
)

# tryDelete (bulk, partial commit)
declarePublicCounter(kvstore_trydelete_total, "kvstore tryDelete calls")
declarePublicCounter(
  kvstore_trydelete_retries_total, "kvstore tryDelete retry iterations"
)
declarePublicCounter(
  kvstore_trydelete_exhausted_total, "kvstore tryDelete max retries exhausted"
)

# tryDeleteAtomic (all-or-nothing)
declarePublicCounter(kvstore_trydeleteatomic_total, "kvstore tryDeleteAtomic calls")
declarePublicCounter(
  kvstore_trydeleteatomic_retries_total, "kvstore tryDeleteAtomic retry iterations"
)
declarePublicCounter(
  kvstore_trydeleteatomic_exhausted_total,
  "kvstore tryDeleteAtomic max retries exhausted",
)
