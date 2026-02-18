{.push raises: [].}

import pkg/metrics
export metrics

# =============================================================================
# SQLite Backend Metrics
# =============================================================================

const
  # Custom buckets tuned for I/O latencies: 100µs → 5s
  ioBuckets =
    [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, Inf]

  # Batch size buckets: 1 → 100k records
  batchBuckets =
    [1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0, 1000.0, 10000.0, 100000.0, Inf]

  # Value size buckets (bytes): 16B → 64KB (metadata records are small)
  sizeBuckets = [
    16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 16384.0, 65536.0, Inf
  ]

# --- Operation counters ---

declarePublicCounter(kvstore_sql_has_total, "kvstore sqlite has operations")
declarePublicCounter(kvstore_sql_get_total, "kvstore sqlite get operations")
declarePublicCounter(kvstore_sql_put_total, "kvstore sqlite put operations")
declarePublicCounter(kvstore_sql_delete_total, "kvstore sqlite delete operations")
declarePublicCounter(kvstore_sql_putatomic_total, "kvstore sqlite putAtomic operations")
declarePublicCounter(
  kvstore_sql_deleteatomic_total, "kvstore sqlite deleteAtomic operations"
)
declarePublicCounter(
  kvstore_sql_moveatomic_total, "kvstore sqlite moveKeysAtomic operations"
)
declarePublicCounter(
  kvstore_sql_dropprefix_total, "kvstore sqlite dropPrefix operations"
)
declarePublicCounter(kvstore_sql_query_total, "kvstore sqlite query operations")

# --- Conflict / rollback counters ---

declarePublicCounter(kvstore_sql_put_conflict_total, "kvstore sqlite put CAS conflicts")
declarePublicCounter(
  kvstore_sql_delete_conflict_total, "kvstore sqlite delete CAS conflicts"
)
declarePublicCounter(
  kvstore_sql_putatomic_conflict_total, "kvstore sqlite putAtomic CAS conflict keys"
)
declarePublicCounter(
  kvstore_sql_putatomic_rollback_total, "kvstore sqlite putAtomic rollbacks"
)
declarePublicCounter(
  kvstore_sql_deleteatomic_conflict_total, "kvstore sqlite deleteAtomic CAS conflicts"
)
declarePublicCounter(
  kvstore_sql_deleteatomic_rollback_total, "kvstore sqlite deleteAtomic rollbacks"
)
declarePublicCounter(
  kvstore_sql_moveatomic_error_total, "kvstore sqlite moveKeysAtomic failures"
)
declarePublicCounter(
  kvstore_sql_dropprefix_error_total, "kvstore sqlite dropPrefix failures"
)

# --- Duration histograms ---

declarePublicHistogram(
  kvstore_sql_has_duration_seconds, "kvstore sqlite has duration", buckets = ioBuckets
)
declarePublicHistogram(
  kvstore_sql_get_duration_seconds, "kvstore sqlite get duration", buckets = ioBuckets
)
declarePublicHistogram(
  kvstore_sql_put_duration_seconds, "kvstore sqlite put duration", buckets = ioBuckets
)
declarePublicHistogram(
  kvstore_sql_delete_duration_seconds,
  "kvstore sqlite delete duration",
  buckets = ioBuckets,
)
declarePublicHistogram(
  kvstore_sql_putatomic_duration_seconds,
  "kvstore sqlite putAtomic duration",
  buckets = ioBuckets,
)
declarePublicHistogram(
  kvstore_sql_deleteatomic_duration_seconds,
  "kvstore sqlite deleteAtomic duration",
  buckets = ioBuckets,
)
declarePublicHistogram(
  kvstore_sql_moveatomic_duration_seconds,
  "kvstore sqlite moveKeysAtomic duration",
  buckets = ioBuckets,
)
declarePublicHistogram(
  kvstore_sql_dropprefix_duration_seconds,
  "kvstore sqlite dropPrefix duration",
  buckets = ioBuckets,
)
declarePublicHistogram(
  kvstore_sql_query_duration_seconds,
  "kvstore sqlite query iterator creation duration",
  buckets = ioBuckets,
)

# --- Batch size histograms ---

declarePublicHistogram(
  kvstore_sql_put_batch_size,
  "kvstore sqlite put batch size (records)",
  buckets = batchBuckets,
)
declarePublicHistogram(
  kvstore_sql_delete_batch_size,
  "kvstore sqlite delete batch size (records)",
  buckets = batchBuckets,
)
declarePublicHistogram(
  kvstore_sql_putatomic_batch_size,
  "kvstore sqlite putAtomic batch size (records)",
  buckets = batchBuckets,
)
declarePublicHistogram(
  kvstore_sql_deleteatomic_batch_size,
  "kvstore sqlite deleteAtomic batch size (records)",
  buckets = batchBuckets,
)

# --- Value size histograms ---

declarePublicHistogram(
  kvstore_sql_put_value_bytes,
  "kvstore sqlite put value size (bytes)",
  buckets = sizeBuckets,
)
declarePublicHistogram(
  kvstore_sql_get_value_bytes,
  "kvstore sqlite get value size (bytes)",
  buckets = sizeBuckets,
)

# --- In-flight gauges ---

declarePublicGauge(kvstore_sql_inflight_has, "kvstore sqlite in-flight has operations")
declarePublicGauge(kvstore_sql_inflight_get, "kvstore sqlite in-flight get operations")
declarePublicGauge(kvstore_sql_inflight_put, "kvstore sqlite in-flight put operations")
declarePublicGauge(
  kvstore_sql_inflight_delete, "kvstore sqlite in-flight delete operations"
)
declarePublicGauge(
  kvstore_sql_inflight_putatomic, "kvstore sqlite in-flight putAtomic operations"
)
declarePublicGauge(
  kvstore_sql_inflight_deleteatomic, "kvstore sqlite in-flight deleteAtomic operations"
)

# --- Active iterators ---

declarePublicGauge(
  kvstore_sql_active_iterators, "kvstore sqlite active query iterators"
)
