{.push raises: [].}

import pkg/metrics
export metrics

# =============================================================================
# FS Backend Metrics
# =============================================================================

const
  # Custom buckets tuned for I/O latencies: 100µs → 5s
  ioBuckets = [
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
    Inf,
  ]

  # Batch size buckets: 1 → 100k records
  batchBuckets = [1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0, 1000.0, 10000.0,
    100000.0, Inf]

  # Value size buckets (bytes): 64B → 1MB
  sizeBuckets = [
    64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, Inf,
  ]

# --- Operation counters ---

declarePublicCounter(kvstore_fs_has_total, "kvstore fs has operations")
declarePublicCounter(kvstore_fs_get_total, "kvstore fs get operations")
declarePublicCounter(kvstore_fs_put_total, "kvstore fs put operations")
declarePublicCounter(kvstore_fs_delete_total, "kvstore fs delete operations")
declarePublicCounter(kvstore_fs_query_total, "kvstore fs query operations")

# --- Conflict counters ---

declarePublicCounter(kvstore_fs_put_conflict_total, "kvstore fs put CAS conflicts")
declarePublicCounter(kvstore_fs_delete_conflict_total, "kvstore fs delete CAS conflicts")

# --- Duration histograms ---

declarePublicHistogram(
  kvstore_fs_has_duration_seconds, "kvstore fs has duration", buckets = ioBuckets
)
declarePublicHistogram(
  kvstore_fs_get_duration_seconds, "kvstore fs get duration", buckets = ioBuckets
)
declarePublicHistogram(
  kvstore_fs_put_duration_seconds, "kvstore fs put duration", buckets = ioBuckets
)
declarePublicHistogram(
  kvstore_fs_delete_duration_seconds, "kvstore fs delete duration", buckets = ioBuckets
)
declarePublicHistogram(
  kvstore_fs_query_duration_seconds,
  "kvstore fs query iterator creation duration",
  buckets = ioBuckets,
)

# --- Batch size histograms ---

declarePublicHistogram(
  kvstore_fs_put_batch_size,
  "kvstore fs put batch size (records)",
  buckets = batchBuckets,
)
declarePublicHistogram(
  kvstore_fs_delete_batch_size,
  "kvstore fs delete batch size (records)",
  buckets = batchBuckets,
)

# --- Value size histograms ---

declarePublicHistogram(
  kvstore_fs_put_value_bytes, "kvstore fs put value size (bytes)", buckets = sizeBuckets
)
declarePublicHistogram(
  kvstore_fs_get_value_bytes, "kvstore fs get value size (bytes)", buckets = sizeBuckets
)

# --- In-flight gauges ---

declarePublicGauge(kvstore_fs_inflight_has, "kvstore fs in-flight has operations")
declarePublicGauge(kvstore_fs_inflight_get, "kvstore fs in-flight get operations")
declarePublicGauge(kvstore_fs_inflight_put, "kvstore fs in-flight put operations")
declarePublicGauge(
  kvstore_fs_inflight_delete, "kvstore fs in-flight delete operations"
)

# --- Active iterators ---

declarePublicGauge(kvstore_fs_active_iterators, "kvstore fs active query iterators")
