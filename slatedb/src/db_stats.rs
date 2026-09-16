use slatedb_common::metrics::{
    CounterFn, GaugeFn, HistogramFn, MetricsRecorderHelper, UpDownCounterFn, LATENCY_BOUNDARIES,
};
use std::sync::Arc;
use std::time::Instant;

pub use crate::merge_operator::MERGE_OPERATOR_OPERANDS;

use crate::merge_operator::{
    MERGE_OPERATOR_FLUSH_PATH, MERGE_OPERATOR_OPERANDS_DESCRIPTION, MERGE_OPERATOR_PATH_LABEL,
    MERGE_OPERATOR_READ_PATH,
};

macro_rules! db_stat_name {
    ($suffix:expr) => {
        concat!("slatedb.db.", $suffix)
    };
}

pub const REQUEST_COUNT: &str = db_stat_name!("request_count");
pub const WRITE_OPS: &str = db_stat_name!("write_ops");
pub const WRITE_BATCH_COUNT: &str = db_stat_name!("write_batch_count");
pub const BACKPRESSURE_COUNT: &str = db_stat_name!("backpressure_count");
pub const BACKPRESSURE_WAITERS: &str = db_stat_name!("backpressure_waiters");
pub const BACKPRESSURE_WAIT_SECONDS: &str = db_stat_name!("backpressure_wait_seconds");
pub const BATCH_WRITE_QUEUE_DEPTH: &str = db_stat_name!("batch_write_queue_depth");
pub const BATCH_WRITE_QUEUE_WAIT_SECONDS: &str = db_stat_name!("batch_write_queue_wait_seconds");
pub const BATCH_WRITE_SERVICE_ACTIVE: &str = db_stat_name!("batch_write_service_active");
pub const BATCH_WRITE_SERVICE_SECONDS: &str = db_stat_name!("batch_write_service_seconds");
pub const L0_STALL_COUNT: &str = db_stat_name!("l0_stall_count");
pub const L0_STALL_TYPE_LABEL: &str = "type";
pub const L0_STALL_TYPE_NUM_SSTS: &str = "num_ssts";
pub const L0_STALL_TYPE_NUM_SSTS_PER_KEY: &str = "num_ssts_per_key";
pub const IMMUTABLE_MEMTABLE_FLUSHES: &str = db_stat_name!("immutable_memtable_flushes");
pub const TOTAL_MEM_SIZE_BYTES: &str = db_stat_name!("total_mem_size_bytes");
pub const L0_SST_COUNT: &str = db_stat_name!("l0_sst_count");
pub const SEGMENT_MAX_L0_SST_COUNT: &str = db_stat_name!("segment_max_l0_sst_count");
pub const SORTED_RUN_COUNT: &str = db_stat_name!("sorted_run_count");
pub const SST_VIEW_COUNT: &str = db_stat_name!("sst_view_count");
pub const SST_COUNT: &str = db_stat_name!("sst_count");
pub const EXTERNAL_DB_COUNT: &str = db_stat_name!("external_db_count");
pub const L0_FLUSH_BYTES: &str = db_stat_name!("l0_flush_bytes");
pub const SST_FILTER_FALSE_POSITIVE_COUNT: &str = db_stat_name!("sst_filter_false_positive_count");
pub const SST_FILTER_POSITIVE_COUNT: &str = db_stat_name!("sst_filter_positive_count");
pub const SST_FILTER_NEGATIVE_COUNT: &str = db_stat_name!("sst_filter_negative_count");
/// Size of key value pairs inserted into the memtable after batch merge operators and overwrites
/// are collapsed.
/// Use as denominator to calculate write amplification:
///   write_amp = (`WAL_FLUSH_BYTES` + `L0_FLUSH_BYTES` + `compactor::stats::BYTES_COMPACTED`)
///               / `MEMTABLE_WRITE_BYTES`
pub const MEMTABLE_WRITE_BYTES: &str = db_stat_name!("memtable_write_bytes");

/// Label key distinguishing filter metrics by query kind. Value is one of
/// [`FILTER_KIND_POINT`], [`FILTER_KIND_PREFIX`] or [`FILTER_KIND_RANGE`].
pub const FILTER_KIND_LABEL: &str = "kind";
pub const FILTER_KIND_POINT: &str = "point";
pub const FILTER_KIND_PREFIX: &str = "prefix";
pub const FILTER_KIND_RANGE: &str = "range";

pub(crate) struct DbStatsInner {
    pub(crate) immutable_memtable_flushes: Arc<dyn CounterFn>,
    pub(crate) sst_filter_point_false_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_point_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_point_negatives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_prefix_false_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_prefix_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_prefix_negatives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_range_false_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_range_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_range_negatives: Arc<dyn CounterFn>,
    pub(crate) backpressure_count: Arc<dyn CounterFn>,
    pub(crate) backpressure_waiters: Arc<dyn UpDownCounterFn>,
    pub(crate) backpressure_wait_seconds: Arc<dyn HistogramFn>,
    pub(crate) batch_write_queue_depth: Arc<dyn UpDownCounterFn>,
    pub(crate) batch_write_queue_wait_seconds: Arc<dyn HistogramFn>,
    pub(crate) batch_write_service_active: Arc<dyn UpDownCounterFn>,
    pub(crate) batch_write_service_seconds: Arc<dyn HistogramFn>,
    pub(crate) l0_stall_count_num_ssts: Arc<dyn CounterFn>,
    pub(crate) l0_stall_count_num_ssts_per_key: Arc<dyn CounterFn>,
    pub(crate) get_requests: Arc<dyn CounterFn>,
    pub(crate) scan_requests: Arc<dyn CounterFn>,
    pub(crate) flush_requests: Arc<dyn CounterFn>,
    pub(crate) write_batch_count: Arc<dyn CounterFn>,
    pub(crate) write_ops: Arc<dyn CounterFn>,
    pub(crate) total_mem_size_bytes: Arc<dyn GaugeFn>,
    pub(crate) l0_sst_count: Arc<dyn GaugeFn>,
    pub(crate) segment_max_l0_sst_count: Arc<dyn GaugeFn>,
    pub(crate) sorted_run_count: Arc<dyn GaugeFn>,
    pub(crate) sst_view_count: Arc<dyn GaugeFn>,
    pub(crate) sst_count: Arc<dyn GaugeFn>,
    pub(crate) external_db_count: Arc<dyn GaugeFn>,
    pub(crate) l0_flush_bytes: Arc<dyn CounterFn>,
    pub(crate) merge_operator_read_operands: Arc<dyn CounterFn>,
    pub(crate) merge_operator_flush_operands: Arc<dyn CounterFn>,
    pub(crate) memtable_write_bytes: Arc<dyn CounterFn>,
}

#[derive(Clone)]
pub(crate) struct DbStats {
    inner: Arc<DbStatsInner>,
}

impl std::ops::Deref for DbStats {
    type Target = DbStatsInner;

    #[inline]
    fn deref(&self) -> &DbStatsInner {
        &self.inner
    }
}

impl DbStats {
    pub(crate) fn new(recorder: &MetricsRecorderHelper) -> DbStats {
        let inner = DbStatsInner {
            immutable_memtable_flushes: recorder.counter(IMMUTABLE_MEMTABLE_FLUSHES).register(),
            sst_filter_point_false_positives: recorder
                .counter(SST_FILTER_FALSE_POSITIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_POINT)])
                .register(),
            sst_filter_point_positives: recorder
                .counter(SST_FILTER_POSITIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_POINT)])
                .register(),
            sst_filter_point_negatives: recorder
                .counter(SST_FILTER_NEGATIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_POINT)])
                .register(),
            sst_filter_prefix_false_positives: recorder
                .counter(SST_FILTER_FALSE_POSITIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_PREFIX)])
                .register(),
            sst_filter_prefix_positives: recorder
                .counter(SST_FILTER_POSITIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_PREFIX)])
                .register(),
            sst_filter_prefix_negatives: recorder
                .counter(SST_FILTER_NEGATIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_PREFIX)])
                .register(),
            sst_filter_range_false_positives: recorder
                .counter(SST_FILTER_FALSE_POSITIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_RANGE)])
                .register(),
            sst_filter_range_positives: recorder
                .counter(SST_FILTER_POSITIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_RANGE)])
                .register(),
            sst_filter_range_negatives: recorder
                .counter(SST_FILTER_NEGATIVE_COUNT)
                .labels(&[(FILTER_KIND_LABEL, FILTER_KIND_RANGE)])
                .register(),
            backpressure_count: recorder.counter(BACKPRESSURE_COUNT).register(),
            backpressure_waiters: recorder.up_down_counter(BACKPRESSURE_WAITERS).register(),
            backpressure_wait_seconds: recorder
                .histogram(BACKPRESSURE_WAIT_SECONDS, LATENCY_BOUNDARIES)
                .register(),
            batch_write_queue_depth: recorder.up_down_counter(BATCH_WRITE_QUEUE_DEPTH).register(),
            batch_write_queue_wait_seconds: recorder
                .histogram(BATCH_WRITE_QUEUE_WAIT_SECONDS, LATENCY_BOUNDARIES)
                .register(),
            batch_write_service_active: recorder
                .up_down_counter(BATCH_WRITE_SERVICE_ACTIVE)
                .register(),
            batch_write_service_seconds: recorder
                .histogram(BATCH_WRITE_SERVICE_SECONDS, LATENCY_BOUNDARIES)
                .register(),
            l0_stall_count_num_ssts: recorder
                .counter(L0_STALL_COUNT)
                .labels(&[(L0_STALL_TYPE_LABEL, L0_STALL_TYPE_NUM_SSTS)])
                .register(),
            l0_stall_count_num_ssts_per_key: recorder
                .counter(L0_STALL_COUNT)
                .labels(&[(L0_STALL_TYPE_LABEL, L0_STALL_TYPE_NUM_SSTS_PER_KEY)])
                .register(),
            get_requests: recorder
                .counter(REQUEST_COUNT)
                .labels(&[("op", "get")])
                .register(),
            scan_requests: recorder
                .counter(REQUEST_COUNT)
                .labels(&[("op", "scan")])
                .register(),
            flush_requests: recorder
                .counter(REQUEST_COUNT)
                .labels(&[("op", "flush")])
                .register(),
            write_batch_count: recorder.counter(WRITE_BATCH_COUNT).register(),
            write_ops: recorder.counter(WRITE_OPS).register(),
            total_mem_size_bytes: recorder.gauge(TOTAL_MEM_SIZE_BYTES).register(),
            l0_sst_count: recorder.gauge(L0_SST_COUNT).register(),
            segment_max_l0_sst_count: recorder.gauge(SEGMENT_MAX_L0_SST_COUNT).register(),
            sorted_run_count: recorder.gauge(SORTED_RUN_COUNT).register(),
            sst_view_count: recorder.gauge(SST_VIEW_COUNT).register(),
            sst_count: recorder.gauge(SST_COUNT).register(),
            external_db_count: recorder.gauge(EXTERNAL_DB_COUNT).register(),
            l0_flush_bytes: recorder.counter(L0_FLUSH_BYTES).register(),
            merge_operator_read_operands: recorder
                .counter(MERGE_OPERATOR_OPERANDS)
                .labels(&[(MERGE_OPERATOR_PATH_LABEL, MERGE_OPERATOR_READ_PATH)])
                .description(MERGE_OPERATOR_OPERANDS_DESCRIPTION)
                .register(),
            merge_operator_flush_operands: recorder
                .counter(MERGE_OPERATOR_OPERANDS)
                .labels(&[(MERGE_OPERATOR_PATH_LABEL, MERGE_OPERATOR_FLUSH_PATH)])
                .description(MERGE_OPERATOR_OPERANDS_DESCRIPTION)
                .register(),
            memtable_write_bytes: recorder.counter(MEMTABLE_WRITE_BYTES).register(),
        };
        DbStats {
            inner: Arc::new(inner),
        }
    }
}

pub(crate) struct ActiveDurationGuard {
    active: Arc<dyn UpDownCounterFn>,
    duration: Arc<dyn HistogramFn>,
    started: Instant,
}

impl ActiveDurationGuard {
    pub(crate) fn new(active: Arc<dyn UpDownCounterFn>, duration: Arc<dyn HistogramFn>) -> Self {
        active.increment(1);
        Self {
            active,
            duration,
            started: Instant::now(),
        }
    }
}

impl Drop for ActiveDurationGuard {
    fn drop(&mut self) {
        self.duration.record(self.started.elapsed().as_secs_f64());
        self.active.increment(-1);
    }
}
