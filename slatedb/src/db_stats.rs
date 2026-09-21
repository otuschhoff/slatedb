use parking_lot::Mutex;
use slatedb_common::metrics::{
    CounterFn, GaugeFn, HistogramFn, MetricsRecorderHelper, UpDownCounterFn, LATENCY_BOUNDARIES,
};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
/// Unix start timestamp in milliseconds for the oldest active backpressure wait.
/// Snapshot consumers can calculate its current age during a stall; zero means no active wait.
pub const BACKPRESSURE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS: &str =
    db_stat_name!("backpressure_oldest_active_started_unix_millis");
pub const BACKPRESSURE_OUTCOME_COUNT: &str = db_stat_name!("backpressure_outcome_count");
pub const BATCH_WRITE_QUEUE_DEPTH: &str = db_stat_name!("batch_write_queue_depth");
pub const BATCH_WRITE_QUEUE_WAIT_SECONDS: &str = db_stat_name!("batch_write_queue_wait_seconds");
pub const BATCH_WRITE_QUEUE_OUTCOME_COUNT: &str = db_stat_name!("batch_write_queue_outcome_count");
pub const BATCH_WRITE_SERVICE_ACTIVE: &str = db_stat_name!("batch_write_service_active");
pub const BATCH_WRITE_SERVICE_SECONDS: &str = db_stat_name!("batch_write_service_seconds");
/// Unix start timestamp in milliseconds for the oldest active writer service.
/// Snapshot consumers can calculate its current age during a stall; zero means no active service.
pub const BATCH_WRITE_SERVICE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS: &str =
    db_stat_name!("batch_write_service_oldest_active_started_unix_millis");
pub const BATCH_WRITE_SERVICE_OUTCOME_COUNT: &str =
    db_stat_name!("batch_write_service_outcome_count");
pub const OUTCOME_LABEL: &str = "outcome";
pub const OUTCOME_SUCCESS: &str = "success";
pub const OUTCOME_FAILURE: &str = "failure";
pub const OUTCOME_CANCELLATION: &str = "cancellation";
pub const OUTCOME_TIMEOUT: &str = "timeout";
pub const QUEUE_OUTCOME_PROCESSED: &str = "processed";
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
pub const MULTI_GET_CALLS: &str = db_stat_name!("multi_get_calls");
pub const MULTI_GET_INPUT_KEYS: &str = db_stat_name!("multi_get_input_keys");
pub const MULTI_GET_UNIQUE_KEYS: &str = db_stat_name!("multi_get_unique_keys");
pub const MULTI_GET_SST_VISITS: &str = db_stat_name!("multi_get_sst_visits");
pub const MULTI_GET_CANDIDATE_KEYS: &str = db_stat_name!("multi_get_candidate_keys");
pub const MULTI_GET_NEEDED_BLOCKS: &str = db_stat_name!("multi_get_needed_blocks");
pub const MULTI_GET_COALESCED_READS: &str = db_stat_name!("multi_get_coalesced_reads");
pub const MULTI_GET_NEEDED_BLOCK_BYTES: &str = db_stat_name!("multi_get_needed_block_bytes");
pub const MULTI_GET_COALESCED_READ_BYTES: &str = db_stat_name!("multi_get_coalesced_read_bytes");
pub const MULTI_GET_PROJECTED_READS_GAP_8: &str = db_stat_name!("multi_get_projected_reads_gap_8");
pub const MULTI_GET_PROJECTED_READ_BYTES_GAP_8: &str =
    db_stat_name!("multi_get_projected_read_bytes_gap_8");
pub const MULTI_GET_PROJECTED_READS_GAP_32: &str =
    db_stat_name!("multi_get_projected_reads_gap_32");
pub const MULTI_GET_PROJECTED_READ_BYTES_GAP_32: &str =
    db_stat_name!("multi_get_projected_read_bytes_gap_32");
pub const MULTI_GET_PROJECTED_READS_GAP_128: &str =
    db_stat_name!("multi_get_projected_reads_gap_128");
pub const MULTI_GET_PROJECTED_READ_BYTES_GAP_128: &str =
    db_stat_name!("multi_get_projected_read_bytes_gap_128");
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
    pub(crate) multi_get_calls: Arc<dyn CounterFn>,
    pub(crate) multi_get_input_keys: Arc<dyn CounterFn>,
    pub(crate) multi_get_unique_keys: Arc<dyn CounterFn>,
    pub(crate) multi_get_sst_visits: Arc<dyn CounterFn>,
    pub(crate) multi_get_candidate_keys: Arc<dyn CounterFn>,
    pub(crate) multi_get_needed_blocks: Arc<dyn CounterFn>,
    pub(crate) multi_get_coalesced_reads: Arc<dyn CounterFn>,
    pub(crate) multi_get_needed_block_bytes: Arc<dyn CounterFn>,
    pub(crate) multi_get_coalesced_read_bytes: Arc<dyn CounterFn>,
    pub(crate) multi_get_projected_reads_gap_8: Arc<dyn CounterFn>,
    pub(crate) multi_get_projected_read_bytes_gap_8: Arc<dyn CounterFn>,
    pub(crate) multi_get_projected_reads_gap_32: Arc<dyn CounterFn>,
    pub(crate) multi_get_projected_read_bytes_gap_32: Arc<dyn CounterFn>,
    pub(crate) multi_get_projected_reads_gap_128: Arc<dyn CounterFn>,
    pub(crate) multi_get_projected_read_bytes_gap_128: Arc<dyn CounterFn>,
    pub(crate) backpressure_count: Arc<dyn CounterFn>,
    pub(crate) backpressure_lifecycle: Arc<ActiveDurationLifecycle>,
    pub(crate) backpressure_timeout_count: Arc<dyn CounterFn>,
    pub(crate) batch_write_queue_depth: Arc<dyn UpDownCounterFn>,
    pub(crate) batch_write_queue_wait_seconds: Arc<dyn HistogramFn>,
    pub(crate) batch_write_queue_processed: Arc<dyn CounterFn>,
    pub(crate) batch_write_queue_failed: Arc<dyn CounterFn>,
    pub(crate) batch_write_queue_cancelled: Arc<dyn CounterFn>,
    pub(crate) batch_write_service_lifecycle: Arc<ActiveDurationLifecycle>,
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
        let backpressure_waiters = recorder.up_down_counter(BACKPRESSURE_WAITERS).register();
        let backpressure_wait_seconds = recorder
            .histogram(BACKPRESSURE_WAIT_SECONDS, LATENCY_BOUNDARIES)
            .register();
        let backpressure_lifecycle = Arc::new(ActiveDurationLifecycle::new(
            backpressure_waiters.clone(),
            backpressure_wait_seconds.clone(),
            recorder
                .gauge(BACKPRESSURE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS)
                .register(),
            LifecycleOutcomeCounters::new(recorder, BACKPRESSURE_OUTCOME_COUNT),
        ));
        let batch_write_service_active = recorder
            .up_down_counter(BATCH_WRITE_SERVICE_ACTIVE)
            .register();
        let batch_write_service_seconds = recorder
            .histogram(BATCH_WRITE_SERVICE_SECONDS, LATENCY_BOUNDARIES)
            .register();
        let batch_write_service_lifecycle = Arc::new(ActiveDurationLifecycle::new(
            batch_write_service_active.clone(),
            batch_write_service_seconds.clone(),
            recorder
                .gauge(BATCH_WRITE_SERVICE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS)
                .register(),
            LifecycleOutcomeCounters::new(recorder, BATCH_WRITE_SERVICE_OUTCOME_COUNT),
        ));
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
            multi_get_calls: recorder.counter(MULTI_GET_CALLS).register(),
            multi_get_input_keys: recorder.counter(MULTI_GET_INPUT_KEYS).register(),
            multi_get_unique_keys: recorder.counter(MULTI_GET_UNIQUE_KEYS).register(),
            multi_get_sst_visits: recorder.counter(MULTI_GET_SST_VISITS).register(),
            multi_get_candidate_keys: recorder.counter(MULTI_GET_CANDIDATE_KEYS).register(),
            multi_get_needed_blocks: recorder.counter(MULTI_GET_NEEDED_BLOCKS).register(),
            multi_get_coalesced_reads: recorder.counter(MULTI_GET_COALESCED_READS).register(),
            multi_get_needed_block_bytes: recorder.counter(MULTI_GET_NEEDED_BLOCK_BYTES).register(),
            multi_get_coalesced_read_bytes: recorder
                .counter(MULTI_GET_COALESCED_READ_BYTES)
                .register(),
            multi_get_projected_reads_gap_8: recorder
                .counter(MULTI_GET_PROJECTED_READS_GAP_8)
                .register(),
            multi_get_projected_read_bytes_gap_8: recorder
                .counter(MULTI_GET_PROJECTED_READ_BYTES_GAP_8)
                .register(),
            multi_get_projected_reads_gap_32: recorder
                .counter(MULTI_GET_PROJECTED_READS_GAP_32)
                .register(),
            multi_get_projected_read_bytes_gap_32: recorder
                .counter(MULTI_GET_PROJECTED_READ_BYTES_GAP_32)
                .register(),
            multi_get_projected_reads_gap_128: recorder
                .counter(MULTI_GET_PROJECTED_READS_GAP_128)
                .register(),
            multi_get_projected_read_bytes_gap_128: recorder
                .counter(MULTI_GET_PROJECTED_READ_BYTES_GAP_128)
                .register(),
            backpressure_count: recorder.counter(BACKPRESSURE_COUNT).register(),
            backpressure_lifecycle,
            backpressure_timeout_count: recorder
                .counter(BACKPRESSURE_OUTCOME_COUNT)
                .labels(&[(OUTCOME_LABEL, OUTCOME_TIMEOUT)])
                .register(),
            batch_write_queue_depth: recorder.up_down_counter(BATCH_WRITE_QUEUE_DEPTH).register(),
            batch_write_queue_wait_seconds: recorder
                .histogram(BATCH_WRITE_QUEUE_WAIT_SECONDS, LATENCY_BOUNDARIES)
                .register(),
            batch_write_queue_processed: recorder
                .counter(BATCH_WRITE_QUEUE_OUTCOME_COUNT)
                .labels(&[(OUTCOME_LABEL, QUEUE_OUTCOME_PROCESSED)])
                .register(),
            batch_write_queue_failed: recorder
                .counter(BATCH_WRITE_QUEUE_OUTCOME_COUNT)
                .labels(&[(OUTCOME_LABEL, OUTCOME_FAILURE)])
                .register(),
            batch_write_queue_cancelled: recorder
                .counter(BATCH_WRITE_QUEUE_OUTCOME_COUNT)
                .labels(&[(OUTCOME_LABEL, OUTCOME_CANCELLATION)])
                .register(),
            batch_write_service_lifecycle,
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

static PROCESS_MONOTONIC_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

#[derive(Clone, Copy)]
pub(crate) enum LifecycleOutcome {
    Success,
    Failure,
    Cancellation,
}

pub(crate) struct LifecycleOutcomeCounters {
    success: Arc<dyn CounterFn>,
    failure: Arc<dyn CounterFn>,
    cancellation: Arc<dyn CounterFn>,
}

impl LifecycleOutcomeCounters {
    fn new(recorder: &MetricsRecorderHelper, metric_name: &str) -> Self {
        let register = |outcome| {
            recorder
                .counter(metric_name)
                .labels(&[(OUTCOME_LABEL, outcome)])
                .register()
        };
        Self {
            success: register(OUTCOME_SUCCESS),
            failure: register(OUTCOME_FAILURE),
            cancellation: register(OUTCOME_CANCELLATION),
        }
    }

    fn record(&self, outcome: LifecycleOutcome) {
        match outcome {
            LifecycleOutcome::Success => self.success.increment(1),
            LifecycleOutcome::Failure => self.failure.increment(1),
            LifecycleOutcome::Cancellation => self.cancellation.increment(1),
        }
    }
}

pub(crate) struct ActiveDurationLifecycle {
    active: Arc<dyn UpDownCounterFn>,
    duration: Arc<dyn HistogramFn>,
    oldest_started_unix_millis: Arc<dyn GaugeFn>,
    active_starts: Mutex<BTreeMap<i64, (usize, i64)>>,
    outcomes: LifecycleOutcomeCounters,
}

impl ActiveDurationLifecycle {
    fn new(
        active: Arc<dyn UpDownCounterFn>,
        duration: Arc<dyn HistogramFn>,
        oldest_started_unix_millis: Arc<dyn GaugeFn>,
        outcomes: LifecycleOutcomeCounters,
    ) -> Self {
        Self {
            active,
            duration,
            oldest_started_unix_millis,
            active_starts: Mutex::new(BTreeMap::new()),
            outcomes,
        }
    }

    pub(crate) fn start(self: &Arc<Self>) -> ActiveDurationGuard {
        ActiveDurationGuard::new(self.clone())
    }
}

pub(crate) struct ActiveDurationGuard {
    lifecycle: Arc<ActiveDurationLifecycle>,
    started: Instant,
    started_monotonic_nanos: i64,
    outcome: LifecycleOutcome,
}

impl ActiveDurationGuard {
    pub(crate) fn new(lifecycle: Arc<ActiveDurationLifecycle>) -> Self {
        let started = Instant::now();
        let started_monotonic_nanos =
            i64::try_from(started.duration_since(*PROCESS_MONOTONIC_EPOCH).as_nanos())
                .unwrap_or(i64::MAX)
                .max(1);
        let started_unix_millis = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(i64::MAX);
        lifecycle.active.increment(1);
        {
            let mut active_starts = lifecycle.active_starts.lock();
            active_starts
                .entry(started_monotonic_nanos)
                .and_modify(|(count, _)| *count += 1)
                .or_insert((1, started_unix_millis));
            lifecycle
                .oldest_started_unix_millis
                .set(active_starts.first_key_value().unwrap().1 .1);
        }
        Self {
            lifecycle,
            started,
            started_monotonic_nanos,
            outcome: LifecycleOutcome::Cancellation,
        }
    }

    pub(crate) fn complete(mut self, outcome: LifecycleOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for ActiveDurationGuard {
    fn drop(&mut self) {
        self.lifecycle
            .duration
            .record(self.started.elapsed().as_secs_f64());
        self.lifecycle.active.increment(-1);
        self.lifecycle.outcomes.record(self.outcome);
        let mut active_starts = self.lifecycle.active_starts.lock();
        let (count, _) = active_starts
            .get_mut(&self.started_monotonic_nanos)
            .expect("active duration start must be registered");
        *count -= 1;
        if *count == 0 {
            active_starts.remove(&self.started_monotonic_nanos);
        }
        self.lifecycle.oldest_started_unix_millis.set(
            active_starts
                .first_key_value()
                .map_or(0, |(_, (_, start))| *start),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb_common::metrics::{lookup_metric, lookup_metric_with_labels, test_recorder_helper};

    #[test]
    fn active_duration_guard_drop_records_cancellation_and_clears_active_state() {
        let (recorder, helper) = test_recorder_helper();
        let stats = DbStats::new(&helper);

        let guard = stats.batch_write_service_lifecycle.start();
        assert_eq!(
            lookup_metric(&recorder, BATCH_WRITE_SERVICE_ACTIVE),
            Some(1)
        );
        assert!(lookup_metric(
            &recorder,
            BATCH_WRITE_SERVICE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS
        )
        .is_some_and(|start| start > 0));

        drop(guard);

        assert_eq!(
            lookup_metric(&recorder, BATCH_WRITE_SERVICE_ACTIVE),
            Some(0)
        );
        assert_eq!(
            lookup_metric(
                &recorder,
                BATCH_WRITE_SERVICE_OLDEST_ACTIVE_STARTED_UNIX_MILLIS
            ),
            Some(0)
        );
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                BATCH_WRITE_SERVICE_OUTCOME_COUNT,
                &[(OUTCOME_LABEL, OUTCOME_CANCELLATION)]
            ),
            Some(1)
        );
    }
}
