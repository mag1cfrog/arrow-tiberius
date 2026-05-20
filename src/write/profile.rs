//! Benchmark-only profiling support for the write path.

use std::time::Duration;

#[cfg(feature = "bench-profile")]
mod enabled {
    use std::{cell::RefCell, time::Duration};

    /// Accumulated timings for the direct raw writer path.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct DirectWriteProfile {
        /// Time spent measuring runtime batches before encoding.
        pub measure_batch: Duration,
        /// Time spent splitting measured batches into bounded row ranges.
        pub row_range_split: Duration,
        /// Time spent encoding/appending raw rows before Tiberius writes packets.
        pub append_encode: Duration,
        /// Total time spent in raw-row send calls, including append encoding.
        pub send_total: Duration,
        /// Number of rows accepted by the direct writer.
        pub rows: u64,
        /// Number of batches accepted by the direct writer.
        pub batches: u64,
        /// Number of raw row ranges sent.
        pub row_ranges: u64,
        /// Total encoded raw row bytes sent or appended.
        pub encoded_bytes: u64,
        /// Largest encoded row range in bytes.
        pub max_row_range_bytes: u64,
        /// Non-null SQL Server `nvarchar` payload bytes after UTF-16 encoding.
        pub nvarchar_utf16_bytes: u64,
        /// Non-null SQL Server `varbinary` payload bytes.
        pub varbinary_bytes: u64,
        /// Number of null cells observed by the profiled direct writer path.
        pub null_cells: u64,
    }

    impl DirectWriteProfile {
        /// Returns approximate send time outside the append/encode closure.
        pub fn send_without_append_encode(&self) -> Duration {
            self.send_total.saturating_sub(self.append_encode)
        }
    }

    thread_local! {
        static DIRECT_PROFILE: RefCell<Option<DirectWriteProfile>> = const { RefCell::new(None) };
    }

    /// Starts direct writer profiling on the current thread.
    ///
    /// Existing data for the current thread is cleared.
    pub fn start_direct_write_profile() {
        DIRECT_PROFILE.with(|profile| {
            *profile.borrow_mut() = Some(DirectWriteProfile::default());
        });
    }

    /// Stops direct writer profiling on the current thread and returns the data.
    pub fn finish_direct_write_profile() -> Option<DirectWriteProfile> {
        DIRECT_PROFILE.with(|profile| profile.borrow_mut().take())
    }

    pub(crate) fn record_measure_batch(elapsed: Duration) {
        with_profile(|profile| profile.measure_batch += elapsed);
    }

    pub(crate) fn record_row_range_split(elapsed: Duration) {
        with_profile(|profile| profile.row_range_split += elapsed);
    }

    pub(crate) fn record_append_encode(elapsed: Duration) {
        with_profile(|profile| profile.append_encode += elapsed);
    }

    pub(crate) fn record_send_total(elapsed: Duration) {
        with_profile(|profile| profile.send_total += elapsed);
    }

    pub(crate) fn record_accepted_batch(rows: usize) {
        with_profile(|profile| {
            profile.rows = profile.rows.saturating_add(usize_to_u64_saturating(rows));
            profile.batches = profile.batches.saturating_add(1);
        });
    }

    pub(crate) fn record_row_range(encoded_bytes: usize) {
        with_profile(|profile| {
            let encoded_bytes = usize_to_u64_saturating(encoded_bytes);
            profile.row_ranges = profile.row_ranges.saturating_add(1);
            profile.encoded_bytes = profile.encoded_bytes.saturating_add(encoded_bytes);
            profile.max_row_range_bytes = profile.max_row_range_bytes.max(encoded_bytes);
        });
    }

    pub(crate) fn record_nvarchar_utf16_bytes(encoded_bytes: usize) {
        with_profile(|profile| {
            profile.nvarchar_utf16_bytes = profile
                .nvarchar_utf16_bytes
                .saturating_add(usize_to_u64_saturating(encoded_bytes));
        });
    }

    pub(crate) fn record_varbinary_bytes(encoded_bytes: usize) {
        with_profile(|profile| {
            profile.varbinary_bytes = profile
                .varbinary_bytes
                .saturating_add(usize_to_u64_saturating(encoded_bytes));
        });
    }

    pub(crate) fn record_null_cell() {
        with_profile(|profile| {
            profile.null_cells = profile.null_cells.saturating_add(1);
        });
    }

    fn with_profile(update: impl FnOnce(&mut DirectWriteProfile)) {
        DIRECT_PROFILE.with(|profile| {
            if let Some(profile) = profile.borrow_mut().as_mut() {
                update(profile);
            }
        });
    }

    fn usize_to_u64_saturating(value: usize) -> u64 {
        u64::try_from(value).unwrap_or(u64::MAX)
    }
}

#[cfg(not(feature = "bench-profile"))]
mod disabled {
    use std::time::Duration;

    pub(crate) fn record_measure_batch(_elapsed: Duration) {}

    pub(crate) fn record_row_range_split(_elapsed: Duration) {}

    pub(crate) fn record_append_encode(_elapsed: Duration) {}

    pub(crate) fn record_send_total(_elapsed: Duration) {}

    pub(crate) fn record_accepted_batch(_rows: usize) {}

    pub(crate) fn record_row_range(_encoded_bytes: usize) {}

    pub(crate) fn record_nvarchar_utf16_bytes(_encoded_bytes: usize) {}

    pub(crate) fn record_varbinary_bytes(_encoded_bytes: usize) {}

    pub(crate) fn record_null_cell() {}
}

#[cfg(not(feature = "bench-profile"))]
pub(crate) use disabled::*;
#[cfg(feature = "bench-profile")]
pub use enabled::{DirectWriteProfile, finish_direct_write_profile, start_direct_write_profile};
#[cfg(feature = "bench-profile")]
pub(crate) use enabled::{
    record_accepted_batch, record_append_encode, record_measure_batch, record_null_cell,
    record_nvarchar_utf16_bytes, record_row_range, record_row_range_split, record_send_total,
    record_varbinary_bytes,
};

pub(crate) fn record_elapsed<T>(start: std::time::Instant, record: fn(Duration), value: T) -> T {
    record(start.elapsed());
    value
}

#[cfg(all(test, feature = "bench-profile"))]
mod tests {
    use std::time::Duration;

    #[test]
    fn direct_profile_accumulates_and_resets_current_thread_data() {
        super::start_direct_write_profile();
        super::record_measure_batch(Duration::from_millis(2));
        super::record_row_range_split(Duration::from_millis(3));
        super::record_append_encode(Duration::from_millis(5));
        super::record_send_total(Duration::from_millis(13));
        super::record_accepted_batch(7);
        super::record_row_range(11);
        super::record_nvarchar_utf16_bytes(17);
        super::record_varbinary_bytes(19);
        super::record_null_cell();

        let profile = super::finish_direct_write_profile().unwrap();

        assert_eq!(profile.measure_batch, Duration::from_millis(2));
        assert_eq!(profile.row_range_split, Duration::from_millis(3));
        assert_eq!(profile.append_encode, Duration::from_millis(5));
        assert_eq!(profile.send_total, Duration::from_millis(13));
        assert_eq!(
            profile.send_without_append_encode(),
            Duration::from_millis(8)
        );
        assert_eq!(profile.rows, 7);
        assert_eq!(profile.batches, 1);
        assert_eq!(profile.row_ranges, 1);
        assert_eq!(profile.encoded_bytes, 11);
        assert_eq!(profile.max_row_range_bytes, 11);
        assert_eq!(profile.nvarchar_utf16_bytes, 17);
        assert_eq!(profile.varbinary_bytes, 19);
        assert_eq!(profile.null_cells, 1);
        assert!(super::finish_direct_write_profile().is_none());
    }
}
