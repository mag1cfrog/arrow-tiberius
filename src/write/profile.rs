//! Benchmark-only profiling support for the write path.

use std::time::Duration;

#[cfg(feature = "bench-profile")]
mod enabled {
    use std::{cell::RefCell, time::Duration};

    use tiberius::BulkLoadStats;

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
        /// Number of bulk packet drain attempts inside Tiberius.
        pub packet_write_calls: u64,
        /// Number of full TDS bulk-load packets written before finalization.
        pub packets_written: u64,
        /// Total packet payload bytes written before finalization.
        pub packet_payload_bytes: u64,
        /// Largest full packet payload written before finalization.
        pub max_packet_payload_bytes: u64,
        /// Largest buffered bulk-load byte count before a packet drain attempt.
        pub max_buffered_bytes_before_write: u64,
        /// Buffered bulk-load tail left after the latest packet drain attempt.
        pub buffered_bytes_after_last_write: u64,
        /// Final `EndOfMessage` packet payload bytes written during finalization.
        pub finalized_packet_payload_bytes: u64,
        /// Time spent inside bulk-load packet drain attempts.
        pub bulk_write_packets_elapsed: Duration,
        /// Number of lower-level connection writes issued by bulk load.
        pub bulk_write_to_wire_calls: u64,
        /// Time spent awaiting lower-level connection writes from bulk load.
        pub bulk_write_to_wire_elapsed: Duration,
        /// Payload bytes passed to lower-level connection writes from bulk load.
        pub bulk_write_to_wire_payload_bytes: u64,
        /// Slowest lower-level connection write awaited by bulk load.
        pub bulk_max_write_to_wire_elapsed: Duration,
        /// Largest lower-level connection write payload from bulk load.
        pub bulk_max_write_to_wire_payload_bytes: u64,
        /// Number of explicit bulk-load flushes.
        pub bulk_flush_calls: u64,
        /// Time spent awaiting explicit bulk-load flushes.
        pub bulk_flush_elapsed: Duration,
        /// Slowest explicit bulk-load flush.
        pub bulk_max_flush_elapsed: Duration,
        /// Time spent finalizing the bulk-load request.
        pub bulk_finalize_elapsed: Duration,
        /// Time spent awaiting the final `EndOfMessage` packet write.
        pub bulk_finalize_write_to_wire_elapsed: Duration,
        /// Time spent awaiting the final explicit flush.
        pub bulk_finalize_flush_elapsed: Duration,
        /// Time spent waiting for the server result after final bulk flush.
        pub bulk_finalize_result_elapsed: Duration,
        /// Number of bulk-load packets passed through the framed connection sink.
        pub bulk_connection_write_calls: u64,
        /// Payload bytes passed through the framed connection sink.
        pub bulk_connection_write_payload_bytes: u64,
        /// Time spent waiting for the framed connection sink to accept packets.
        pub bulk_connection_write_ready_elapsed: Duration,
        /// Time spent encoding packets into the framed connection sink.
        pub bulk_connection_write_encode_elapsed: Duration,
        /// Time spent flushing packets through the framed connection sink.
        pub bulk_connection_write_flush_elapsed: Duration,
        /// Slowest framed connection sink readiness wait.
        pub bulk_connection_write_max_ready_elapsed: Duration,
        /// Slowest packet encode into the framed connection sink.
        pub bulk_connection_write_max_encode_elapsed: Duration,
        /// Slowest framed connection sink flush.
        pub bulk_connection_write_max_flush_elapsed: Duration,
        /// Largest payload passed through the framed connection sink.
        pub bulk_connection_write_max_payload_bytes: u64,
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

    pub(crate) fn record_bulk_load_stats(stats: BulkLoadStats) {
        let packet = stats.packet;
        let timing = stats.write_timing;
        let connection = timing.connection_write;

        with_profile(|profile| {
            profile.packet_write_calls = profile
                .packet_write_calls
                .saturating_add(packet.write_packets_calls);
            profile.packets_written = profile
                .packets_written
                .saturating_add(packet.packets_written);
            profile.packet_payload_bytes = profile
                .packet_payload_bytes
                .saturating_add(packet.packet_payload_bytes);
            profile.max_packet_payload_bytes = profile
                .max_packet_payload_bytes
                .max(usize_to_u64_saturating(packet.max_packet_payload_bytes));
            profile.max_buffered_bytes_before_write =
                profile
                    .max_buffered_bytes_before_write
                    .max(usize_to_u64_saturating(
                        packet.max_buffered_bytes_before_write,
                    ));
            profile.buffered_bytes_after_last_write =
                usize_to_u64_saturating(packet.buffered_bytes_after_last_write);
            profile.finalized_packet_payload_bytes = profile
                .finalized_packet_payload_bytes
                .saturating_add(usize_to_u64_saturating(
                    packet.finalized_packet_payload_bytes,
                ));
            profile.bulk_write_packets_elapsed += timing.write_packets_elapsed;
            profile.bulk_write_to_wire_calls = profile
                .bulk_write_to_wire_calls
                .saturating_add(timing.write_to_wire_calls);
            profile.bulk_write_to_wire_elapsed += timing.write_to_wire_elapsed;
            profile.bulk_write_to_wire_payload_bytes = profile
                .bulk_write_to_wire_payload_bytes
                .saturating_add(timing.write_to_wire_payload_bytes);
            profile.bulk_max_write_to_wire_elapsed = profile
                .bulk_max_write_to_wire_elapsed
                .max(timing.max_write_to_wire_elapsed);
            profile.bulk_max_write_to_wire_payload_bytes = profile
                .bulk_max_write_to_wire_payload_bytes
                .max(usize_to_u64_saturating(
                    timing.max_write_to_wire_payload_bytes,
                ));
            profile.bulk_flush_calls = profile.bulk_flush_calls.saturating_add(timing.flush_calls);
            profile.bulk_flush_elapsed += timing.flush_elapsed;
            profile.bulk_max_flush_elapsed =
                profile.bulk_max_flush_elapsed.max(timing.max_flush_elapsed);
            profile.bulk_finalize_elapsed += timing.finalize_elapsed;
            profile.bulk_finalize_write_to_wire_elapsed += timing.finalize_write_to_wire_elapsed;
            profile.bulk_finalize_flush_elapsed += timing.finalize_flush_elapsed;
            profile.bulk_finalize_result_elapsed += timing.finalize_result_elapsed;
            profile.bulk_connection_write_calls = profile
                .bulk_connection_write_calls
                .saturating_add(connection.calls);
            profile.bulk_connection_write_payload_bytes = profile
                .bulk_connection_write_payload_bytes
                .saturating_add(connection.payload_bytes);
            profile.bulk_connection_write_ready_elapsed += connection.ready_elapsed;
            profile.bulk_connection_write_encode_elapsed += connection.encode_elapsed;
            profile.bulk_connection_write_flush_elapsed += connection.flush_elapsed;
            profile.bulk_connection_write_max_ready_elapsed = profile
                .bulk_connection_write_max_ready_elapsed
                .max(connection.max_ready_elapsed);
            profile.bulk_connection_write_max_encode_elapsed = profile
                .bulk_connection_write_max_encode_elapsed
                .max(connection.max_encode_elapsed);
            profile.bulk_connection_write_max_flush_elapsed = profile
                .bulk_connection_write_max_flush_elapsed
                .max(connection.max_flush_elapsed);
            profile.bulk_connection_write_max_payload_bytes = profile
                .bulk_connection_write_max_payload_bytes
                .max(usize_to_u64_saturating(connection.max_payload_bytes));
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

    pub(crate) fn record_bulk_load_stats(_stats: tiberius::BulkLoadStats) {}
}

#[cfg(not(feature = "bench-profile"))]
pub(crate) use disabled::*;
#[cfg(feature = "bench-profile")]
pub use enabled::{DirectWriteProfile, finish_direct_write_profile, start_direct_write_profile};
#[cfg(feature = "bench-profile")]
pub(crate) use enabled::{
    record_accepted_batch, record_append_encode, record_bulk_load_stats, record_measure_batch,
    record_null_cell, record_nvarchar_utf16_bytes, record_row_range, record_row_range_split,
    record_send_total, record_varbinary_bytes,
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
        super::record_bulk_load_stats(tiberius::BulkLoadStats {
            packet: tiberius::BulkLoadPacketStats {
                write_packets_calls: 23,
                packets_written: 29,
                packet_payload_bytes: 31,
                max_packet_payload_bytes: 37,
                max_buffered_bytes_before_write: 41,
                buffered_bytes_after_last_write: 43,
                finalized_packet_payload_bytes: 47,
            },
            write_timing: tiberius::BulkLoadWriteTimingStats {
                write_packets_elapsed: Duration::from_millis(53),
                write_to_wire_calls: 59,
                write_to_wire_elapsed: Duration::from_millis(61),
                write_to_wire_payload_bytes: 67,
                max_write_to_wire_elapsed: Duration::from_millis(71),
                max_write_to_wire_payload_bytes: 73,
                flush_calls: 79,
                flush_elapsed: Duration::from_millis(83),
                max_flush_elapsed: Duration::from_millis(89),
                finalize_elapsed: Duration::from_millis(97),
                finalize_write_to_wire_elapsed: Duration::from_millis(101),
                finalize_flush_elapsed: Duration::from_millis(103),
                finalize_result_elapsed: Duration::from_millis(107),
                connection_write: tiberius::BulkLoadConnectionWriteStats {
                    calls: 109,
                    payload_bytes: 113,
                    ready_elapsed: Duration::from_millis(127),
                    encode_elapsed: Duration::from_millis(131),
                    flush_elapsed: Duration::from_millis(137),
                    max_ready_elapsed: Duration::from_millis(139),
                    max_encode_elapsed: Duration::from_millis(149),
                    max_flush_elapsed: Duration::from_millis(151),
                    max_payload_bytes: 157,
                },
            },
        });

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
        assert_eq!(profile.packet_write_calls, 23);
        assert_eq!(profile.packets_written, 29);
        assert_eq!(profile.packet_payload_bytes, 31);
        assert_eq!(profile.max_packet_payload_bytes, 37);
        assert_eq!(profile.max_buffered_bytes_before_write, 41);
        assert_eq!(profile.buffered_bytes_after_last_write, 43);
        assert_eq!(profile.finalized_packet_payload_bytes, 47);
        assert_eq!(
            profile.bulk_write_packets_elapsed,
            Duration::from_millis(53)
        );
        assert_eq!(profile.bulk_write_to_wire_calls, 59);
        assert_eq!(
            profile.bulk_write_to_wire_elapsed,
            Duration::from_millis(61)
        );
        assert_eq!(profile.bulk_write_to_wire_payload_bytes, 67);
        assert_eq!(
            profile.bulk_max_write_to_wire_elapsed,
            Duration::from_millis(71)
        );
        assert_eq!(profile.bulk_max_write_to_wire_payload_bytes, 73);
        assert_eq!(profile.bulk_flush_calls, 79);
        assert_eq!(profile.bulk_flush_elapsed, Duration::from_millis(83));
        assert_eq!(profile.bulk_max_flush_elapsed, Duration::from_millis(89));
        assert_eq!(profile.bulk_finalize_elapsed, Duration::from_millis(97));
        assert_eq!(
            profile.bulk_finalize_write_to_wire_elapsed,
            Duration::from_millis(101)
        );
        assert_eq!(
            profile.bulk_finalize_flush_elapsed,
            Duration::from_millis(103)
        );
        assert_eq!(
            profile.bulk_finalize_result_elapsed,
            Duration::from_millis(107)
        );
        assert_eq!(profile.bulk_connection_write_calls, 109);
        assert_eq!(profile.bulk_connection_write_payload_bytes, 113);
        assert_eq!(
            profile.bulk_connection_write_ready_elapsed,
            Duration::from_millis(127)
        );
        assert_eq!(
            profile.bulk_connection_write_encode_elapsed,
            Duration::from_millis(131)
        );
        assert_eq!(
            profile.bulk_connection_write_flush_elapsed,
            Duration::from_millis(137)
        );
        assert_eq!(
            profile.bulk_connection_write_max_ready_elapsed,
            Duration::from_millis(139)
        );
        assert_eq!(
            profile.bulk_connection_write_max_encode_elapsed,
            Duration::from_millis(149)
        );
        assert_eq!(
            profile.bulk_connection_write_max_flush_elapsed,
            Duration::from_millis(151)
        );
        assert_eq!(profile.bulk_connection_write_max_payload_bytes, 157);
        assert!(super::finish_direct_write_profile().is_none());
    }
}
