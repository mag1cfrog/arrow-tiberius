//! Benchmark-only profiling support for the write path.

use std::time::Duration;

#[cfg(feature = "bench-profile")]
mod enabled {
    use std::{
        cell::{Cell, RefCell},
        time::Duration,
    };

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
        /// Number of bulk-load packets written through the direct packet path.
        pub bulk_direct_packet_write_calls: u64,
        /// Payload bytes written through the direct packet path.
        pub bulk_direct_packet_payload_bytes: u64,
        /// Header bytes written through the direct packet path.
        pub bulk_direct_packet_header_bytes: u64,
        /// Largest packet payload written through the direct packet path.
        pub bulk_direct_packet_max_payload_bytes: u64,
        /// Number of final `EndOfMessage` direct packets.
        pub bulk_direct_packet_final_calls: u64,
        /// Payload bytes in final `EndOfMessage` direct packets.
        pub bulk_direct_packet_final_payload_bytes: u64,
        /// Header bytes in final `EndOfMessage` direct packets.
        pub bulk_direct_packet_final_header_bytes: u64,
        /// Direct packet writes observed on raw, non-TLS streams.
        pub bulk_direct_packet_raw_stream_calls: u64,
        /// Direct packet writes observed on TLS streams.
        pub bulk_direct_packet_tls_stream_calls: u64,
        /// Low-level write calls issued by the direct packet path.
        pub bulk_direct_packet_low_level_write_calls: u64,
        /// Low-level bytes written by the direct packet path.
        pub bulk_direct_packet_low_level_write_bytes: u64,
        /// Largest low-level write accepted by the direct packet path.
        pub bulk_direct_packet_max_low_level_write_bytes: u64,
        /// Time spent in low-level writes for the direct packet path.
        pub bulk_direct_packet_write_elapsed: Duration,
        /// Slowest low-level write in the direct packet path.
        pub bulk_direct_packet_max_write_elapsed: Duration,
        /// Low-level write calls used for direct packet headers.
        pub bulk_direct_packet_header_write_calls: u64,
        /// Header bytes accepted by low-level writes.
        pub bulk_direct_packet_header_write_bytes: u64,
        /// Largest header byte count accepted by one low-level write.
        pub bulk_direct_packet_header_max_write_bytes: u64,
        /// Time spent in low-level header writes.
        pub bulk_direct_packet_header_write_elapsed: Duration,
        /// Slowest low-level header write.
        pub bulk_direct_packet_header_max_write_elapsed: Duration,
        /// Low-level header writes that accepted fewer bytes than remained.
        pub bulk_direct_packet_header_partial_writes: u64,
        /// Low-level write calls used for direct packet payloads.
        pub bulk_direct_packet_payload_write_calls: u64,
        /// Payload bytes accepted by low-level writes.
        pub bulk_direct_packet_payload_write_bytes: u64,
        /// Largest payload byte count accepted by one low-level write.
        pub bulk_direct_packet_payload_max_write_bytes: u64,
        /// Time spent in low-level payload writes.
        pub bulk_direct_packet_payload_write_elapsed: Duration,
        /// Slowest low-level payload write.
        pub bulk_direct_packet_payload_max_write_elapsed: Duration,
        /// Low-level payload writes that accepted fewer bytes than remained.
        pub bulk_direct_packet_payload_partial_writes: u64,
        /// Number of low-level `poll_write` attempts.
        pub bulk_direct_packet_poll_write_polls: u64,
        /// Number of low-level `poll_write` attempts that returned `Pending`.
        pub bulk_direct_packet_poll_write_pending_count: u64,
        /// Time spent waiting after low-level `poll_write` returned `Pending`.
        pub bulk_direct_packet_poll_write_pending_elapsed: Duration,
        /// Slowest wait after low-level `poll_write` returned `Pending`.
        pub bulk_direct_packet_poll_write_max_pending_elapsed: Duration,
        /// Number of low-level `poll_write` attempts that returned ready.
        pub bulk_direct_packet_poll_write_ready_count: u64,
        /// Time spent in ready low-level `poll_write` attempts.
        pub bulk_direct_packet_poll_write_ready_elapsed: Duration,
        /// Slowest ready low-level `poll_write` attempt.
        pub bulk_direct_packet_poll_write_max_ready_elapsed: Duration,
        /// Explicit flush calls issued by the direct packet path.
        pub bulk_direct_packet_flush_calls: u64,
        /// Time spent flushing the direct packet path.
        pub bulk_direct_packet_flush_elapsed: Duration,
        /// Slowest flush in the direct packet path.
        pub bulk_direct_packet_max_flush_elapsed: Duration,
        /// Number of direct packet flush polls that returned `Pending`.
        pub bulk_direct_packet_flush_pending_count: u64,
        /// Time spent waiting after direct packet flush polls returned `Pending`.
        pub bulk_direct_packet_flush_pending_elapsed: Duration,
        /// Slowest wait after a direct packet flush poll returned `Pending`.
        pub bulk_direct_packet_flush_max_pending_elapsed: Duration,
    }

    impl DirectWriteProfile {
        /// Returns approximate send time outside the append/encode closure.
        pub fn send_without_append_encode(&self) -> Duration {
            self.send_total.saturating_sub(self.append_encode)
        }
    }

    thread_local! {
        static DIRECT_PROFILE: RefCell<Option<DirectWriteProfile>> = const { RefCell::new(None) };
        static DIRECT_DATE_FAST_PATH_DISABLED: Cell<bool> = const { Cell::new(false) };
        static DIRECT_FIXED_WIDTH_FAST_PATH_DISABLED: Cell<bool> = const { Cell::new(false) };
    }

    /// Scoped benchmark override that disables the direct date fast path.
    #[derive(Debug)]
    pub struct DirectDateFastPathOverride {
        previous: bool,
    }

    /// Scoped benchmark override that disables the whole fixed-width fast path.
    #[derive(Debug)]
    pub struct DirectFixedWidthFastPathOverride {
        previous: bool,
    }

    impl Drop for DirectDateFastPathOverride {
        fn drop(&mut self) {
            DIRECT_DATE_FAST_PATH_DISABLED.with(|disabled| disabled.set(self.previous));
        }
    }

    impl Drop for DirectFixedWidthFastPathOverride {
        fn drop(&mut self) {
            DIRECT_FIXED_WIDTH_FAST_PATH_DISABLED.with(|disabled| disabled.set(self.previous));
        }
    }

    /// Disables direct date fixed-width fast-path encoding for the current scope.
    pub fn disable_direct_date_fast_path_for_scope() -> DirectDateFastPathOverride {
        let previous = DIRECT_DATE_FAST_PATH_DISABLED.with(|disabled| {
            let previous = disabled.get();
            disabled.set(true);
            previous
        });

        DirectDateFastPathOverride { previous }
    }

    /// Disables direct fixed-width fast-path encoding for the current scope.
    pub fn disable_direct_fixed_width_fast_path_for_scope() -> DirectFixedWidthFastPathOverride {
        let previous = DIRECT_FIXED_WIDTH_FAST_PATH_DISABLED.with(|disabled| {
            let previous = disabled.get();
            disabled.set(true);
            previous
        });

        DirectFixedWidthFastPathOverride { previous }
    }

    pub(crate) fn direct_date_fast_path_disabled() -> bool {
        DIRECT_DATE_FAST_PATH_DISABLED.with(Cell::get)
    }

    pub(crate) fn direct_fixed_width_fast_path_disabled() -> bool {
        DIRECT_FIXED_WIDTH_FAST_PATH_DISABLED.with(Cell::get)
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
        let direct = timing.direct_packet_write;

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
            profile.bulk_direct_packet_write_calls = profile
                .bulk_direct_packet_write_calls
                .saturating_add(direct.calls);
            profile.bulk_direct_packet_payload_bytes = profile
                .bulk_direct_packet_payload_bytes
                .saturating_add(direct.payload_bytes);
            profile.bulk_direct_packet_header_bytes = profile
                .bulk_direct_packet_header_bytes
                .saturating_add(direct.header_bytes);
            profile.bulk_direct_packet_max_payload_bytes = profile
                .bulk_direct_packet_max_payload_bytes
                .max(usize_to_u64_saturating(direct.max_payload_bytes));
            profile.bulk_direct_packet_final_calls = profile
                .bulk_direct_packet_final_calls
                .saturating_add(direct.final_calls);
            profile.bulk_direct_packet_final_payload_bytes = profile
                .bulk_direct_packet_final_payload_bytes
                .saturating_add(direct.final_payload_bytes);
            profile.bulk_direct_packet_final_header_bytes = profile
                .bulk_direct_packet_final_header_bytes
                .saturating_add(direct.final_header_bytes);
            profile.bulk_direct_packet_raw_stream_calls = profile
                .bulk_direct_packet_raw_stream_calls
                .saturating_add(direct.raw_stream_calls);
            profile.bulk_direct_packet_tls_stream_calls = profile
                .bulk_direct_packet_tls_stream_calls
                .saturating_add(direct.tls_stream_calls);
            profile.bulk_direct_packet_low_level_write_calls = profile
                .bulk_direct_packet_low_level_write_calls
                .saturating_add(direct.write_calls);
            profile.bulk_direct_packet_low_level_write_bytes = profile
                .bulk_direct_packet_low_level_write_bytes
                .saturating_add(direct.write_bytes);
            profile.bulk_direct_packet_max_low_level_write_bytes = profile
                .bulk_direct_packet_max_low_level_write_bytes
                .max(usize_to_u64_saturating(direct.max_write_bytes));
            profile.bulk_direct_packet_write_elapsed += direct.write_elapsed;
            profile.bulk_direct_packet_max_write_elapsed = profile
                .bulk_direct_packet_max_write_elapsed
                .max(direct.max_write_elapsed);
            profile.bulk_direct_packet_header_write_calls = profile
                .bulk_direct_packet_header_write_calls
                .saturating_add(direct.header_write_calls);
            profile.bulk_direct_packet_header_write_bytes = profile
                .bulk_direct_packet_header_write_bytes
                .saturating_add(direct.header_write_bytes);
            profile.bulk_direct_packet_header_max_write_bytes = profile
                .bulk_direct_packet_header_max_write_bytes
                .max(usize_to_u64_saturating(direct.header_max_write_bytes));
            profile.bulk_direct_packet_header_write_elapsed += direct.header_write_elapsed;
            profile.bulk_direct_packet_header_max_write_elapsed = profile
                .bulk_direct_packet_header_max_write_elapsed
                .max(direct.header_max_write_elapsed);
            profile.bulk_direct_packet_header_partial_writes = profile
                .bulk_direct_packet_header_partial_writes
                .saturating_add(direct.header_partial_writes);
            profile.bulk_direct_packet_payload_write_calls = profile
                .bulk_direct_packet_payload_write_calls
                .saturating_add(direct.payload_write_calls);
            profile.bulk_direct_packet_payload_write_bytes = profile
                .bulk_direct_packet_payload_write_bytes
                .saturating_add(direct.payload_write_bytes);
            profile.bulk_direct_packet_payload_max_write_bytes = profile
                .bulk_direct_packet_payload_max_write_bytes
                .max(usize_to_u64_saturating(direct.payload_max_write_bytes));
            profile.bulk_direct_packet_payload_write_elapsed += direct.payload_write_elapsed;
            profile.bulk_direct_packet_payload_max_write_elapsed = profile
                .bulk_direct_packet_payload_max_write_elapsed
                .max(direct.payload_max_write_elapsed);
            profile.bulk_direct_packet_payload_partial_writes = profile
                .bulk_direct_packet_payload_partial_writes
                .saturating_add(direct.payload_partial_writes);
            profile.bulk_direct_packet_poll_write_polls = profile
                .bulk_direct_packet_poll_write_polls
                .saturating_add(direct.poll_write_polls);
            profile.bulk_direct_packet_poll_write_pending_count = profile
                .bulk_direct_packet_poll_write_pending_count
                .saturating_add(direct.poll_write_pending_count);
            profile.bulk_direct_packet_poll_write_pending_elapsed +=
                direct.poll_write_pending_elapsed;
            profile.bulk_direct_packet_poll_write_max_pending_elapsed = profile
                .bulk_direct_packet_poll_write_max_pending_elapsed
                .max(direct.poll_write_max_pending_elapsed);
            profile.bulk_direct_packet_poll_write_ready_count = profile
                .bulk_direct_packet_poll_write_ready_count
                .saturating_add(direct.poll_write_ready_count);
            profile.bulk_direct_packet_poll_write_ready_elapsed += direct.poll_write_ready_elapsed;
            profile.bulk_direct_packet_poll_write_max_ready_elapsed = profile
                .bulk_direct_packet_poll_write_max_ready_elapsed
                .max(direct.poll_write_max_ready_elapsed);
            profile.bulk_direct_packet_flush_calls = profile
                .bulk_direct_packet_flush_calls
                .saturating_add(direct.flush_calls);
            profile.bulk_direct_packet_flush_elapsed += direct.flush_elapsed;
            profile.bulk_direct_packet_max_flush_elapsed = profile
                .bulk_direct_packet_max_flush_elapsed
                .max(direct.max_flush_elapsed);
            profile.bulk_direct_packet_flush_pending_count = profile
                .bulk_direct_packet_flush_pending_count
                .saturating_add(direct.flush_pending_count);
            profile.bulk_direct_packet_flush_pending_elapsed += direct.flush_pending_elapsed;
            profile.bulk_direct_packet_flush_max_pending_elapsed = profile
                .bulk_direct_packet_flush_max_pending_elapsed
                .max(direct.flush_max_pending_elapsed);
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

    pub(crate) fn direct_date_fast_path_disabled() -> bool {
        false
    }

    pub(crate) fn direct_fixed_width_fast_path_disabled() -> bool {
        false
    }
}

#[cfg(not(feature = "bench-profile"))]
pub(crate) use disabled::*;
#[cfg(feature = "bench-profile")]
pub use enabled::{
    DirectDateFastPathOverride, DirectFixedWidthFastPathOverride, DirectWriteProfile,
    disable_direct_date_fast_path_for_scope, disable_direct_fixed_width_fast_path_for_scope,
    finish_direct_write_profile, start_direct_write_profile,
};
#[cfg(feature = "bench-profile")]
pub(crate) use enabled::{
    direct_date_fast_path_disabled, direct_fixed_width_fast_path_disabled, record_accepted_batch,
    record_append_encode, record_bulk_load_stats, record_measure_batch, record_null_cell,
    record_nvarchar_utf16_bytes, record_row_range, record_row_range_split, record_send_total,
    record_varbinary_bytes,
};

pub(crate) fn record_elapsed<T>(start: std::time::Instant, record: fn(Duration), value: T) -> T {
    record(start.elapsed());
    value
}

#[cfg(all(test, feature = "bench-profile"))]
mod tests {
    use std::{thread, time::Duration};

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
                direct_packet_write: tiberius::BulkLoadDirectPacketWriteStats {
                    calls: 163,
                    payload_bytes: 167,
                    header_bytes: 173,
                    max_payload_bytes: 179,
                    final_calls: 181,
                    final_payload_bytes: 191,
                    final_header_bytes: 193,
                    raw_stream_calls: 197,
                    tls_stream_calls: 199,
                    write_calls: 179,
                    write_bytes: 181,
                    max_write_bytes: 191,
                    write_elapsed: Duration::from_millis(193),
                    max_write_elapsed: Duration::from_millis(197),
                    header_write_calls: 211,
                    header_write_bytes: 223,
                    header_max_write_bytes: 227,
                    header_write_elapsed: Duration::from_millis(229),
                    header_max_write_elapsed: Duration::from_millis(233),
                    header_partial_writes: 239,
                    payload_write_calls: 241,
                    payload_write_bytes: 251,
                    payload_max_write_bytes: 257,
                    payload_write_elapsed: Duration::from_millis(263),
                    payload_max_write_elapsed: Duration::from_millis(269),
                    payload_partial_writes: 271,
                    poll_write_polls: 277,
                    poll_write_pending_count: 281,
                    poll_write_pending_elapsed: Duration::from_millis(283),
                    poll_write_max_pending_elapsed: Duration::from_millis(293),
                    poll_write_ready_count: 307,
                    poll_write_ready_elapsed: Duration::from_millis(311),
                    poll_write_max_ready_elapsed: Duration::from_millis(313),
                    flush_calls: 199,
                    flush_elapsed: Duration::from_millis(211),
                    max_flush_elapsed: Duration::from_millis(223),
                    flush_pending_count: 317,
                    flush_pending_elapsed: Duration::from_millis(331),
                    flush_max_pending_elapsed: Duration::from_millis(337),
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
        assert_eq!(profile.bulk_direct_packet_write_calls, 163);
        assert_eq!(profile.bulk_direct_packet_payload_bytes, 167);
        assert_eq!(profile.bulk_direct_packet_header_bytes, 173);
        assert_eq!(profile.bulk_direct_packet_max_payload_bytes, 179);
        assert_eq!(profile.bulk_direct_packet_final_calls, 181);
        assert_eq!(profile.bulk_direct_packet_final_payload_bytes, 191);
        assert_eq!(profile.bulk_direct_packet_final_header_bytes, 193);
        assert_eq!(profile.bulk_direct_packet_raw_stream_calls, 197);
        assert_eq!(profile.bulk_direct_packet_tls_stream_calls, 199);
        assert_eq!(profile.bulk_direct_packet_low_level_write_calls, 179);
        assert_eq!(profile.bulk_direct_packet_low_level_write_bytes, 181);
        assert_eq!(profile.bulk_direct_packet_max_low_level_write_bytes, 191);
        assert_eq!(
            profile.bulk_direct_packet_write_elapsed,
            Duration::from_millis(193)
        );
        assert_eq!(
            profile.bulk_direct_packet_max_write_elapsed,
            Duration::from_millis(197)
        );
        assert_eq!(profile.bulk_direct_packet_header_write_calls, 211);
        assert_eq!(profile.bulk_direct_packet_header_write_bytes, 223);
        assert_eq!(profile.bulk_direct_packet_header_max_write_bytes, 227);
        assert_eq!(
            profile.bulk_direct_packet_header_write_elapsed,
            Duration::from_millis(229)
        );
        assert_eq!(
            profile.bulk_direct_packet_header_max_write_elapsed,
            Duration::from_millis(233)
        );
        assert_eq!(profile.bulk_direct_packet_header_partial_writes, 239);
        assert_eq!(profile.bulk_direct_packet_payload_write_calls, 241);
        assert_eq!(profile.bulk_direct_packet_payload_write_bytes, 251);
        assert_eq!(profile.bulk_direct_packet_payload_max_write_bytes, 257);
        assert_eq!(
            profile.bulk_direct_packet_payload_write_elapsed,
            Duration::from_millis(263)
        );
        assert_eq!(
            profile.bulk_direct_packet_payload_max_write_elapsed,
            Duration::from_millis(269)
        );
        assert_eq!(profile.bulk_direct_packet_payload_partial_writes, 271);
        assert_eq!(profile.bulk_direct_packet_poll_write_polls, 277);
        assert_eq!(profile.bulk_direct_packet_poll_write_pending_count, 281);
        assert_eq!(
            profile.bulk_direct_packet_poll_write_pending_elapsed,
            Duration::from_millis(283)
        );
        assert_eq!(
            profile.bulk_direct_packet_poll_write_max_pending_elapsed,
            Duration::from_millis(293)
        );
        assert_eq!(profile.bulk_direct_packet_poll_write_ready_count, 307);
        assert_eq!(
            profile.bulk_direct_packet_poll_write_ready_elapsed,
            Duration::from_millis(311)
        );
        assert_eq!(
            profile.bulk_direct_packet_poll_write_max_ready_elapsed,
            Duration::from_millis(313)
        );
        assert_eq!(profile.bulk_direct_packet_flush_calls, 199);
        assert_eq!(
            profile.bulk_direct_packet_flush_elapsed,
            Duration::from_millis(211)
        );
        assert_eq!(
            profile.bulk_direct_packet_max_flush_elapsed,
            Duration::from_millis(223)
        );
        assert_eq!(profile.bulk_direct_packet_flush_pending_count, 317);
        assert_eq!(
            profile.bulk_direct_packet_flush_pending_elapsed,
            Duration::from_millis(331)
        );
        assert_eq!(
            profile.bulk_direct_packet_flush_max_pending_elapsed,
            Duration::from_millis(337)
        );
        assert!(super::finish_direct_write_profile().is_none());
    }

    #[test]
    fn scoped_fast_path_overrides_do_not_cross_threads() -> Result<(), String> {
        assert!(!super::direct_date_fast_path_disabled());
        assert!(!super::direct_fixed_width_fast_path_disabled());

        let _date_guard = super::disable_direct_date_fast_path_for_scope();
        let _fixed_width_guard = super::disable_direct_fixed_width_fast_path_for_scope();

        assert!(super::direct_date_fast_path_disabled());
        assert!(super::direct_fixed_width_fast_path_disabled());

        let handle = thread::spawn(|| {
            (
                super::direct_date_fast_path_disabled(),
                super::direct_fixed_width_fast_path_disabled(),
            )
        });
        let thread_state = handle
            .join()
            .map_err(|_| "fast path override thread panicked".to_owned())?;

        assert_eq!(thread_state, (false, false));
        Ok(())
    }

    #[test]
    fn scoped_fast_path_overrides_restore_previous_thread_state() {
        assert!(!super::direct_date_fast_path_disabled());
        assert!(!super::direct_fixed_width_fast_path_disabled());

        {
            let _date_guard = super::disable_direct_date_fast_path_for_scope();
            let _fixed_width_guard = super::disable_direct_fixed_width_fast_path_for_scope();

            assert!(super::direct_date_fast_path_disabled());
            assert!(super::direct_fixed_width_fast_path_disabled());
        }

        assert!(!super::direct_date_fast_path_disabled());
        assert!(!super::direct_fixed_width_fast_path_disabled());
    }
}
