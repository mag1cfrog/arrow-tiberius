//! Crate-owned tracing names and test capture helpers.

use std::{fmt::Write as _, time::Duration};

use crate::DiagnosticSet;

pub(crate) mod schema;
pub(crate) mod writer;

/// Crate-level tracing target used by `arrow-tiberius` instrumentation.
pub(crate) const TRACE_TARGET: &str = "arrow_tiberius";

/// Stable phase name for Arrow-to-SQL Server schema planning telemetry.
pub(crate) const SCHEMA_PLANNING_PHASE: &str = "schema_planning";

/// Stable span name for Arrow-to-SQL Server schema planning telemetry.
pub(crate) const SCHEMA_PLANNING_SPAN: &str = "arrow_tiberius.schema_planning";

/// Stable event marker emitted when schema planning starts.
pub(crate) const SCHEMA_PLANNING_STARTED_EVENT: &str = "arrow_tiberius.schema_planning.started";

/// Stable event marker emitted when schema planning completes successfully.
pub(crate) const SCHEMA_PLANNING_COMPLETED_EVENT: &str = "arrow_tiberius.schema_planning.completed";

/// Stable event marker emitted when schema planning fails.
pub(crate) const SCHEMA_PLANNING_FAILED_EVENT: &str = "arrow_tiberius.schema_planning.failed";

/// Stable phase name for bulk writer initialization telemetry.
pub(crate) const WRITER_INITIALIZATION_PHASE: &str = "writer_initialization";

/// Stable phase name for target metadata validation telemetry.
pub(crate) const TARGET_METADATA_VALIDATION_PHASE: &str = "target_metadata_validation";

/// Stable span name for bulk writer initialization telemetry.
pub(crate) const WRITER_INITIALIZATION_SPAN: &str = "arrow_tiberius.writer_initialization";

/// Stable event marker emitted when writer initialization starts.
pub(crate) const WRITER_INITIALIZATION_STARTED_EVENT: &str =
    "arrow_tiberius.writer_initialization.started";

/// Stable event marker emitted when writer initialization completes successfully.
pub(crate) const WRITER_INITIALIZATION_COMPLETED_EVENT: &str =
    "arrow_tiberius.writer_initialization.completed";

/// Stable event marker emitted when writer initialization fails.
pub(crate) const WRITER_INITIALIZATION_FAILED_EVENT: &str =
    "arrow_tiberius.writer_initialization.failed";

/// Stable event marker emitted when target metadata validation starts.
pub(crate) const TARGET_METADATA_VALIDATION_STARTED_EVENT: &str =
    "arrow_tiberius.target_metadata_validation.started";

/// Stable event marker emitted when target metadata validation completes successfully.
pub(crate) const TARGET_METADATA_VALIDATION_COMPLETED_EVENT: &str =
    "arrow_tiberius.target_metadata_validation.completed";

/// Stable event marker emitted when target metadata validation fails.
pub(crate) const TARGET_METADATA_VALIDATION_FAILED_EVENT: &str =
    "arrow_tiberius.target_metadata_validation.failed";

/// Stable phase name for bulk writer batch write telemetry.
pub(crate) const BATCH_WRITE_PHASE: &str = "batch_write";

/// Stable phase name for batch schema validation telemetry.
#[cfg(test)]
pub(crate) const BATCH_SCHEMA_VALIDATION_PHASE: &str = "batch_schema_validation";

/// Stable phase name for batch value conversion telemetry.
#[cfg(test)]
pub(crate) const VALUE_CONVERSION_PHASE: &str = "value_conversion";

/// Stable phase name for direct encoding telemetry.
pub(crate) const DIRECT_ENCODING_PHASE: &str = "direct_encoding";

/// Stable phase name for packet write telemetry.
pub(crate) const PACKET_WRITE_PHASE: &str = "packet_write";

/// Stable span name for bulk writer batch write telemetry.
pub(crate) const BATCH_WRITE_SPAN: &str = "arrow_tiberius.batch_write";

/// Stable event marker emitted when batch writing starts.
pub(crate) const BATCH_WRITE_STARTED_EVENT: &str = "arrow_tiberius.batch_write.started";

/// Stable event marker emitted when batch writing completes successfully.
pub(crate) const BATCH_WRITE_COMPLETED_EVENT: &str = "arrow_tiberius.batch_write.completed";

/// Stable event marker emitted when batch writing fails.
pub(crate) const BATCH_WRITE_FAILED_EVENT: &str = "arrow_tiberius.batch_write.failed";

/// Stable event marker emitted when direct raw measurement completes.
pub(crate) const DIRECT_RAW_MEASURED_EVENT: &str = "arrow_tiberius.direct_raw.measured";

/// Stable event marker emitted when direct raw row ranges are planned.
pub(crate) const DIRECT_RAW_RANGES_PLANNED_EVENT: &str = "arrow_tiberius.direct_raw.ranges_planned";

/// Stable event marker emitted when a direct raw range packet write completes.
pub(crate) const DIRECT_RAW_PACKET_WRITE_COMPLETED_EVENT: &str =
    "arrow_tiberius.direct_raw.packet_write.completed";

/// Stable event marker emitted when direct raw encoding or packet writing fails.
pub(crate) const DIRECT_RAW_FAILED_EVENT: &str = "arrow_tiberius.direct_raw.failed";

/// Stable phase name for bulk writer finish telemetry.
pub(crate) const FINISH_PHASE: &str = "finish";

/// Stable phase name for bulk writer finalize telemetry.
pub(crate) const FINALIZE_PHASE: &str = "finalize";

/// Stable span name for bulk writer finish telemetry.
pub(crate) const FINISH_SPAN: &str = "arrow_tiberius.finish";

/// Stable event marker emitted when writer finish starts.
pub(crate) const FINISH_STARTED_EVENT: &str = "arrow_tiberius.finish.started";

/// Stable event marker emitted when writer finish completes successfully.
pub(crate) const FINISH_COMPLETED_EVENT: &str = "arrow_tiberius.finish.completed";

/// Stable event marker emitted when writer finish fails during finalization.
pub(crate) const FINISH_FAILED_EVENT: &str = "arrow_tiberius.finish.failed";

pub(crate) fn duration_micros_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

pub(crate) fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(crate) fn diagnostic_codes(diagnostics: &DiagnosticSet) -> String {
    let mut codes = String::new();
    for diagnostic in diagnostics.all() {
        append_debug_name(&mut codes, diagnostic.code());
    }
    codes
}

pub(crate) fn append_debug_name<T: std::fmt::Debug>(target: &mut String, value: T) {
    if !target.is_empty() {
        target.push(',');
    }
    let _ = write!(target, "{value:?}");
}

pub(crate) fn append_text(target: &mut String, value: &str) {
    if !target.is_empty() {
        target.push(',');
    }
    target.push_str(value);
}

pub(crate) fn append_unique_text(target: &mut String, value: &str) {
    if target.split(',').any(|existing| existing == value) {
        return;
    }

    append_text(target, value);
}

/// Test-only span name used to prove tracing capture support.
#[cfg(test)]
pub(crate) const TEST_CAPTURE_SPAN: &str = "arrow_tiberius.test_capture";

/// Test-only event message used to prove tracing capture support.
#[cfg(test)]
pub(crate) const TEST_CAPTURE_EVENT: &str = "arrow_tiberius.test_capture_smoke";

/// Emits a test-only event through the same tracing path production code uses.
#[cfg(test)]
pub(crate) fn emit_test_capture_smoke_event() {
    let span = tracing::info_span!(
        target: TRACE_TARGET,
        TEST_CAPTURE_SPAN,
        phase = "test_capture"
    );
    let _span_guard = span.enter();

    tracing::info!(
        target: TRACE_TARGET,
        phase = "test_capture",
        smoke_count = 1_u64,
        smoke_label = "foundation",
        message = TEST_CAPTURE_EVENT
    );
}

/// Test-only helpers for scoped tracing capture.
#[cfg(test)]
pub(crate) mod test_support {
    use std::{
        collections::BTreeMap,
        fmt,
        sync::{Arc, Mutex},
    };

    use tracing::{
        Event, Level, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id},
    };
    use tracing_subscriber::{
        Layer, Registry,
        layer::{Context, SubscriberExt},
        registry::LookupSpan,
    };

    static CAPTURE_LOCK: Mutex<()> = Mutex::new(());

    /// Kind of captured tracing record.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum CapturedTraceKind {
        /// A span was created.
        Span,
        /// An event was emitted.
        Event,
    }

    /// One captured tracing span or event.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct CapturedTrace {
        kind: CapturedTraceKind,
        name: String,
        target: String,
        level: Level,
        span_name: Option<String>,
        fields: BTreeMap<String, String>,
    }

    impl CapturedTrace {
        /// Returns whether this record is a span or event.
        pub(crate) const fn kind(&self) -> CapturedTraceKind {
            self.kind
        }

        /// Returns the tracing metadata name.
        pub(crate) fn name(&self) -> &str {
            &self.name
        }

        /// Returns the tracing target.
        pub(crate) fn target(&self) -> &str {
            &self.target
        }

        /// Returns the tracing level.
        pub(crate) fn level(&self) -> Level {
            self.level
        }

        /// Returns the active span name captured for this event.
        pub(crate) fn span_name(&self) -> Option<&str> {
            self.span_name.as_deref()
        }

        /// Returns captured structured fields.
        pub(crate) const fn fields(&self) -> &BTreeMap<String, String> {
            &self.fields
        }

        fn contains_text(&self, text: &str) -> bool {
            self.name.contains(text)
                || self.target.contains(text)
                || self
                    .span_name
                    .as_ref()
                    .is_some_and(|span_name| span_name.contains(text))
                || self
                    .fields
                    .iter()
                    .any(|(key, value)| key.contains(text) || value.contains(text))
        }
    }

    /// Records captured inside one scoped subscriber.
    #[derive(Debug, Clone)]
    pub(crate) struct CapturedTraces {
        records: Arc<Mutex<Vec<CapturedTrace>>>,
    }

    impl CapturedTraces {
        /// Returns a cloned snapshot of captured records.
        pub(crate) fn records(&self) -> Result<Vec<CapturedTrace>, String> {
            self.records
                .lock()
                .map_err(|_| "captured traces lock poisoned".to_owned())
                .map(|records| records.clone())
        }

        /// Returns whether any captured record contains the supplied text.
        pub(crate) fn contains_text(&self, text: &str) -> Result<bool, String> {
            Ok(self
                .records()?
                .into_iter()
                .any(|record| record.contains_text(text)))
        }

        /// Fails if any captured record contains one of the supplied texts.
        pub(crate) fn assert_no_forbidden_text(&self, forbidden: &[&str]) -> Result<(), String> {
            let records = self.records()?;
            for text in forbidden {
                if records.iter().any(|record| record.contains_text(text)) {
                    return Err(format!("captured trace contained forbidden text `{text}`"));
                }
            }

            Ok(())
        }
    }

    /// Runs a closure with a scoped tracing subscriber and returns captured records.
    pub(crate) fn capture_traces<R>(operation: impl FnOnce() -> R) -> (R, CapturedTraces) {
        let _capture_guard = match CAPTURE_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let records = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Registry::default().with(CaptureLayer {
            records: Arc::clone(&records),
        });
        let result = tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            operation()
        });
        tracing::callsite::rebuild_interest_cache();

        (result, CapturedTraces { records })
    }

    pub(crate) fn trace_event<'a>(
        records: &'a [CapturedTrace],
        telemetry_event: &str,
    ) -> Result<&'a CapturedTrace, String> {
        records
            .iter()
            .find(|record| {
                record.kind() == CapturedTraceKind::Event
                    && record
                        .fields()
                        .get("telemetry_event")
                        .is_some_and(|value| value == telemetry_event)
            })
            .ok_or_else(|| format!("missing trace event {telemetry_event}: {records:#?}"))
    }

    pub(crate) fn assert_trace_field(record: &CapturedTrace, field: &str, expected: &str) {
        assert_eq!(
            record.fields().get(field).map(String::as_str),
            Some(expected),
            "trace record: {record:#?}"
        );
    }

    struct CaptureLayer {
        records: Arc<Mutex<Vec<CapturedTrace>>>,
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
            let metadata = attrs.metadata();
            let mut visitor = FieldVisitor::default();
            attrs.record(&mut visitor);
            self.push(CapturedTrace {
                kind: CapturedTraceKind::Span,
                name: metadata.name().to_owned(),
                target: metadata.target().to_owned(),
                level: *metadata.level(),
                span_name: None,
                fields: visitor.fields,
            });
        }

        fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
            let metadata = event.metadata();
            let mut visitor = FieldVisitor::default();
            event.record(&mut visitor);
            let span_name = ctx.event_scope(event).and_then(|scope| {
                scope
                    .from_root()
                    .last()
                    .map(|span| span.metadata().name().to_owned())
            });

            self.push(CapturedTrace {
                kind: CapturedTraceKind::Event,
                name: metadata.name().to_owned(),
                target: metadata.target().to_owned(),
                level: *metadata.level(),
                span_name,
                fields: visitor.fields,
            });
        }
    }

    impl CaptureLayer {
        fn push(&self, record: CapturedTrace) {
            if let Ok(mut records) = self.records.lock() {
                records.push(record);
            }
        }
    }

    #[derive(Default)]
    struct FieldVisitor {
        fields: BTreeMap<String, String>,
    }

    impl FieldVisitor {
        fn insert(&mut self, field: &Field, value: impl Into<String>) {
            self.fields.insert(field.name().to_owned(), value.into());
        }
    }

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.insert(field, format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.insert(field, value);
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.insert(field, value.to_string());
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.insert(field, value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.insert(field, value.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing::Level;

    use super::{
        TEST_CAPTURE_EVENT, TEST_CAPTURE_SPAN, TRACE_TARGET, emit_test_capture_smoke_event,
        test_support::{CapturedTraceKind, capture_traces},
    };

    #[test]
    fn scoped_capture_records_event_and_fields() -> Result<(), String> {
        let (_result, traces) = capture_traces(emit_test_capture_smoke_event);
        let records = traces.records()?;

        let has_span = records.iter().any(|record| {
            record.kind() == CapturedTraceKind::Span
                && record.name() == TEST_CAPTURE_SPAN
                && record.target() == TRACE_TARGET
                && record.level() == Level::INFO
                && record
                    .fields()
                    .get("phase")
                    .is_some_and(|value| value == "test_capture")
        });
        assert!(has_span, "captured records: {records:#?}");

        let has_event = records.iter().any(|record| {
            record.kind() == CapturedTraceKind::Event
                && record.target() == TRACE_TARGET
                && record.level() == Level::INFO
                && record.span_name() == Some(TEST_CAPTURE_SPAN)
                && record
                    .fields()
                    .get("message")
                    .is_some_and(|value| value.contains(TEST_CAPTURE_EVENT))
                && record
                    .fields()
                    .get("smoke_count")
                    .is_some_and(|value| value == "1")
                && record
                    .fields()
                    .get("smoke_label")
                    .is_some_and(|value| value == "foundation")
        });
        assert!(has_event, "captured records: {records:#?}");

        Ok(())
    }

    #[test]
    fn smoke_event_runs_without_subscriber() {
        emit_test_capture_smoke_event();
    }

    #[test]
    fn capture_helper_detects_forbidden_text() -> Result<(), String> {
        let (_result, traces) = capture_traces(|| {
            tracing::info!(
                target: TRACE_TARGET,
                safe_field = "credential-free",
                secret_like = "password=secret",
                "test forbidden scan"
            );
        });

        assert!(traces.contains_text("password=secret")?);
        traces.assert_no_forbidden_text(&["server=tcp:sql.example.com"])?;
        assert!(
            traces
                .assert_no_forbidden_text(&["password=secret"])
                .is_err()
        );

        Ok(())
    }
}
