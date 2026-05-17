use std::fs::File;
use std::path::Path;

use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::FileWriter;

use super::{GeneratedBatchReader, GeneratedBatchSummary, WriterBenchError, WriterBenchOptions};

pub(super) fn write_ipc_dataset(
    options: &WriterBenchOptions,
    path: &Path,
) -> Result<GeneratedBatchSummary, WriterBenchError> {
    let schema = (options.scenario.schema)();
    let file = File::create(path).map_err(WriterBenchError::Io)?;
    let mut writer = FileWriter::try_new(file, schema.as_ref()).map_err(WriterBenchError::Arrow)?;
    let mut summary = GeneratedBatchSummary {
        batches: 0,
        rows: 0,
    };

    for batch in GeneratedBatchReader::new_with_schema(options, schema) {
        let batch = batch?;
        summary.batches += 1;
        summary.rows += batch.num_rows();
        writer.write(&batch).map_err(WriterBenchError::Arrow)?;
    }

    writer.finish().map_err(WriterBenchError::Arrow)?;
    Ok(summary)
}

pub(super) fn ipc_dataset_reader(path: &Path) -> Result<FileReader<File>, WriterBenchError> {
    let file = File::open(path).map_err(WriterBenchError::Io)?;

    FileReader::try_new(file, None).map_err(WriterBenchError::Arrow)
}

pub(super) fn summarize_ipc_dataset(
    path: &Path,
) -> Result<GeneratedBatchSummary, WriterBenchError> {
    let mut summary = GeneratedBatchSummary {
        batches: 0,
        rows: 0,
    };

    for batch in ipc_dataset_reader(path)? {
        let batch = batch.map_err(WriterBenchError::Arrow)?;
        summary.batches += 1;
        summary.rows += batch.num_rows();
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{ipc_dataset_reader, summarize_ipc_dataset, write_ipc_dataset};
    use crate::writer_bench::{
        BenchmarkOutput, GeneratedBatchReader, SCENARIOS, WriterBenchOptions,
    };

    static IPC_TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn ipc_dataset_round_trips_every_shared_scenario() {
        for scenario in SCENARIOS {
            let options = WriterBenchOptions {
                rows: 65,
                batch_size: 16,
                scenario,
                repeat: 1,
                output: BenchmarkOutput::Human,
            };
            let path = temp_ipc_path(scenario.name);

            let summary = write_ipc_dataset(&options, &path).unwrap();
            let actual = ipc_dataset_reader(&path)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let expected = GeneratedBatchReader::new(&options)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            assert_eq!(summary.batches, 5);
            assert_eq!(summary.rows, 65);
            assert_eq!(actual, expected, "scenario {} did not round trip", scenario);

            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn ipc_dataset_preserves_partial_tail_batch_boundaries() {
        let options = WriterBenchOptions {
            rows: 25,
            batch_size: 8,
            scenario: crate::writer_bench::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };
        let path = temp_ipc_path("partial-tail");

        write_ipc_dataset(&options, &path).unwrap();
        let replayed = summarize_ipc_dataset(&path).unwrap();
        let batches = ipc_dataset_reader(&path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let lengths = batches
            .iter()
            .map(arrow_array::RecordBatch::num_rows)
            .collect::<Vec<_>>();

        assert_eq!(replayed.batches, 4);
        assert_eq!(replayed.rows, 25);
        assert_eq!(lengths, vec![8, 8, 8, 1]);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ipc_dataset_missing_file_returns_io_error() {
        let path = temp_ipc_path("missing");
        let err = ipc_dataset_reader(&path).unwrap_err();

        assert!(matches!(err, crate::writer_bench::WriterBenchError::Io(_)));
    }

    fn temp_ipc_path(name: &str) -> PathBuf {
        let counter = IPC_TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "arrow-tiberius-{name}-{}-{counter}.arrow",
            std::process::id()
        ))
    }
}
