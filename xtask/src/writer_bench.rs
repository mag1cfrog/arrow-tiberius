use std::ffi::OsString;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

pub(super) fn run(args: &[OsString]) -> Result<(), WriterBenchError> {
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_help();
        return Ok(());
    }

    let options = WriterBenchOptions::parse(args)?;
    let summary = summarize_generated_batches(&options)?;
    print_summary(&options, &summary);
    Ok(())
}

fn print_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench [OPTIONS]\n\nOptions:\n  --rows <COUNT>          Total rows to generate [default: 100000]\n  --batch-size <COUNT>    Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>       Benchmark scenario [default: narrow_numeric]\n  --repeat <COUNT>        Number of benchmark repeats [default: 1]\n  --output <FORMAT>       Output format: human [default: human]\n  -h, --help              Print help\n\nScenarios:"
    );
    for scenario in SCENARIOS {
        println!("  {:<16}  {}", scenario.name, scenario.description);
    }
}

fn print_summary(options: &WriterBenchOptions, summary: &GeneratedBatchSummary) {
    println!("writer-bench");
    println!("  rows: {}", options.rows);
    println!("  batch size: {}", options.batch_size);
    println!("  scenario: {}", options.scenario);
    println!("  repeat: {}", options.repeat);
    println!("  output: {}", options.output);
    println!("  batches: {}", summary.batches);
    println!("  generated rows: {}", summary.rows);
}

#[derive(Debug, Clone)]
struct WriterBenchOptions {
    rows: usize,
    batch_size: usize,
    scenario: &'static BenchmarkScenarioDefinition,
    repeat: usize,
    output: BenchmarkOutput,
}

impl Default for WriterBenchOptions {
    fn default() -> Self {
        Self {
            rows: 100_000,
            batch_size: 8_192,
            scenario: scenario_by_name("narrow_numeric").unwrap_or(&NARROW_NUMERIC_SCENARIO),
            repeat: 1,
            output: BenchmarkOutput::Human,
        }
    }
}

impl WriterBenchOptions {
    fn parse(args: &[OsString]) -> Result<Self, WriterBenchError> {
        let mut options = Self::default();
        let mut index = 0;

        while index < args.len() {
            let arg = args[index]
                .to_str()
                .ok_or_else(|| WriterBenchError::InvalidUtf8Argument(args[index].clone()))?;

            match arg {
                "-h" | "--help" => {
                    print_help();
                    return Ok(options);
                }
                "--rows" => {
                    options.rows = parse_positive_usize("--rows", &required_value(args, index)?)?;
                    index += 1;
                }
                "--batch-size" => {
                    options.batch_size =
                        parse_positive_usize("--batch-size", &required_value(args, index)?)?;
                    index += 1;
                }
                "--scenario" => {
                    options.scenario = parse_scenario(&required_value(args, index)?)?;
                    index += 1;
                }
                "--repeat" => {
                    options.repeat =
                        parse_positive_usize("--repeat", &required_value(args, index)?)?;
                    index += 1;
                }
                "--output" => {
                    options.output = required_value(args, index)?.parse()?;
                    index += 1;
                }
                other => return Err(WriterBenchError::UnknownOption(other.to_owned())),
            }

            index += 1;
        }

        Ok(options)
    }
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkScenarioDefinition {
    name: &'static str,
    description: &'static str,
    schema: fn() -> SchemaRef,
    columns: fn(offset: usize, len: usize) -> Vec<ArrayRef>,
}

impl fmt::Display for BenchmarkScenarioDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

const NARROW_NUMERIC_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "narrow_numeric",
    description: "Primitive numeric throughput",
    schema: narrow_numeric_schema,
    columns: narrow_numeric_columns,
};

const MIXED_NULLABLE_SCENARIO: BenchmarkScenarioDefinition = BenchmarkScenarioDefinition {
    name: "mixed_nullable",
    description: "Nullable primitives and short strings",
    schema: mixed_nullable_schema,
    columns: mixed_nullable_columns,
};

const SCENARIOS: &[BenchmarkScenarioDefinition] =
    &[NARROW_NUMERIC_SCENARIO, MIXED_NULLABLE_SCENARIO];

fn scenario_by_name(name: &str) -> Option<&'static BenchmarkScenarioDefinition> {
    SCENARIOS.iter().find(|scenario| scenario.name == name)
}

fn parse_scenario(value: &str) -> Result<&'static BenchmarkScenarioDefinition, WriterBenchError> {
    scenario_by_name(value).ok_or_else(|| WriterBenchError::InvalidScenario(value.to_owned()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkOutput {
    Human,
}

impl fmt::Display for BenchmarkOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Human => f.write_str("human"),
        }
    }
}

impl FromStr for BenchmarkOutput {
    type Err = WriterBenchError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "human" => Ok(Self::Human),
            other => Err(WriterBenchError::InvalidOutput(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone)]
struct GeneratedBatchReader {
    scenario: &'static BenchmarkScenarioDefinition,
    schema: SchemaRef,
    rows: usize,
    batch_size: usize,
    next_offset: usize,
}

impl GeneratedBatchReader {
    fn new(options: &WriterBenchOptions) -> Self {
        Self {
            scenario: options.scenario,
            schema: (options.scenario.schema)(),
            rows: options.rows,
            batch_size: options.batch_size,
            next_offset: 0,
        }
    }
}

impl Iterator for GeneratedBatchReader {
    type Item = Result<RecordBatch, WriterBenchError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_offset == self.rows {
            return None;
        }

        let offset = self.next_offset;
        let len = self.batch_size.min(self.rows - offset);
        self.next_offset += len;

        Some(generate_batch(
            self.scenario,
            self.schema.clone(),
            offset,
            len,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GeneratedBatchSummary {
    batches: usize,
    rows: usize,
}

fn summarize_generated_batches(
    options: &WriterBenchOptions,
) -> Result<GeneratedBatchSummary, WriterBenchError> {
    let mut summary = GeneratedBatchSummary {
        batches: 0,
        rows: 0,
    };

    for batch in GeneratedBatchReader::new(options) {
        let batch = batch?;
        summary.batches += 1;
        summary.rows += batch.num_rows();
    }

    Ok(summary)
}

fn generate_batch(
    scenario: &BenchmarkScenarioDefinition,
    schema: SchemaRef,
    offset: usize,
    len: usize,
) -> Result<RecordBatch, WriterBenchError> {
    let columns = (scenario.columns)(offset, len);

    RecordBatch::try_new(schema, columns).map_err(WriterBenchError::Arrow)
}

fn narrow_numeric_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id32", DataType::Int32, false),
        Field::new("id64", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]))
}

fn narrow_numeric_columns(offset: usize, len: usize) -> Vec<ArrayRef> {
    let id32 = (offset..offset + len)
        .map(deterministic_i32)
        .collect::<Int32Array>();
    let id64 = (offset..offset + len)
        .map(|row| i64::from(deterministic_i32(row)) * 1_000)
        .collect::<Int64Array>();
    let score = (offset..offset + len)
        .map(deterministic_score)
        .collect::<Float64Array>();

    vec![Arc::new(id32), Arc::new(id64), Arc::new(score)]
}

fn mixed_nullable_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id32", DataType::Int32, false),
        Field::new("maybe_id64", DataType::Int64, true),
        Field::new("maybe_score", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn mixed_nullable_columns(offset: usize, len: usize) -> Vec<ArrayRef> {
    let id32 = (offset..offset + len)
        .map(deterministic_i32)
        .collect::<Int32Array>();
    let maybe_id64 = (offset..offset + len)
        .map(|row| {
            if row % 7 == 0 {
                None
            } else {
                Some(i64::from(deterministic_i32(row)) * 10)
            }
        })
        .collect::<Int64Array>();
    let maybe_score = (offset..offset + len)
        .map(|row| {
            if row % 11 == 0 {
                None
            } else {
                Some(deterministic_score(row))
            }
        })
        .collect::<Float64Array>();
    let category_values = ["alpha", "beta", "gamma", "delta", "epsilon"];
    let category = (offset..offset + len)
        .map(|row| {
            if row % 5 == 0 {
                None
            } else {
                Some(category_values[row % category_values.len()])
            }
        })
        .collect::<StringArray>();

    vec![
        Arc::new(id32),
        Arc::new(maybe_id64),
        Arc::new(maybe_score),
        Arc::new(category),
    ]
}

fn deterministic_i32(row: usize) -> i32 {
    let mixed = row.wrapping_mul(1_103_515_245).wrapping_add(12_345);
    (mixed % 1_000_000) as i32
}

fn deterministic_score(row: usize) -> f64 {
    let bucket = row.wrapping_mul(37).wrapping_add(17) % 100_000;
    bucket as f64 / 100.0
}

fn parse_positive_usize(option: &'static str, value: &str) -> Result<usize, WriterBenchError> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| WriterBenchError::InvalidPositiveInteger {
            option,
            value: value.to_owned(),
        })?;

    if parsed == 0 {
        return Err(WriterBenchError::InvalidPositiveInteger {
            option,
            value: value.to_owned(),
        });
    }

    Ok(parsed)
}

fn required_value(args: &[OsString], index: usize) -> Result<String, WriterBenchError> {
    let value = args
        .get(index + 1)
        .ok_or_else(|| WriterBenchError::MissingOptionValue(option_name(args, index)))?;

    value
        .to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| WriterBenchError::InvalidUtf8Argument(value.clone()))
}

fn option_name(args: &[OsString], index: usize) -> String {
    args.get(index)
        .and_then(|arg| arg.to_str())
        .unwrap_or("<unknown>")
        .to_owned()
}

#[derive(Debug)]
pub(super) enum WriterBenchError {
    UnknownOption(String),
    MissingOptionValue(String),
    InvalidUtf8Argument(OsString),
    InvalidPositiveInteger { option: &'static str, value: String },
    InvalidScenario(String),
    InvalidOutput(String),
    Arrow(arrow_schema::ArrowError),
}

impl fmt::Display for WriterBenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownOption(option) => write!(f, "unknown writer-bench option `{option}`"),
            Self::MissingOptionValue(option) => write!(f, "missing value for `{option}`"),
            Self::InvalidUtf8Argument(arg) => write!(f, "argument is not valid UTF-8: {arg:?}"),
            Self::InvalidPositiveInteger { option, value } => {
                write!(f, "{option} must be a positive integer, got `{value}`")
            }
            Self::InvalidScenario(value) => {
                write!(
                    f,
                    "unknown writer-bench scenario `{value}`; expected one of: {}",
                    scenario_names()
                )
            }
            Self::InvalidOutput(value) => {
                write!(f, "unknown writer-bench output `{value}`; expected human")
            }
            Self::Arrow(source) => write!(f, "failed to generate Arrow benchmark data: {source}"),
        }
    }
}

fn scenario_names() -> String {
    SCENARIOS
        .iter()
        .map(|scenario| scenario.name)
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::{BenchmarkOutput, WriterBenchError, WriterBenchOptions};
    use arrow_array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::DataType;
    use std::ffi::OsString;

    #[test]
    fn parses_writer_bench_defaults() {
        let options = WriterBenchOptions::parse(&[]).unwrap();

        assert_eq!(options.rows, 100_000);
        assert_eq!(options.batch_size, 8_192);
        assert_eq!(options.scenario.name, "narrow_numeric");
        assert_eq!(options.repeat, 1);
        assert_eq!(options.output, BenchmarkOutput::Human);
    }

    #[test]
    fn parses_writer_bench_options() {
        let args = [
            OsString::from("--rows"),
            OsString::from("17"),
            OsString::from("--batch-size"),
            OsString::from("4"),
            OsString::from("--scenario"),
            OsString::from("mixed_nullable"),
            OsString::from("--repeat"),
            OsString::from("3"),
            OsString::from("--output"),
            OsString::from("human"),
        ];

        let options = WriterBenchOptions::parse(&args).unwrap();

        assert_eq!(options.rows, 17);
        assert_eq!(options.batch_size, 4);
        assert_eq!(options.scenario.name, "mixed_nullable");
        assert_eq!(options.repeat, 3);
        assert_eq!(options.output, BenchmarkOutput::Human);
    }

    #[test]
    fn rejects_zero_rows() {
        let args = [OsString::from("--rows"), OsString::from("0")];
        let err = WriterBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::InvalidPositiveInteger {
                option: "--rows",
                value
            } if value == "0"
        ));
    }

    #[test]
    fn rejects_invalid_batch_size() {
        let args = [OsString::from("--batch-size"), OsString::from("nope")];
        let err = WriterBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(
            err,
            WriterBenchError::InvalidPositiveInteger {
                option: "--batch-size",
                value
            } if value == "nope"
        ));
    }

    #[test]
    fn rejects_unknown_scenario() {
        let args = [OsString::from("--scenario"), OsString::from("tpch")];
        let err = WriterBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(err, WriterBenchError::InvalidScenario(value) if value == "tpch"));
    }

    #[test]
    fn rejects_unknown_output() {
        let args = [OsString::from("--output"), OsString::from("json")];
        let err = WriterBenchOptions::parse(&args).unwrap_err();

        assert!(matches!(err, WriterBenchError::InvalidOutput(value) if value == "json"));
    }

    #[test]
    fn generates_requested_row_count_across_batches() {
        let options = WriterBenchOptions {
            rows: 10,
            batch_size: 4,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 4);
        assert_eq!(batches[1].num_rows(), 4);
        assert_eq!(batches[2].num_rows(), 2);
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            10
        );
    }

    #[test]
    fn narrow_numeric_schema_matches_definition() {
        let options = WriterBenchOptions {
            rows: 1,
            batch_size: 1,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let schema = batches[0].schema();

        assert_eq!(schema.field(0).name(), "id32");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "id64");
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert!(!schema.field(1).is_nullable());
        assert_eq!(schema.field(2).name(), "score");
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert!(!schema.field(2).is_nullable());
    }

    #[test]
    fn mixed_nullable_schema_matches_definition() {
        let options = WriterBenchOptions {
            rows: 1,
            batch_size: 1,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let schema = batches[0].schema();

        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert!(schema.field(2).is_nullable());
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert!(schema.field(3).is_nullable());
    }

    #[test]
    fn nullable_scenario_contains_nulls() {
        let options = WriterBenchOptions {
            rows: 32,
            batch_size: 32,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];

        assert!(batch.column(1).null_count() > 0);
        assert!(batch.column(2).null_count() > 0);
        assert!(batch.column(3).null_count() > 0);
    }

    #[test]
    fn generated_values_are_deterministic() {
        let options = WriterBenchOptions {
            rows: 17,
            batch_size: 5,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let first = generated_batches(&options);
        let second = generated_batches(&options);

        assert_eq!(first, second);
    }

    #[test]
    fn generated_values_continue_across_batch_boundaries() {
        let options = WriterBenchOptions {
            rows: 6,
            batch_size: 4,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let first_batch_id32 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let second_batch_id32 = batches[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(first_batch_id32.value(3), super::deterministic_i32(3));
        assert_eq!(second_batch_id32.value(0), super::deterministic_i32(4));
        assert_ne!(first_batch_id32.value(3), second_batch_id32.value(0));
    }

    #[test]
    fn generated_columns_have_expected_runtime_array_types() {
        let options = WriterBenchOptions {
            rows: 3,
            batch_size: 3,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = generated_batches(&options);
        let batch = &batches[0];

        assert!(batch.column(0).as_any().is::<Int32Array>());
        assert!(batch.column(1).as_any().is::<Int64Array>());
        assert!(batch.column(2).as_any().is::<Float64Array>());
        assert!(batch.column(3).as_any().is::<StringArray>());
    }

    #[test]
    fn lazy_reader_emits_no_empty_batch_for_exact_multiple() {
        let options = WriterBenchOptions {
            rows: 8,
            batch_size: 4,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = super::GeneratedBatchReader::new(&options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(batches.len(), 2);
        assert!(batches.iter().all(|batch| batch.num_rows() == 4));
    }

    #[test]
    fn lazy_reader_emits_partial_tail_batch() {
        let options = WriterBenchOptions {
            rows: 9,
            batch_size: 4,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let batches = super::GeneratedBatchReader::new(&options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 4);
        assert_eq!(batches[1].num_rows(), 4);
        assert_eq!(batches[2].num_rows(), 1);
    }

    #[test]
    fn lazy_readers_are_repeatable_without_shared_state() {
        let options = WriterBenchOptions {
            rows: 17,
            batch_size: 6,
            scenario: super::scenario_by_name("mixed_nullable").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let first = super::GeneratedBatchReader::new(&options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let second = super::GeneratedBatchReader::new(&options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(first, second);
    }

    #[test]
    fn lazy_reader_summarizes_large_volume_without_collecting_batches() {
        let options = WriterBenchOptions {
            rows: 1_000_001,
            batch_size: 8_192,
            scenario: super::scenario_by_name("narrow_numeric").unwrap(),
            repeat: 1,
            output: BenchmarkOutput::Human,
        };

        let summary = super::summarize_generated_batches(&options).unwrap();

        assert_eq!(summary.rows, 1_000_001);
        assert_eq!(summary.batches, 123);
    }

    #[test]
    fn scenario_registry_lists_supported_scenarios() {
        let names = super::SCENARIOS
            .iter()
            .map(|scenario| scenario.name)
            .collect::<Vec<_>>();

        assert_eq!(names, ["narrow_numeric", "mixed_nullable"]);
        assert!(
            super::SCENARIOS
                .iter()
                .all(|scenario| !scenario.description.is_empty())
        );
    }

    fn generated_batches(options: &WriterBenchOptions) -> Vec<RecordBatch> {
        super::GeneratedBatchReader::new(options)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }
}
