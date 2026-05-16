use std::ffi::OsString;
use std::fmt;
use std::str::FromStr;

pub(super) fn run(args: &[OsString]) -> Result<(), WriterBenchError> {
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        print_help();
        return Ok(());
    }

    let options = WriterBenchOptions::parse(args)?;
    print_summary(&options);
    Ok(())
}

fn print_help() {
    println!(
        "Usage:\n  cargo xtask writer-bench [OPTIONS]\n\nOptions:\n  --rows <COUNT>          Total rows to generate [default: 100000]\n  --batch-size <COUNT>    Maximum rows per generated RecordBatch [default: 8192]\n  --scenario <NAME>       Benchmark scenario: narrow_numeric, mixed_nullable [default: narrow_numeric]\n  --repeat <COUNT>        Number of benchmark repeats [default: 1]\n  --output <FORMAT>       Output format: human [default: human]\n  -h, --help              Print help"
    );
}

fn print_summary(options: &WriterBenchOptions) {
    println!("writer-bench");
    println!("  rows: {}", options.rows);
    println!("  batch size: {}", options.batch_size);
    println!("  scenario: {}", options.scenario);
    println!("  repeat: {}", options.repeat);
    println!("  output: {}", options.output);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriterBenchOptions {
    rows: usize,
    batch_size: usize,
    scenario: BenchmarkScenario,
    repeat: usize,
    output: BenchmarkOutput,
}

impl Default for WriterBenchOptions {
    fn default() -> Self {
        Self {
            rows: 100_000,
            batch_size: 8_192,
            scenario: BenchmarkScenario::NarrowNumeric,
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
                    options.scenario = required_value(args, index)?.parse()?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkScenario {
    NarrowNumeric,
    MixedNullable,
}

impl fmt::Display for BenchmarkScenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NarrowNumeric => f.write_str("narrow_numeric"),
            Self::MixedNullable => f.write_str("mixed_nullable"),
        }
    }
}

impl FromStr for BenchmarkScenario {
    type Err = WriterBenchError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "narrow_numeric" => Ok(Self::NarrowNumeric),
            "mixed_nullable" => Ok(Self::MixedNullable),
            other => Err(WriterBenchError::InvalidScenario(other.to_owned())),
        }
    }
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
                    "unknown writer-bench scenario `{value}`; expected narrow_numeric or mixed_nullable"
                )
            }
            Self::InvalidOutput(value) => {
                write!(f, "unknown writer-bench output `{value}`; expected human")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BenchmarkOutput, BenchmarkScenario, WriterBenchError, WriterBenchOptions};
    use std::ffi::OsString;

    #[test]
    fn parses_writer_bench_defaults() {
        let options = WriterBenchOptions::parse(&[]).unwrap();

        assert_eq!(options.rows, 100_000);
        assert_eq!(options.batch_size, 8_192);
        assert_eq!(options.scenario, BenchmarkScenario::NarrowNumeric);
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
        assert_eq!(options.scenario, BenchmarkScenario::MixedNullable);
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
}
