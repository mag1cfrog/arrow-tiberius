//! Configure writer backend selection without opening a SQL Server connection.

use arrow_tiberius::{WriteBackend, WriteOptions};

fn main() {
    let auto = WriteOptions::default();
    let direct_raw = WriteOptions {
        backend: WriteBackend::DirectRawBulk,
        ..WriteOptions::default()
    };
    let baseline = WriteOptions {
        backend: WriteBackend::BaselineTokenRow,
        ..WriteOptions::default()
    };

    // Backend execution still validates the planned mappings and target table
    // metadata before rows are sent.
    print_backend("auto", auto);
    print_backend("direct raw", direct_raw);
    print_backend("baseline", baseline);
}

fn print_backend(label: &str, options: WriteOptions) {
    println!("{label}: {:?}", options.backend);
}
