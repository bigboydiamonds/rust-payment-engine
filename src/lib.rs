pub mod account;
pub mod engine;
pub mod error;
pub mod transaction;

/// Create a CSV ReaderBuilder with the standard configuration used by the
/// engine: trim all whitespace around fields and headers, and tolerate rows
/// with fewer columns (dispute/resolve/chargeback omit the amount field).
///
/// Both the CLI entry point and integration tests use this to ensure
/// consistent CSV parsing behavior.
pub fn csv_reader_builder() -> csv::ReaderBuilder {
    let mut builder = csv::ReaderBuilder::new();
    builder.trim(csv::Trim::All).flexible(true);
    builder
}
