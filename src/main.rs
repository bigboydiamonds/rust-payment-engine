use std::env;
use std::io;
use std::process;

use csv::WriterBuilder;

use payment_engine::csv_reader_builder;
use payment_engine::engine::Engine;
use payment_engine::transaction::{RawTransaction, Transaction};

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args().nth(1).ok_or(
        "Usage: payment_engine <transactions.csv>\n\
         Processes a CSV of transactions and outputs client account balances to stdout.",
    )?;

    let mut reader = csv_reader_builder().from_path(&path)?;

    let mut engine = Engine::new();

    for (line_idx, result) in reader.deserialize::<RawTransaction>().enumerate() {
        let line_num = line_idx + 2; // +1 for 0-index, +1 for header row

        let raw = match result {
            Ok(raw) => raw,
            Err(e) => {
                eprintln!("warn: line {line_num}: csv parse error: {e}");
                continue;
            }
        };

        let tx = match Transaction::try_from(raw) {
            Ok(tx) => tx,
            Err(e) => {
                eprintln!("warn: line {line_num}: {e}");
                continue;
            }
        };

        if let Err(e) = engine.process(tx) {
            eprintln!("warn: line {line_num}: {e}");
        }
    }

    // Acquire stdout lock once for the entire write, avoiding per-record lock overhead.
    let stdout = io::stdout().lock();
    let mut writer = WriterBuilder::new().from_writer(stdout);

    for account in engine.into_accounts() {
        writer.serialize(&account)?;
    }
    writer.flush()?;

    Ok(())
}
