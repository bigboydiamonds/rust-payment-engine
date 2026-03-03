use std::collections::HashMap;
use std::path::Path;

use csv::{ReaderBuilder, Trim, WriterBuilder};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use payment_engine::csv_reader_builder;
use payment_engine::engine::Engine;
use payment_engine::transaction::{RawTransaction, Transaction};

/// Run the engine against a CSV input string and return parsed output rows.
fn run_engine(input: &str) -> Vec<OutputRow> {
    let mut reader = csv_reader_builder().from_reader(input.as_bytes());

    let mut engine = Engine::new();

    for result in reader.deserialize::<RawTransaction>() {
        let raw = match result {
            Ok(r) => r,
            Err(_) => continue,
        };
        let tx = match Transaction::try_from(raw) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let _ = engine.process(tx);
    }

    let mut output = Vec::new();
    {
        let mut writer = WriterBuilder::new().from_writer(&mut output);
        for account in engine.into_accounts() {
            writer.serialize(&account).unwrap();
        }
        writer.flush().unwrap();
    }

    let mut out_reader = ReaderBuilder::new()
        .trim(Trim::All)
        .from_reader(output.as_slice());

    out_reader
        .deserialize::<OutputRow>()
        .map(|r| r.unwrap())
        .collect()
}

/// Helper to index output rows by client ID.
fn by_client(rows: &[OutputRow]) -> HashMap<u16, &OutputRow> {
    rows.iter().map(|r| (r.client, r)).collect()
}

#[derive(Debug, serde::Deserialize)]
struct OutputRow {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

// ─── Test Cases ─────────────────────────────────────────────────────────────

#[test]
fn basic_deposits_and_withdrawal() {
    let input = "\
type, client, tx, amount
deposit, 1, 1, 1.0
deposit, 2, 2, 2.0
deposit, 1, 3, 2.0
withdrawal, 1, 4, 1.5
withdrawal, 2, 5, 3.0
";
    let rows = run_engine(input);
    let map = by_client(&rows);

    // Client 1: deposited 1.0 + 2.0 = 3.0, withdrew 1.5 => 1.5
    let c1 = map[&1];
    assert_eq!(c1.available, dec!(1.5));
    assert_eq!(c1.held, dec!(0.0));
    assert_eq!(c1.total, dec!(1.5));
    assert!(!c1.locked);

    // Client 2: deposited 2.0, tried to withdraw 3.0 (fails) => 2.0
    let c2 = map[&2];
    assert_eq!(c2.available, dec!(2.0));
    assert_eq!(c2.held, dec!(0.0));
    assert_eq!(c2.total, dec!(2.0));
    assert!(!c2.locked);
}

#[test]
fn dispute_and_resolve_cycle() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
dispute,1,1,
resolve,1,1,
";
    let rows = run_engine(input);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].available, dec!(10.0));
    assert_eq!(rows[0].held, dec!(0.0));
    assert_eq!(rows[0].total, dec!(10.0));
    assert!(!rows[0].locked);
}

#[test]
fn dispute_and_chargeback_freezes_account() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
dispute,1,1,
chargeback,1,1,
";
    let rows = run_engine(input);
    assert_eq!(rows[0].available, dec!(0.0));
    assert_eq!(rows[0].held, dec!(0.0));
    assert_eq!(rows[0].total, dec!(0.0));
    assert!(rows[0].locked);
}

#[test]
fn frozen_account_rejects_further_transactions() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
dispute,1,1,
chargeback,1,1,
deposit,1,2,5.0
withdrawal,1,3,1.0
";
    let rows = run_engine(input);
    // Account should remain at 0 despite attempted deposit and withdrawal
    assert_eq!(rows[0].available, dec!(0.0));
    assert_eq!(rows[0].total, dec!(0.0));
    assert!(rows[0].locked);
}

#[test]
fn dispute_nonexistent_tx_ignored() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
dispute,1,999,
";
    let rows = run_engine(input);
    assert_eq!(rows[0].available, dec!(10.0));
    assert_eq!(rows[0].total, dec!(10.0));
}

#[test]
fn resolve_without_dispute_ignored() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
resolve,1,1,
";
    let rows = run_engine(input);
    assert_eq!(rows[0].available, dec!(10.0));
}

#[test]
fn chargeback_without_dispute_ignored() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
chargeback,1,1,
";
    let rows = run_engine(input);
    assert_eq!(rows[0].available, dec!(10.0));
    assert!(!rows[0].locked);
}

#[test]
fn multiple_clients_interleaved() {
    let input = "\
type,client,tx,amount
deposit,2,1,20.0
deposit,1,2,10.0
withdrawal,2,3,5.0
deposit,1,4,5.0
withdrawal,1,5,3.0
";
    let rows = run_engine(input);
    let map = by_client(&rows);

    assert_eq!(map[&1].available, dec!(12.0));
    assert_eq!(map[&2].available, dec!(15.0));
}

#[test]
fn precision_four_decimal_places() {
    let input = "\
type,client,tx,amount
deposit,1,1,1.1111
deposit,1,2,2.2222
withdrawal,1,3,0.3333
";
    let rows = run_engine(input);
    assert_eq!(rows[0].available, dec!(3.0));
    assert_eq!(rows[0].total, dec!(3.0));
}

#[test]
fn dispute_after_partial_withdrawal() {
    let input = "\
type,client,tx,amount
deposit,1,1,100.0
withdrawal,1,2,60.0
dispute,1,1,
";
    let rows = run_engine(input);
    // available = 40 - 100 (held) = -60
    assert_eq!(rows[0].available, dec!(-60.0));
    assert_eq!(rows[0].held, dec!(100.0));
    assert_eq!(rows[0].total, dec!(40.0));
}

#[test]
fn chargeback_after_partial_withdrawal() {
    let input = "\
type,client,tx,amount
deposit,1,1,100.0
withdrawal,1,2,60.0
dispute,1,1,
chargeback,1,1,
";
    let rows = run_engine(input);
    // After chargeback: held drops by 100, total drops by 100
    // available was -60, held was 100, total was 40
    // After: available = -60, held = 0, total = -60
    assert_eq!(rows[0].available, dec!(-60.0));
    assert_eq!(rows[0].held, dec!(0.0));
    assert_eq!(rows[0].total, dec!(-60.0));
    assert!(rows[0].locked);
}

#[test]
fn whitespace_in_csv_handled() {
    let input = "\
type ,  client ,  tx , amount
  deposit , 1 , 1 , 5.0
  withdrawal , 1 , 2 , 2.0
";
    let rows = run_engine(input);
    assert_eq!(rows[0].available, dec!(3.0));
}

#[test]
fn duplicate_deposit_tx_id_rejected() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
deposit,1,1,5.0
";
    let rows = run_engine(input);
    // Second deposit rejected — only the first 10.0 counts
    assert_eq!(rows[0].available, dec!(10.0));
}

#[test]
fn dispute_wrong_client_ignored() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
dispute,2,1,
";
    let rows = run_engine(input);
    let map = by_client(&rows);
    // Client 1's deposit should not be affected by client 2's dispute
    assert_eq!(map[&1].available, dec!(10.0));
    assert_eq!(map[&1].held, dec!(0.0));
}

#[test]
fn re_dispute_after_resolve() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
dispute,1,1,
resolve,1,1,
dispute,1,1,
chargeback,1,1,
";
    let rows = run_engine(input);
    assert_eq!(rows[0].available, dec!(0.0));
    assert_eq!(rows[0].held, dec!(0.0));
    assert_eq!(rows[0].total, dec!(0.0));
    assert!(rows[0].locked);
}

#[test]
fn empty_file_produces_header_only() {
    let input = "type,client,tx,amount\n";
    let rows = run_engine(input);
    assert!(rows.is_empty());
}

#[test]
fn deposit_tx_id_collision_with_withdrawal() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
withdrawal,1,2,3.0
deposit,1,2,5.0
";
    let rows = run_engine(input);
    // The second deposit reuses TX ID 2 (already used by the withdrawal) and
    // should be rejected. Client 1: deposited 10, withdrew 3 => 7.0
    assert_eq!(rows[0].available, dec!(7.0));
    assert_eq!(rows[0].total, dec!(7.0));
}

#[test]
fn amount_with_more_than_four_decimal_places() {
    let input = "\
type,client,tx,amount
deposit,1,1,1.123456
";
    let rows = run_engine(input);
    // Higher-precision values are accepted and stored at full precision internally.
    // Output serialization rounds to 4 decimal places via round_dp(4).
    // 1.123456 rounded to 4dp => 1.1235 (6 > 5, rounds up unambiguously).
    assert_eq!(rows[0].available, dec!(1.1235));
    assert_eq!(rows[0].total, dec!(1.1235));
}

#[test]
fn dispute_on_frozen_account_deposit() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
deposit,1,2,20.0
dispute,1,2,
chargeback,1,2,
dispute,1,1,
";
    let rows = run_engine(input);
    // After deposits: available = 30, held = 0, total = 30.
    // After dispute TX 2: available = 10, held = 20, total = 30.
    // After chargeback TX 2: available = 10, held = 0, total = 10, locked.
    // Dispute on TX 1 still proceeds — the spec freezes deposits/withdrawals, not disputes.
    // After dispute TX 1: available = 0, held = 10, total = 10.
    assert_eq!(rows[0].available, dec!(0.0));
    assert_eq!(rows[0].held, dec!(10.0));
    assert_eq!(rows[0].total, dec!(10.0));
    assert!(rows[0].locked);
}

#[test]
fn multiple_disputes_different_deposits_same_client() {
    let input = "\
type,client,tx,amount
deposit,1,1,10.0
deposit,1,2,20.0
dispute,1,1,
dispute,1,2,
resolve,1,1,
chargeback,1,2,
";
    let rows = run_engine(input);
    // After deposits: available = 30, held = 0, total = 30.
    // After dispute TX 1: available = 20, held = 10, total = 30.
    // After dispute TX 2: available = 0, held = 30, total = 30.
    // After resolve TX 1: available = 10, held = 20, total = 30.
    // After chargeback TX 2: available = 10, held = 0, total = 10, locked.
    assert_eq!(rows[0].available, dec!(10.0));
    assert_eq!(rows[0].held, dec!(0.0));
    assert_eq!(rows[0].total, dec!(10.0));
    assert!(rows[0].locked);
}

#[test]
fn maximum_precision_arithmetic() {
    let input = "\
type,client,tx,amount
deposit,1,1,9999.9999
deposit,1,2,0.0001
";
    let rows = run_engine(input);
    // 9999.9999 + 0.0001 = 10000.0000 — no precision loss at the boundary.
    assert_eq!(rows[0].available, dec!(10000));
    assert_eq!(rows[0].total, dec!(10000));
}

// ─── Fixture file tests ─────────────────────────────────────────────────────

/// Run the engine against a fixture CSV file on disk and return parsed output rows.
fn run_engine_from_file(path: &Path) -> Vec<OutputRow> {
    let mut reader = csv_reader_builder().from_path(path).unwrap();

    let mut engine = Engine::new();

    for result in reader.deserialize::<RawTransaction>() {
        let raw = match result {
            Ok(r) => r,
            Err(_) => continue,
        };
        let tx = match Transaction::try_from(raw) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let _ = engine.process(tx);
    }

    let mut output = Vec::new();
    {
        let mut writer = WriterBuilder::new().from_writer(&mut output);
        for account in engine.into_accounts() {
            writer.serialize(&account).unwrap();
        }
        writer.flush().unwrap();
    }

    let mut out_reader = ReaderBuilder::new()
        .trim(Trim::All)
        .from_reader(output.as_slice());

    out_reader
        .deserialize::<OutputRow>()
        .map(|r| r.unwrap())
        .collect()
}

/// Parse an expected-output fixture CSV into OutputRows for comparison.
fn load_expected(path: &Path) -> Vec<OutputRow> {
    let mut reader = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(path)
        .unwrap();
    reader
        .deserialize::<OutputRow>()
        .map(|r| r.unwrap())
        .collect()
}

fn assert_rows_match(actual: &[OutputRow], expected: &[OutputRow]) {
    let actual_map = by_client(actual);
    let expected_map = by_client(expected);
    assert_eq!(
        actual_map.len(),
        expected_map.len(),
        "client count mismatch: got {}, expected {}",
        actual_map.len(),
        expected_map.len()
    );
    for (client, exp) in &expected_map {
        let act = actual_map
            .get(client)
            .unwrap_or_else(|| panic!("missing client {client} in output"));
        assert_eq!(act.available, exp.available, "client {client} available");
        assert_eq!(act.held, exp.held, "client {client} held");
        assert_eq!(act.total, exp.total, "client {client} total");
        assert_eq!(act.locked, exp.locked, "client {client} locked");
    }
}

#[test]
fn fixture_sample_input() {
    let actual = run_engine_from_file(Path::new("fixtures/sample_input.csv"));
    let expected = load_expected(Path::new("fixtures/sample_output.csv"));
    assert_rows_match(&actual, &expected);
}

#[test]
fn fixture_disputes() {
    let actual = run_engine_from_file(Path::new("fixtures/disputes.csv"));
    let expected = load_expected(Path::new("fixtures/disputes_output.csv"));
    assert_rows_match(&actual, &expected);
}
