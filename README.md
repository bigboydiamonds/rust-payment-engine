# Payment Engine

A payment engine that reads a CSV of transactions (deposits, withdrawals, disputes, resolves, chargebacks) and outputs final account balances. Built as a streaming pipeline so it handles large files without issues.

```bash
cargo run -- transactions.csv > accounts.csv
```

Input CSV is the only argument, output goes to stdout.

## How it works

Transactions are processed one at a time off the CSV reader. Only deposits get stored in memory (needed later if they're disputed) — withdrawals don't need to be retained. Accounts are keyed by client ID in a HashMap.

The dispute flow is a simple state machine: `Clean → Disputed → Resolved` or `Clean → Disputed → ChargedBack`. A chargeback freezes the account permanently.

### Project layout

```
src/
  main.rs          - CLI entry, wires up CSV reader → engine → CSV writer
  lib.rs           - Module exports, shared CSV reader config
  engine.rs        - Transaction processing + dispute state machine
  transaction.rs   - Transaction types and CSV deserialization
  account.rs       - Client account balances
  error.rs         - Error types (thiserror)
```

## Design choices

**Two-phase parsing** — CSV rows first deser into a flat `RawTransaction`, then get validated and converted into a proper `Transaction` enum via `TryFrom`. I went this route because serde's enum representations don't play nice with CSV's flat row format. Splitting it also makes each layer easy to test on its own.

**Flexible precision** — Inputs can have more than 4 decimal places. I store them at full precision internally and only round to 4 places on output. Seemed better than silently truncating on the way in.

**Errors are non-fatal** — A bad transaction (insufficient funds, invalid dispute, frozen account) gets logged to stderr and skipped. Only actual I/O errors kill the process. One bad row shouldn't halt processing of everything else.

**TX IDs are globally unique** — If a withdrawal uses TX 5, a later deposit with TX 5 gets rejected. Tracked with a simple `HashSet<u32>`.

## Testing

59 tests across three layers:

- **Unit tests** in `account.rs` (9), `engine.rs` (17), `transaction.rs` (12) — cover balance math, dispute lifecycle, frozen accounts, duplicate detection, CSV parsing edge cases, etc.
- **Integration tests** in `tests/integration.rs` (21) — full CSV-in, CSV-out pipeline tests. Multi-client scenarios, dispute cycles, whitespace handling, precision, and various edge cases.

Some interesting edge cases worth calling out:

- Disputing a deposit after part of it was withdrawn — `available` goes negative, which is correct (the full deposit is held, but some funds are already gone)
- Chargebacks can produce negative totals — the client effectively owes money back
- Resolved disputes go back to `Clean`, so the same deposit can be disputed again
- CSV whitespace is handled via `Trim::All` on the reader, and missing amount fields on dispute/resolve/chargeback rows work fine with `flexible(true)`

## Memory / performance

Memory is `O(deposits + clients + tx_ids)` — not dependent on total input size. The CSV reader streams with an 8KB buffer, stdout is locked once for the whole output pass, and I use `Cow<str>` in the decimal deserializer to avoid unnecessary heap allocs.

Charged-back deposits get removed from the map since they're terminal, which helps if the chargeback rate is high.

## Dependencies

| Crate | Why |
|-------|-----|
| `csv` | Streaming CSV I/O with serde support |
| `serde` | Derive `Serialize`/`Deserialize` |
| `rust_decimal` | Exact decimal arithmetic for money |
| `thiserror` | Cleaner error type definitions |

## Running the fixtures

```bash
cargo run -- fixtures/sample_input.csv    # basic deposits/withdrawals
cargo run -- fixtures/disputes.csv        # dispute/resolve/chargeback scenarios
```

