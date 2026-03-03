use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;

use crate::account::ClientAccount;
use crate::error::EngineError;
use crate::transaction::Transaction;

/// The dispute lifecycle for a stored deposit.
///
/// State transitions:
///   Clean -> Disputed  (via dispute)
///   Disputed -> Clean  (via resolve)
///   Disputed -> ChargedBack  (via chargeback, terminal — record is removed)
#[derive(Debug, Clone, PartialEq)]
enum DisputeState {
    Clean,
    Disputed,
    /// Terminal state. Currently deposit records are removed on chargeback
    /// (see `process_chargeback`), so this variant is only used if the
    /// removal is disabled for diagnostic retention.
    #[allow(dead_code)]
    ChargedBack,
}

/// Record of a deposit stored for dispute resolution.
///
/// Only deposits are stored because only deposits can be disputed — the spec
/// defines disputes as reversing a credit. Withdrawals are not stored, which
/// cuts memory usage roughly in half for balanced workloads.
#[derive(Debug, Clone)]
struct DepositRecord {
    client: u16,
    amount: Decimal,
    state: DisputeState,
}

/// Core transaction processing engine.
///
/// Processes transactions sequentially, maintaining client accounts and
/// deposit history for dispute resolution. Memory usage is O(unique_deposits)
/// for the deposit store plus O(unique_clients) for account state, regardless
/// of total transaction count.
#[derive(Default)]
pub struct Engine {
    accounts: HashMap<u16, ClientAccount>,
    deposits: HashMap<u32, DepositRecord>,
    /// All transaction IDs that have been processed (deposits + withdrawals).
    /// Used to detect duplicate TX IDs globally, since the spec states TX IDs
    /// are unique across the entire system — not per-client or per-type.
    used_tx_ids: HashSet<u32>,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
            deposits: HashMap::new(),
            used_tx_ids: HashSet::new(),
        }
    }

    /// Process a single transaction, updating account and deposit state.
    ///
    /// Returns `Ok(())` on success or `Err(EngineError)` for non-fatal
    /// business logic violations (insufficient funds, invalid dispute state, etc.).
    /// Callers should log errors and continue processing — these are not I/O failures.
    pub fn process(&mut self, tx: Transaction) -> Result<(), EngineError> {
        match tx {
            Transaction::Deposit { client, tx, amount } => self.process_deposit(client, tx, amount),
            Transaction::Withdrawal { client, tx, amount } => {
                self.process_withdrawal(client, tx, amount)
            }
            Transaction::Dispute { client, tx } => self.process_dispute(client, tx),
            Transaction::Resolve { client, tx } => self.process_resolve(client, tx),
            Transaction::Chargeback { client, tx } => self.process_chargeback(client, tx),
        }
    }

    fn process_deposit(
        &mut self,
        client: u16,
        tx: u32,
        amount: Decimal,
    ) -> Result<(), EngineError> {
        // Reject duplicate transaction IDs globally — IDs are unique across
        // all transaction types, not just deposits.
        if !self.used_tx_ids.insert(tx) {
            return Err(EngineError::DuplicateTransaction { tx });
        }

        let account = self.get_or_create_account(client);
        if account.locked {
            return Err(EngineError::AccountFrozen { client });
        }

        account.deposit(amount);
        self.deposits.insert(
            tx,
            DepositRecord {
                client,
                amount,
                state: DisputeState::Clean,
            },
        );
        Ok(())
    }

    fn process_withdrawal(
        &mut self,
        client: u16,
        tx: u32,
        amount: Decimal,
    ) -> Result<(), EngineError> {
        // Reject duplicate transaction IDs (same global check as deposits).
        if !self.used_tx_ids.insert(tx) {
            return Err(EngineError::DuplicateTransaction { tx });
        }

        let account = self.get_or_create_account(client);
        if account.locked {
            return Err(EngineError::AccountFrozen { client });
        }
        if !account.withdraw(amount) {
            return Err(EngineError::InsufficientFunds { client });
        }
        Ok(())
    }

    fn process_dispute(&mut self, client: u16, tx: u32) -> Result<(), EngineError> {
        let deposit = self
            .deposits
            .get_mut(&tx)
            .ok_or(EngineError::TransactionNotFound { tx })?;

        if deposit.client != client {
            return Err(EngineError::ClientMismatch {
                tx,
                expected: deposit.client,
                got: client,
            });
        }
        if deposit.state != DisputeState::Clean {
            return Err(EngineError::InvalidDisputeState { tx });
        }

        deposit.state = DisputeState::Disputed;
        let amount = deposit.amount;

        self.accounts
            .get_mut(&client)
            .expect("account must exist: deposit was processed")
            .hold(amount);
        Ok(())
    }

    fn process_resolve(&mut self, client: u16, tx: u32) -> Result<(), EngineError> {
        let deposit = self
            .deposits
            .get_mut(&tx)
            .ok_or(EngineError::TransactionNotFound { tx })?;

        if deposit.client != client {
            return Err(EngineError::ClientMismatch {
                tx,
                expected: deposit.client,
                got: client,
            });
        }
        if deposit.state != DisputeState::Disputed {
            return Err(EngineError::InvalidDisputeState { tx });
        }

        deposit.state = DisputeState::Clean;
        let amount = deposit.amount;

        self.accounts
            .get_mut(&client)
            .expect("account must exist: deposit was processed")
            .release(amount);
        Ok(())
    }

    fn process_chargeback(&mut self, client: u16, tx: u32) -> Result<(), EngineError> {
        let amount = {
            let deposit = self
                .deposits
                .get(&tx)
                .ok_or(EngineError::TransactionNotFound { tx })?;

            if deposit.client != client {
                return Err(EngineError::ClientMismatch {
                    tx,
                    expected: deposit.client,
                    got: client,
                });
            }
            if deposit.state != DisputeState::Disputed {
                return Err(EngineError::InvalidDisputeState { tx });
            }

            deposit.amount
        };

        // Remove the deposit record: ChargedBack is a terminal state with no
        // further transitions, so the record is never needed again. This reclaims
        // memory for workloads with many chargebacks.
        // To retain charged-back records for diagnostics, comment out the remove
        // and uncomment the state mutation below it.
        self.deposits.remove(&tx);
        // self.deposits.get_mut(&tx).unwrap().state = DisputeState::ChargedBack;

        self.accounts
            .get_mut(&client)
            .expect("account must exist: deposit was processed")
            .chargeback(amount);
        Ok(())
    }

    /// Get an existing account or create a new one for the given client.
    fn get_or_create_account(&mut self, client: u16) -> &mut ClientAccount {
        self.accounts
            .entry(client)
            .or_insert_with(|| ClientAccount::new(client))
    }

    /// Consume the engine and return all client accounts sorted by client ID
    /// for deterministic, reproducible output.
    pub fn into_accounts(self) -> Vec<ClientAccount> {
        let mut accounts: Vec<_> = self.accounts.into_values().collect();
        accounts.sort_by_key(|a| a.client);
        accounts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// Helper to assert the total == available + held invariant on all accounts.
    fn assert_all_invariants(engine: &Engine) {
        for account in engine.accounts.values() {
            assert_eq!(
                account.total,
                account.available + account.held,
                "invariant violated for client {}: total ({}) != available ({}) + held ({})",
                account.client,
                account.total,
                account.available,
                account.held
            );
        }
    }

    #[test]
    fn deposit_credits_account() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].available, dec!(10.0));
        assert_eq!(accounts[0].total, dec!(10.0));
    }

    #[test]
    fn withdrawal_debits_account() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Withdrawal {
                client: 1,
                tx: 2,
                amount: dec!(3.0),
            })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts[0].available, dec!(7.0));
        assert_eq!(accounts[0].total, dec!(7.0));
    }

    #[test]
    fn withdrawal_insufficient_funds() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(5.0),
            })
            .unwrap();

        let err = engine
            .process(Transaction::Withdrawal {
                client: 1,
                tx: 2,
                amount: dec!(10.0),
            })
            .unwrap_err();
        assert_eq!(err, EngineError::InsufficientFunds { client: 1 });
        assert_all_invariants(&engine);
    }

    #[test]
    fn dispute_holds_funds() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(10.0));
        assert_eq!(accounts[0].total, dec!(10.0));
    }

    #[test]
    fn resolve_releases_held_funds() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();
        engine
            .process(Transaction::Resolve { client: 1, tx: 1 })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts[0].available, dec!(10.0));
        assert_eq!(accounts[0].held, dec!(0.0));
        assert_eq!(accounts[0].total, dec!(10.0));
        assert!(!accounts[0].locked);
    }

    #[test]
    fn chargeback_reverses_and_freezes() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();
        engine
            .process(Transaction::Chargeback { client: 1, tx: 1 })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(0.0));
        assert_eq!(accounts[0].total, dec!(0.0));
        assert!(accounts[0].locked);
    }

    #[test]
    fn frozen_account_rejects_deposit() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();
        engine
            .process(Transaction::Chargeback { client: 1, tx: 1 })
            .unwrap();

        let err = engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 2,
                amount: dec!(5.0),
            })
            .unwrap_err();
        assert_eq!(err, EngineError::AccountFrozen { client: 1 });
    }

    #[test]
    fn frozen_account_rejects_withdrawal() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();
        engine
            .process(Transaction::Chargeback { client: 1, tx: 1 })
            .unwrap();

        let err = engine
            .process(Transaction::Withdrawal {
                client: 1,
                tx: 2,
                amount: dec!(5.0),
            })
            .unwrap_err();
        assert_eq!(err, EngineError::AccountFrozen { client: 1 });
    }

    #[test]
    fn dispute_nonexistent_tx_ignored() {
        let mut engine = Engine::new();
        let err = engine
            .process(Transaction::Dispute { client: 1, tx: 999 })
            .unwrap_err();
        assert_eq!(err, EngineError::TransactionNotFound { tx: 999 });
    }

    #[test]
    fn resolve_non_disputed_tx_ignored() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();

        let err = engine
            .process(Transaction::Resolve { client: 1, tx: 1 })
            .unwrap_err();
        assert_eq!(err, EngineError::InvalidDisputeState { tx: 1 });
    }

    #[test]
    fn chargeback_non_disputed_tx_ignored() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();

        let err = engine
            .process(Transaction::Chargeback { client: 1, tx: 1 })
            .unwrap_err();
        assert_eq!(err, EngineError::InvalidDisputeState { tx: 1 });
    }

    #[test]
    fn dispute_client_mismatch_rejected() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();

        let err = engine
            .process(Transaction::Dispute { client: 2, tx: 1 })
            .unwrap_err();
        assert_eq!(
            err,
            EngineError::ClientMismatch {
                tx: 1,
                expected: 1,
                got: 2
            }
        );
    }

    #[test]
    fn duplicate_tx_id_rejected() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();

        let err = engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(5.0),
            })
            .unwrap_err();
        assert_eq!(err, EngineError::DuplicateTransaction { tx: 1 });
    }

    #[test]
    fn multiple_clients_independent() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Deposit {
                client: 2,
                tx: 2,
                amount: dec!(20.0),
            })
            .unwrap();
        engine
            .process(Transaction::Withdrawal {
                client: 1,
                tx: 3,
                amount: dec!(5.0),
            })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts.len(), 2);
        // Sorted by client ID
        assert_eq!(accounts[0].client, 1);
        assert_eq!(accounts[0].available, dec!(5.0));
        assert_eq!(accounts[1].client, 2);
        assert_eq!(accounts[1].available, dec!(20.0));
    }

    #[test]
    fn dispute_after_partial_withdrawal() {
        // Edge case: client deposits 100, withdraws 50, then deposit is disputed.
        // Available should go to -50 (held 100, total 50).
        // This is correct: the dispute holds the original deposit amount.
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(100.0),
            })
            .unwrap();
        engine
            .process(Transaction::Withdrawal {
                client: 1,
                tx: 2,
                amount: dec!(50.0),
            })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts[0].available, dec!(-50.0));
        assert_eq!(accounts[0].held, dec!(100.0));
        assert_eq!(accounts[0].total, dec!(50.0));
    }

    #[test]
    fn already_charged_back_cannot_be_disputed_again() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();
        engine
            .process(Transaction::Chargeback { client: 1, tx: 1 })
            .unwrap();

        // Cannot dispute again — deposit record was removed after chargeback
        let err = engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap_err();
        assert_eq!(err, EngineError::TransactionNotFound { tx: 1 });
    }

    #[test]
    fn dispute_resolve_then_re_dispute() {
        // A resolved transaction can be disputed again.
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.0),
            })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();
        engine
            .process(Transaction::Resolve { client: 1, tx: 1 })
            .unwrap();
        engine
            .process(Transaction::Dispute { client: 1, tx: 1 })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts[0].available, dec!(0.0));
        assert_eq!(accounts[0].held, dec!(10.0));
        assert_eq!(accounts[0].total, dec!(10.0));
    }

    #[test]
    fn precision_preserved() {
        let mut engine = Engine::new();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(1.1111),
            })
            .unwrap();
        engine
            .process(Transaction::Deposit {
                client: 1,
                tx: 2,
                amount: dec!(2.2222),
            })
            .unwrap();

        let accounts = engine.into_accounts();
        assert_eq!(accounts[0].available, dec!(3.3333));
    }
}
