use rust_decimal::Decimal;
use serde::Serialize;

/// A client's asset account tracking available, held, and total balances.
///
/// Invariant: `total == available + held` must always hold after any operation.
/// We maintain `total` as a running value rather than computing it on access
/// so that tests can assert the invariant is preserved — if a bug breaks it,
/// tests catch the drift rather than a computed property masking it.
#[derive(Debug, Clone, Serialize)]
pub struct ClientAccount {
    pub client: u16,
    #[serde(serialize_with = "serialize_decimal")]
    pub available: Decimal,
    #[serde(serialize_with = "serialize_decimal")]
    pub held: Decimal,
    #[serde(serialize_with = "serialize_decimal")]
    pub total: Decimal,
    pub locked: bool,
}

/// Serialize a Decimal with up to 4 decimal places of precision.
///
/// The spec requires output precision of up to four places past the decimal.
/// We intentionally accept higher-precision input (e.g., `1.123456`) and round
/// only on output — this avoids data loss during intermediate arithmetic.
///
/// `round_dp(4)` rounds to *at most* 4 decimal places but does not pad trailing
/// zeros (e.g., `Decimal::from(1)` serializes as "1", not "1.0000"). This is
/// acceptable per the spec: "Spacing and displaying decimals for round values
/// do not matter."
fn serialize_decimal<S>(val: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let rounded = val.round_dp(4);
    serializer.serialize_str(&rounded.to_string())
}

impl ClientAccount {
    pub fn new(client: u16) -> Self {
        Self {
            client,
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            total: Decimal::ZERO,
            locked: false,
        }
    }

    /// Credit funds to the account. Increases available and total.
    pub fn deposit(&mut self, amount: Decimal) {
        self.available += amount;
        self.total += amount;
    }

    /// Debit funds from the account. Returns false if insufficient available funds.
    /// On failure, account state is unchanged.
    pub fn withdraw(&mut self, amount: Decimal) -> bool {
        if self.available < amount {
            return false;
        }
        self.available -= amount;
        self.total -= amount;
        true
    }

    /// Move funds from available to held (dispute).
    /// Total remains unchanged.
    pub fn hold(&mut self, amount: Decimal) {
        self.available -= amount;
        self.held += amount;
    }

    /// Move funds from held back to available (resolve).
    /// Total remains unchanged.
    pub fn release(&mut self, amount: Decimal) {
        self.held -= amount;
        self.available += amount;
    }

    /// Reverse a disputed transaction (chargeback).
    /// Decreases held and total, and freezes the account.
    pub fn chargeback(&mut self, amount: Decimal) {
        self.held -= amount;
        self.total -= amount;
        self.locked = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// Helper to assert the fundamental invariant: total == available + held.
    fn assert_invariant(account: &ClientAccount) {
        assert_eq!(
            account.total,
            account.available + account.held,
            "invariant violated: total ({}) != available ({}) + held ({})",
            account.total,
            account.available,
            account.held
        );
    }

    #[test]
    fn new_account_is_zeroed() {
        let account = ClientAccount::new(1);
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::ZERO);
        assert_eq!(account.total, Decimal::ZERO);
        assert!(!account.locked);
        assert_invariant(&account);
    }

    #[test]
    fn deposit_increases_available_and_total() {
        let mut account = ClientAccount::new(1);
        account.deposit(dec!(10.5));
        assert_eq!(account.available, dec!(10.5));
        assert_eq!(account.total, dec!(10.5));
        assert_eq!(account.held, Decimal::ZERO);
        assert_invariant(&account);
    }

    #[test]
    fn withdrawal_decreases_available_and_total() {
        let mut account = ClientAccount::new(1);
        account.deposit(dec!(10.0));
        assert!(account.withdraw(dec!(3.5)));
        assert_eq!(account.available, dec!(6.5));
        assert_eq!(account.total, dec!(6.5));
        assert_invariant(&account);
    }

    #[test]
    fn withdrawal_fails_on_insufficient_funds() {
        let mut account = ClientAccount::new(1);
        account.deposit(dec!(5.0));
        assert!(!account.withdraw(dec!(10.0)));
        // State unchanged on failure
        assert_eq!(account.available, dec!(5.0));
        assert_eq!(account.total, dec!(5.0));
        assert_invariant(&account);
    }

    #[test]
    fn hold_moves_funds_to_held() {
        let mut account = ClientAccount::new(1);
        account.deposit(dec!(10.0));
        account.hold(dec!(4.0));
        assert_eq!(account.available, dec!(6.0));
        assert_eq!(account.held, dec!(4.0));
        assert_eq!(account.total, dec!(10.0));
        assert_invariant(&account);
    }

    #[test]
    fn release_moves_funds_back_to_available() {
        let mut account = ClientAccount::new(1);
        account.deposit(dec!(10.0));
        account.hold(dec!(4.0));
        account.release(dec!(4.0));
        assert_eq!(account.available, dec!(10.0));
        assert_eq!(account.held, Decimal::ZERO);
        assert_eq!(account.total, dec!(10.0));
        assert_invariant(&account);
    }

    #[test]
    fn chargeback_decreases_held_and_total_and_freezes() {
        let mut account = ClientAccount::new(1);
        account.deposit(dec!(10.0));
        account.hold(dec!(10.0));
        account.chargeback(dec!(10.0));
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::ZERO);
        assert_eq!(account.total, Decimal::ZERO);
        assert!(account.locked);
        assert_invariant(&account);
    }

    #[test]
    fn multiple_deposits_accumulate() {
        let mut account = ClientAccount::new(1);
        account.deposit(dec!(1.1111));
        account.deposit(dec!(2.2222));
        assert_eq!(account.available, dec!(3.3333));
        assert_eq!(account.total, dec!(3.3333));
        assert_invariant(&account);
    }

    #[test]
    fn withdrawal_of_exact_balance_succeeds() {
        let mut account = ClientAccount::new(1);
        account.deposit(dec!(5.0));
        assert!(account.withdraw(dec!(5.0)));
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.total, Decimal::ZERO);
        assert_invariant(&account);
    }
}
