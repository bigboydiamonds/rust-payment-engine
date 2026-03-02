use std::borrow::Cow;

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::error::TransactionParseError;

/// Raw CSV row matching the input schema.
///
/// This flat struct is the deserialization target for `csv::Reader`.
/// Conversion to the validated [`Transaction`] enum happens via `TryFrom`.
#[derive(Debug, Deserialize)]
pub struct RawTransaction {
    #[serde(rename = "type")]
    pub r#type: String,
    pub client: u16,
    pub tx: u32,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    pub amount: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Transaction {
    Deposit {
        client: u16,
        tx: u32,
        amount: Decimal,
    },
    Withdrawal {
        client: u16,
        tx: u32,
        amount: Decimal,
    },
    Dispute {
        client: u16,
        tx: u32,
    },
    Resolve {
        client: u16,
        tx: u32,
    },
    Chargeback {
        client: u16,
        tx: u32,
    },
}

impl TryFrom<RawTransaction> for Transaction {
    type Error = TransactionParseError;

    fn try_from(raw: RawTransaction) -> Result<Self, Self::Error> {
        // Trim handles whitespace around the type field, which is common
        // in CSV files with spaces after delimiters (e.g., "type, client").
        match raw.r#type.trim() {
            "deposit" => {
                let amount = raw.amount.ok_or(TransactionParseError::MissingAmount)?;
                if amount.is_sign_negative() || amount.is_zero() {
                    return Err(TransactionParseError::InvalidAmount);
                }
                Ok(Transaction::Deposit {
                    client: raw.client,
                    tx: raw.tx,
                    amount,
                })
            }
            "withdrawal" => {
                let amount = raw.amount.ok_or(TransactionParseError::MissingAmount)?;
                if amount.is_sign_negative() || amount.is_zero() {
                    return Err(TransactionParseError::InvalidAmount);
                }
                Ok(Transaction::Withdrawal {
                    client: raw.client,
                    tx: raw.tx,
                    amount,
                })
            }
            "dispute" => Ok(Transaction::Dispute {
                client: raw.client,
                tx: raw.tx,
            }),
            "resolve" => Ok(Transaction::Resolve {
                client: raw.client,
                tx: raw.tx,
            }),
            "chargeback" => Ok(Transaction::Chargeback {
                client: raw.client,
                tx: raw.tx,
            }),
            other => Err(TransactionParseError::UnknownType(other.to_string())),
        }
    }
}

/// Custom deserializer for the optional amount field.
///
/// Dispute, resolve, and chargeback rows may omit the amount or leave it
/// as an empty string. The default `Option<Decimal>` deserializer would
/// error on an empty string, so we handle that case explicitly.
fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Use Cow<str> to avoid allocating a String when the deserializer can
    // provide a borrowed reference (which csv's deserializer supports).
    let s: Cow<'de, str> = Cow::deserialize(deserializer)?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse::<Decimal>()
        .map(Some)
        .map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn raw(type_: &str, client: u16, tx: u32, amount: Option<Decimal>) -> RawTransaction {
        RawTransaction {
            r#type: type_.to_string(),
            client,
            tx,
            amount,
        }
    }

    #[test]
    fn parse_deposit() {
        let tx = Transaction::try_from(raw("deposit", 1, 1, Some(dec!(10.5)))).unwrap();
        assert_eq!(
            tx,
            Transaction::Deposit {
                client: 1,
                tx: 1,
                amount: dec!(10.5)
            }
        );
    }

    #[test]
    fn parse_withdrawal() {
        let tx = Transaction::try_from(raw("withdrawal", 2, 5, Some(dec!(3.0)))).unwrap();
        assert_eq!(
            tx,
            Transaction::Withdrawal {
                client: 2,
                tx: 5,
                amount: dec!(3.0)
            }
        );
    }

    #[test]
    fn parse_dispute() {
        let tx = Transaction::try_from(raw("dispute", 1, 1, None)).unwrap();
        assert_eq!(tx, Transaction::Dispute { client: 1, tx: 1 });
    }

    #[test]
    fn parse_resolve() {
        let tx = Transaction::try_from(raw("resolve", 1, 1, None)).unwrap();
        assert_eq!(tx, Transaction::Resolve { client: 1, tx: 1 });
    }

    #[test]
    fn parse_chargeback() {
        let tx = Transaction::try_from(raw("chargeback", 1, 1, None)).unwrap();
        assert_eq!(tx, Transaction::Chargeback { client: 1, tx: 1 });
    }

    #[test]
    fn deposit_missing_amount() {
        let err = Transaction::try_from(raw("deposit", 1, 1, None)).unwrap_err();
        assert_eq!(err, TransactionParseError::MissingAmount);
    }

    #[test]
    fn withdrawal_missing_amount() {
        let err = Transaction::try_from(raw("withdrawal", 1, 1, None)).unwrap_err();
        assert_eq!(err, TransactionParseError::MissingAmount);
    }

    #[test]
    fn negative_amount_rejected() {
        let err = Transaction::try_from(raw("deposit", 1, 1, Some(dec!(-5.0)))).unwrap_err();
        assert_eq!(err, TransactionParseError::InvalidAmount);
    }

    #[test]
    fn zero_amount_rejected() {
        let err = Transaction::try_from(raw("deposit", 1, 1, Some(dec!(0)))).unwrap_err();
        assert_eq!(err, TransactionParseError::InvalidAmount);
    }

    #[test]
    fn unknown_type_rejected() {
        let err = Transaction::try_from(raw("refund", 1, 1, Some(dec!(1.0)))).unwrap_err();
        assert_eq!(err, TransactionParseError::UnknownType("refund".into()));
    }

    #[test]
    fn whitespace_in_type_trimmed() {
        let tx = Transaction::try_from(raw("  deposit  ", 1, 1, Some(dec!(1.0)))).unwrap();
        assert!(matches!(tx, Transaction::Deposit { .. }));
    }
}
