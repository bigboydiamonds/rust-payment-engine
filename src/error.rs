use thiserror::Error;

/// Errors that occur during engine processing of a transaction.
///
/// These errors are non-fatal: the engine logs them and continues
/// processing subsequent transactions. Only I/O errors are fatal.
#[derive(Debug, Error, PartialEq)]
pub enum EngineError {
    #[error("account {client} is frozen due to chargeback")]
    AccountFrozen { client: u16 },

    #[error("insufficient available funds for client {client}")]
    InsufficientFunds { client: u16 },

    #[error("referenced transaction {tx} not found")]
    TransactionNotFound { tx: u32 },

    #[error("client mismatch on tx {tx}: expected {expected}, got {got}")]
    ClientMismatch { tx: u32, expected: u16, got: u16 },

    #[error("invalid dispute state for tx {tx}")]
    InvalidDisputeState { tx: u32 },

    #[error("duplicate transaction id {tx}")]
    DuplicateTransaction { tx: u32 },
}

/// Errors during CSV row parsing and validation.
#[derive(Debug, Error, PartialEq)]
pub enum TransactionParseError {
    #[error("missing amount for deposit or withdrawal")]
    MissingAmount,

    #[error("amount must be positive")]
    InvalidAmount,

    #[error("unknown transaction type: {0}")]
    UnknownType(String),
}
