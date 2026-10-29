use {
    crate::messages::{TransactionCheckError, TransactionCheckRequest, TransactionCheckResponse},
    solana_runtime::bank_forks::BankForks,
    solana_transaction::TransactionVerificationMode,
    std::sync::RwLock,
};

pub(crate) const THREAD_NAME_PREFIX: &str = "solBlkVerChk";

/// Sanitizes the transaction and resolves its address lookup tables against the bank of the
/// block.
pub(crate) fn check_transaction(
    bank_forks: &RwLock<BankForks>,
    TransactionCheckRequest {
        bank_id,
        slot,
        transaction,
    }: TransactionCheckRequest,
) -> TransactionCheckResponse {
    let bank = bank_forks
        .read()
        .unwrap()
        .get(slot)
        .filter(|bank| u64::from(bank.bank_id()) == bank_id);

    let result = match bank {
        Some(bank) => bank
            .verify_transaction(transaction, TransactionVerificationMode::HashOnly)
            .map(|_resolved_transaction| ())
            .map_err(TransactionCheckError::InvalidTransaction),
        None => Err(TransactionCheckError::BankNotFound),
    };

    TransactionCheckResponse { bank_id, result }
}
