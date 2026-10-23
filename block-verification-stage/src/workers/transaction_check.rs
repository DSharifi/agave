use {
    crate::messages::{TransactionCheckError, TransactionCheckRequest, TransactionCheckResponse},
    solana_runtime::bank_forks::BankForks,
    solana_transaction::TransactionVerificationMode,
    std::sync::RwLock,
};

pub(crate) const THREAD_NAME_PREFIX: &str = "solBlkVerChk";

/// Sanitizes the transactions and resolves their address lookup tables against the bank of the
/// block, as replay does before executing them. Signatures are verified by the signature
/// verification workers instead.
pub(crate) fn check_transactions(
    bank_forks: &RwLock<BankForks>,
    TransactionCheckRequest {
        bank_id,
        slot,
        transactions,
    }: TransactionCheckRequest,
) -> TransactionCheckResponse {
    // The bank id is compared too, as replay can replace the bank at a slot by dumping it.
    let bank = bank_forks
        .read()
        .unwrap()
        .get(slot)
        .filter(|bank| u64::from(bank.bank_id()) == bank_id);

    let result = match bank {
        Some(bank) => transactions.into_iter().try_for_each(|transaction| {
            bank.verify_transaction(transaction, TransactionVerificationMode::HashOnly)
                .map(|_resolved_transaction| ())
                .map_err(TransactionCheckError::InvalidTransaction)
        }),
        None => Err(TransactionCheckError::BankNotFound),
    };

    TransactionCheckResponse { bank_id, result }
}
