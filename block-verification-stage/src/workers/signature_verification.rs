use crate::messages::{SignatureVerificationRequest, SignatureVerificationResponse};

pub(crate) const THREAD_NAME_PREFIX: &str = "solBlkVerSig";

/// Verifies each signature of the transaction against the static account key of its signer.
pub(crate) fn verify_signatures(
    SignatureVerificationRequest {
        bank_id,
        transaction,
    }: SignatureVerificationRequest,
) -> SignatureVerificationResponse {
    let signatures = transaction.signatures();
    let static_account_keys = transaction.static_account_keys();
    let num_required_signatures = usize::from(transaction.num_required_signatures());

    let has_a_signature_for_every_signer = signatures.len() == num_required_signatures;
    let every_signer_has_a_static_account_key =
        static_account_keys.len() >= num_required_signatures;

    let message = transaction.message_data();
    let every_signer_signed_the_message = signatures
        .iter()
        .zip(static_account_keys)
        .all(|(signature, signer)| signature.verify(signer.as_ref(), message));

    let verified = has_a_signature_for_every_signer
        && every_signer_has_a_static_account_key
        && every_signer_signed_the_message;

    SignatureVerificationResponse { bank_id, verified }
}
