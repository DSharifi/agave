use {
    block_verification_stage::{
        BlockVerificationStage, SchedulerConfig,
        messages::BlockVerificationToReplayMessage,
        stage::{BlockVerificationSession, Started},
    },
    bytes::Bytes,
    solana_clock::{BankId, Slot},
    solana_entry::entry::{EntryView, entry_views_for_tests, next_entry_mut},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_leader_schedule::SlotLeader,
    solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::create_genesis_config},
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    solana_transaction::Transaction,
    std::{
        num::NonZeroUsize,
        sync::{Arc, RwLock},
        time::Duration,
    },
};

pub const TEST_SCHEDULER_CONFIG: SchedulerConfig = SchedulerConfig {
    // Room for every message a test sends, so sending never waits on the scheduler.
    replay_to_block_verification_channel_size: 1024,
    block_verification_to_replay_channel_size: 10,
    num_entry_hash_workers: NonZeroUsize::new(2).unwrap(),
    max_entry_hash_verifications_in_flight: NonZeroUsize::new(64).unwrap(),
    num_signature_workers: NonZeroUsize::new(2).unwrap(),
    max_signature_verifications_in_flight: NonZeroUsize::new(16).unwrap(),
    num_transaction_check_workers: NonZeroUsize::new(2).unwrap(),
    max_transaction_checks_in_flight: NonZeroUsize::new(16).unwrap(),
};
pub const BOGUS_HASH: Hash = Hash::new_from_array([1; _]);
pub const PARENT_BANK_LAST_ENTRY_HASH: Hash = Hash::new_from_array([2; _]);
/// The bank id of the first bank created from a root bank.
pub const BANK_ID: BankId = 1;
pub const SLOT: Slot = 120;
pub const RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// Returns bank forks with the bank at `SLOT` whose transactions the tests check.
pub fn new_bank_forks() -> Arc<RwLock<BankForks>> {
    let (root_bank, bank_forks) =
        Bank::new_with_bank_forks_for_tests(&create_genesis_config(1).genesis_config);
    // The root bank lacks the slot hashes sysvar that address lookup tables are resolved with.
    let bank =
        Bank::new_from_parent_with_bank_forks(&bank_forks, root_bank, SlotLeader::default(), SLOT);
    assert_eq!(u64::from(bank.bank_id()), BANK_ID);
    bank_forks
}

/// Replaces the signatures of `transaction` with those of `signers` over its message.
pub fn sign(transaction: &mut Transaction, signers: &[&Keypair]) {
    let message = transaction.message_data();
    transaction.signatures = signers
        .iter()
        .map(|signer| signer.sign_message(&message))
        .collect();
}

/// Returns a transfer from `payer` to `recipient`, signed by the payer, and by the recipient too
/// with two signatures. Its static account keys are the payer, the recipient and the system
/// program.
pub fn transfer(payer: &Keypair, recipient: &Keypair, num_signatures: u8) -> Transaction {
    let mut transaction =
        system_transaction::transfer(payer, &recipient.pubkey(), 1, Hash::default());
    transaction.message.header.num_required_signatures = num_signatures;
    sign(
        &mut transaction,
        &[payer, recipient][..usize::from(num_signatures)],
    );
    transaction
}

/// Returns a chain of `num_entries` entries following `start_hash`, alternating between ticks and
/// entries with a transfer with `num_signatures` signatures, each `num_hashes` after the previous
/// one.
pub fn entry_views_for_chain(
    start_hash: Hash,
    num_entries: usize,
    num_hashes: u64,
    num_signatures: u8,
) -> Vec<EntryView<Bytes>> {
    let payer = Keypair::new();
    let recipient = Keypair::new();
    let mut hash = start_hash;
    let entries = (0..num_entries)
        .map(|index| {
            let transactions = if index.is_multiple_of(2) {
                vec![]
            } else {
                vec![transfer(&payer, &recipient, num_signatures)]
            };
            next_entry_mut(&mut hash, num_hashes, transactions)
        })
        .collect();
    entry_views_for_tests(entries)
}

pub fn submit_entries(
    session: &BlockVerificationSession<Started>,
    entry_views: Vec<EntryView<Bytes>>,
) {
    for entry_view in entry_views {
        session.submit_entry(entry_view).unwrap();
    }
}

pub fn recv_outcome(stage: &BlockVerificationStage) -> BlockVerificationToReplayMessage {
    stage
        .scheduler_message_receiver()
        .recv_timeout(RECV_TIMEOUT)
        .unwrap()
}
