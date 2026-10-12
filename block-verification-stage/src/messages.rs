use {
    bytes::Bytes,
    solana_clock::{BankId, Slot},
    solana_entry::entry::EntryView,
    solana_hash::Hash,
};

#[derive(Debug, PartialEq, Eq)]
pub enum BlockVerificationOutcome {
    Verified,
    VerificationFailed,
    Aborted,
}

#[derive(Debug)]
pub(crate) enum ReplayToBlockVerificationMessage {
    Begin(BeginMessage),
    Entry(EntryMessage),
    AllEntriesSubmitted(AllEntriesSubmittedMessage),
    Abort(AbortMessage),
}

#[derive(Debug)]
pub(crate) struct BeginMessage {
    pub(crate) bank_id: BankId,
    pub(crate) parent_bank_last_entry_hash: Hash,
    pub(crate) slot: Slot,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlockVerificationToReplayMessage {
    pub slot: Slot,
    pub bank_id: BankId,
    pub verification_status: BlockVerificationOutcome,
}

#[derive(Debug)]
pub(crate) struct EntryMessage {
    pub(crate) bank_id: BankId,
    pub(crate) entry_view: EntryView<Bytes>,
}

#[derive(Debug)]
pub(crate) struct AllEntriesSubmittedMessage {
    pub(crate) bank_id: BankId,
}

#[derive(Debug)]
pub(crate) struct AbortMessage {
    pub(crate) bank_id: BankId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VerificationSessionId(u64);

pub(crate) struct VerificationSessionIdGenerator {
    next_id: u64,
}

impl VerificationSessionIdGenerator {
    pub(crate) fn new() -> Self {
        Self { next_id: 0 }
    }

    pub(crate) fn next_unique_id(&mut self) -> VerificationSessionId {
        let verification_session_id = VerificationSessionId(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("we never start more than u64::MAX verification sessions");

        verification_session_id
    }
}

/// A request to verify the hash of a single entry of a block against the hash of the entry
/// preceding it.
#[derive(Debug)]
pub(crate) struct EntryHashVerificationRequest {
    pub(crate) block_verification_id: VerificationSessionId,
    /// The hash of the entry preceding `entry_view` in its block, or the parent bank's last entry
    /// hash for the first entry of the block.
    pub(crate) start_hash: Hash,
    pub(crate) entry_view: EntryView<Bytes>,
}

#[derive(Debug)]
pub(crate) struct EntryHashVerificationResponse {
    pub(crate) block_verification_id: VerificationSessionId,
    pub(crate) verified: bool,
}
