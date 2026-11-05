use {
    bytes::Bytes, solana_accounts_db::bank_id::BankId, solana_clock::Slot,
    solana_entry::entry::EntryView, solana_hash::Hash,
};

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
