use {
    crate::{
        config::SchedulerConfig,
        messages::{
            AbortMessage, AllEntriesSubmittedMessage, BeginMessage, BlockVerificationOutcome,
            BlockVerificationToReplayMessage, EntryMessage, ReplayToBlockVerificationMessage,
        },
        stage::BlockVerificationStage,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded},
    solana_clock::{BankId, Slot},
    solana_hash::Hash,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

/// How long the event loop waits for a replay message before checking for shutdown.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(100);

pub(super) struct BlockVerificationScheduler {
    replay_message_receiver: Receiver<ReplayToBlockVerificationMessage>,
    replay_message_sender: Sender<BlockVerificationToReplayMessage>,

    shutdown_signal: Arc<AtomicBool>,
    blocks_in_progress: Vec<BlockVerificationState>,
}

impl BlockVerificationScheduler {
    pub(super) fn run_scheduler(
        scheduler_config: SchedulerConfig,
        shutdown_signal: Arc<AtomicBool>,
    ) -> BlockVerificationStage {
        let replay_to_block_verification =
            bounded(scheduler_config.replay_to_block_verification_channel_size);
        let block_verification_to_replay =
            bounded(scheduler_config.block_verification_to_replay_channel_size);

        let scheduler = Self {
            replay_message_receiver: replay_to_block_verification.1,
            replay_message_sender: block_verification_to_replay.0,
            shutdown_signal: shutdown_signal.clone(),
            blocks_in_progress: Vec::new(),
        };

        let scheduler_thread_join_handle =
            std::thread::spawn(move || scheduler.run_scheduler_event_loop());

        BlockVerificationStage::new(
            replay_to_block_verification.0,
            block_verification_to_replay.1,
            scheduler_thread_join_handle,
        )
    }

    /// Runs until shutdown is requested or every replay message sender is dropped.
    ///
    /// Blocks still in progress are dropped on exit without an outcome being sent for them.
    /// Dropping the scheduler drops its outcome sender, disconnecting the stage's receiver.
    fn run_scheduler_event_loop(mut self) {
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            match self
                .replay_message_receiver
                .recv_timeout(SHUTDOWN_POLL_INTERVAL)
            {
                Ok(replay_message) => self.handle_replay_message(replay_message),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn handle_replay_message(&mut self, replay_message: ReplayToBlockVerificationMessage) {
        match replay_message {
            ReplayToBlockVerificationMessage::Begin(begin_message) => {
                self.handle_begin_message(begin_message)
            }
            ReplayToBlockVerificationMessage::Entry(entry_message) => {
                self.handle_entry_message(entry_message)
            }
            ReplayToBlockVerificationMessage::AllEntriesSubmitted(
                all_entries_submitted_message,
            ) => {
                self.handle_all_entries_submitted_message(all_entries_submitted_message);
            }
            ReplayToBlockVerificationMessage::Abort(abort_message) => {
                self.handle_abort_message(abort_message)
            }
        }
    }

    fn handle_begin_message(
        &mut self,
        BeginMessage {
            bank_id,
            parent_bank_last_entry_hash,
            slot,
        }: BeginMessage,
    ) {
        let bank_id_is_in_use = self.blocks_in_progress.iter().any(|e| e.bank_id == bank_id);
        if bank_id_is_in_use {
            panic!(
                "internal invariant violated. A duplicate bank id was requested to be scheduled"
            );
        }

        let slot_is_in_use = self.blocks_in_progress.iter().any(|e| e.slot == slot);
        if slot_is_in_use {
            panic!("internal invariant violated. A duplicate slot was requested to be scheduled");
        }

        self.blocks_in_progress.push(BlockVerificationState::new(
            bank_id,
            parent_bank_last_entry_hash,
            slot,
        ));
    }

    fn handle_entry_message(&mut self, _entry_message: EntryMessage) {}

    fn handle_abort_message(&mut self, AbortMessage { bank_id }: AbortMessage) {
        let Some(block_verification_state) = self.take_block_verification_state(&bank_id) else {
            // The block completed or failed verification before the abort message arrived
            return;
        };

        let _ = self
            .replay_message_sender
            .send(BlockVerificationToReplayMessage {
                slot: block_verification_state.slot,
                bank_id: block_verification_state.bank_id,
                verification_status: BlockVerificationOutcome::Aborted,
            });
    }

    fn handle_all_entries_submitted_message(
        &mut self,
        AllEntriesSubmittedMessage { bank_id }: AllEntriesSubmittedMessage,
    ) {
        let Some(mut block_verification_state) = self.take_block_verification_state(&bank_id)
        else {
            // The block already failed verification
            return;
        };

        block_verification_state
            .progress_tracker
            .all_entries_are_submitted = true;

        self.try_complete_block(block_verification_state);
    }

    /// Takes the state of the block being verified in `bank_id` out of the scheduler state.
    fn take_block_verification_state(
        &mut self,
        bank_id: &BankId,
    ) -> Option<BlockVerificationState> {
        let block_verification_state_index = self
            .blocks_in_progress
            .iter()
            .position(|state| state.bank_id == *bank_id)?;

        Some(
            self.blocks_in_progress
                .swap_remove(block_verification_state_index),
        )
    }

    /// Sends a [`BlockVerificationOutcome::Verified`] message back to replay if every verification
    /// operation of `block_verification_state` has finished, and returns the state to the
    /// scheduler otherwise.
    fn try_complete_block(&mut self, block_verification_state: BlockVerificationState) {
        if block_verification_state.progress_tracker.is_completed() {
            let _ = self
                .replay_message_sender
                .send(BlockVerificationToReplayMessage {
                    slot: block_verification_state.slot,
                    bank_id: block_verification_state.bank_id,
                    verification_status: BlockVerificationOutcome::Verified,
                });
        } else {
            self.blocks_in_progress.push(block_verification_state);
        }
    }
}

struct BlockVerificationState {
    bank_id: BankId,
    #[expect(dead_code)]
    parent_bank_last_entry_hash: Hash,
    slot: Slot,
    progress_tracker: BlockVerificationStatus,
}

impl BlockVerificationState {
    fn new(bank_id: BankId, parent_bank_last_entry_hash: Hash, slot: Slot) -> Self {
        Self {
            bank_id,
            parent_bank_last_entry_hash,
            slot,
            progress_tracker: BlockVerificationStatus::default(),
        }
    }
}

#[derive(Default)]
struct BlockVerificationStatus {
    all_entries_are_submitted: bool,
}

impl BlockVerificationStatus {
    fn is_completed(&self) -> bool {
        self.all_entries_are_submitted
    }
}
