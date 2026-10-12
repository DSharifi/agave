use {
    crate::{
        config::SchedulerConfig,
        messages::{
            AbortMessage, AllEntriesSubmittedMessage, BeginMessage, BlockVerificationOutcome,
            BlockVerificationToReplayMessage, EntryHashVerificationRequest,
            EntryHashVerificationResponse, EntryMessage, ReplayToBlockVerificationMessage,
            VerificationSessionId, VerificationSessionIdGenerator,
        },
        stage::BlockVerificationStage,
        workers::entry_hash_verification::EntryHashVerificationWorkers,
    },
    crossbeam_channel::{Receiver, RecvError, Sender, TrySendError, bounded, select_biased},
    shaq::mpmc,
    solana_clock::{BankId, Slot},
    solana_hash::Hash,
    std::{
        collections::VecDeque,
        mem,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

/// The timeout used on waiting for messages before checking for a timeout
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(thiserror::Error, Debug)]
pub enum SchedulerExitReason {
    #[error("the block verification response receiver was dropped")]
    DisconnectedResponseChannel,
    #[error("the block verification request sender was dropped")]
    DisconnectedRequestChannel,
    #[error("every entry hash verification worker exited")]
    EntryHashVerificationWorkersExited,
}

impl From<DisconnectedResponseChannel> for SchedulerExitReason {
    fn from(_: DisconnectedResponseChannel) -> Self {
        SchedulerExitReason::DisconnectedResponseChannel
    }
}

pub(super) struct BlockVerificationScheduler {
    replay_message_receiver: Receiver<ReplayToBlockVerificationMessage>,
    replay_message_sender: Sender<BlockVerificationToReplayMessage>,

    entry_hash_verification_request_producer: mpmc::Producer<EntryHashVerificationRequest>,
    entry_hash_verification_response_receiver: Receiver<EntryHashVerificationResponse>,

    shutdown_signal: Arc<AtomicBool>,

    verification_session_id_generator: VerificationSessionIdGenerator,
    blocks_in_progress: Vec<BlockVerificationState>,
    buffered_replay_response_messages: VecDeque<BlockVerificationToReplayMessage>,

    // worker handles
    entry_hash_verification_workers: EntryHashVerificationWorkers,
}

impl BlockVerificationScheduler {
    pub(super) fn run_scheduler(
        scheduler_config: SchedulerConfig,
        shutdown_signal: Arc<AtomicBool>,
    ) -> BlockVerificationStage {
        // entry - scheduler
        let (entry_hash_verification_request_producer, entry_hash_verification_request_consumer) =
            mpmc::pair(scheduler_config.max_entry_hash_jobs_in_flight)
                .expect("failed to allocate the entry hash verification request queue");
        let (entry_hash_verification_response_sender, entry_hash_verification_response_receiver) =
            bounded(scheduler_config.max_entry_hash_jobs_in_flight);

        // replay - scheduler
        let replay_to_block_verification =
            bounded(scheduler_config.replay_to_block_verification_channel_size);
        let block_verification_to_replay =
            bounded(scheduler_config.block_verification_to_replay_channel_size);

        let entry_hash_verification_workers: EntryHashVerificationWorkers =
            EntryHashVerificationWorkers::spawn(
                shutdown_signal.clone(),
                scheduler_config.num_entry_hash_workers,
                entry_hash_verification_request_consumer,
                entry_hash_verification_response_sender,
            );

        let scheduler = Self {
            replay_message_receiver: replay_to_block_verification.1,
            replay_message_sender: block_verification_to_replay.0,
            entry_hash_verification_request_producer,
            entry_hash_verification_response_receiver,
            shutdown_signal,
            verification_session_id_generator: VerificationSessionIdGenerator::new(),
            blocks_in_progress: Vec::new(),
            buffered_replay_response_messages: VecDeque::new(),
            entry_hash_verification_workers,
        };

        let scheduler_thread_join_handle =
            std::thread::spawn(move || scheduler.run_scheduler_event_loop());

        BlockVerificationStage::new(
            replay_to_block_verification.0,
            block_verification_to_replay.1,
            scheduler_thread_join_handle,
        )
    }

    /// Runs until shutdown is requested, or any of the channels to replay or the workers disconnect.
    ///
    /// Blocks still in progress are dropped on exit without an outcome being sent for them.
    /// Dropping the scheduler drops its outcome sender, disconnecting the stage's receiver.
    fn run_scheduler_event_loop(mut self) -> Result<(), SchedulerExitReason> {
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            self.try_flush_buffered_replay_responses()?;

            // Responses come first, as handling them completes blocks and frees up job slots.
            select_biased! {
                recv(self.entry_hash_verification_response_receiver) -> response => {
                    let response = response.map_err(|_: RecvError| SchedulerExitReason::EntryHashVerificationWorkersExited)?;
                    let () = self.handle_entry_hash_verification_response(response)?;
                }
                recv(self.replay_message_receiver) -> replay_message => {
                    let replay_message = replay_message.map_err(|_: RecvError| SchedulerExitReason::DisconnectedResponseChannel)?;
                    let () = self.handle_replay_message(replay_message)?;
                }
                default(SHUTDOWN_POLL_INTERVAL) => {},
            }
        }

        let () = self.entry_hash_verification_workers.join_workers();

        Ok(())
    }

    /// Sends buffered replay responses without blocking, stopping at the first
    /// failed send. Any remaining responses stay buffered until the next flush.
    fn try_flush_buffered_replay_responses(&mut self) -> Result<(), DisconnectedResponseChannel> {
        while let Some(message) = self.buffered_replay_response_messages.pop_front() {
            match self.replay_message_sender.try_send(message) {
                Ok(()) => {}
                Err(TrySendError::Full(message)) => {
                    self.buffered_replay_response_messages.push_front(message);
                    break;
                }
                Err(TrySendError::Disconnected(_message)) => {
                    return Err(DisconnectedResponseChannel);
                }
            }
        }

        Ok(())
    }

    fn handle_replay_message(
        &mut self,
        replay_message: ReplayToBlockVerificationMessage,
    ) -> Result<(), DisconnectedResponseChannel> {
        match replay_message {
            ReplayToBlockVerificationMessage::Begin(begin_message) => {
                self.handle_begin_message(begin_message);
                Ok(())
            }
            ReplayToBlockVerificationMessage::Entry(entry_message) => {
                self.handle_entry_message(entry_message);
                Ok(())
            }
            ReplayToBlockVerificationMessage::AllEntriesSubmitted(
                all_entries_submitted_message,
            ) => self.handle_all_entries_submitted_message(all_entries_submitted_message),
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

        let block_verification_id = self.verification_session_id_generator.next_unique_id();

        self.blocks_in_progress.push(BlockVerificationState::new(
            block_verification_id,
            bank_id,
            parent_bank_last_entry_hash,
            slot,
        ));
    }

    fn handle_entry_message(
        &mut self,
        EntryMessage {
            bank_id,
            entry_view,
        }: EntryMessage,
    ) {
        let Some(block_verification_state) = self
            .blocks_in_progress
            .iter_mut()
            .find(|state| state.bank_id == bank_id)
        else {
            // The block already failed verification
            return;
        };

        // Every entry carries its own hash, which the next entry is verified against. So the
        // entries of a block are verified independently of each other.
        let start_hash = mem::replace(
            &mut block_verification_state.last_entry_hash,
            entry_view.hash,
        );
        let progress_tracker = &mut block_verification_state.progress_tracker;

        progress_tracker.entry_hash_jobs_in_flight = progress_tracker
            .entry_hash_jobs_in_flight
            .checked_add(1)
            .expect("entry hash jobs in flight fits in u64");

        let request = EntryHashVerificationRequest {
            block_verification_id: block_verification_state.block_verification_id,
            start_hash,
            entry_view,
        };

        let () = self
            .entry_hash_verification_request_producer
            .try_write(request)
            .expect("verification workers can keep up");
    }

    fn handle_abort_message(
        &mut self,
        AbortMessage { bank_id }: AbortMessage,
    ) -> Result<(), DisconnectedResponseChannel> {
        let Some(block_verification_state) =
            self.take_block_verification_state_by_bank_id(&bank_id)
        else {
            // The block completed or failed verification before the abort message arrived
            return Ok(());
        };

        self.send_outcome(block_verification_state, BlockVerificationOutcome::Aborted)
    }

    fn handle_all_entries_submitted_message(
        &mut self,
        AllEntriesSubmittedMessage { bank_id }: AllEntriesSubmittedMessage,
    ) -> Result<(), DisconnectedResponseChannel> {
        let Some(mut block_verification_state) =
            self.take_block_verification_state_by_bank_id(&bank_id)
        else {
            // The block already failed verification
            return Ok(());
        };

        block_verification_state
            .progress_tracker
            .all_entries_are_submitted = true;

        self.try_complete_block(block_verification_state)
    }

    fn handle_entry_hash_verification_response(
        &mut self,
        EntryHashVerificationResponse {
            block_verification_id,
            verified,
        }: EntryHashVerificationResponse,
    ) -> Result<(), DisconnectedResponseChannel> {
        let Some(mut block_verification_state) = self.take_block_verification_state(|state| {
            state.block_verification_id == block_verification_id
        }) else {
            // The block was aborted or already failed verification
            return Ok(());
        };

        if !verified {
            return self.send_outcome(
                block_verification_state,
                BlockVerificationOutcome::VerificationFailed,
            );
        }

        let progress_tracker = &mut block_verification_state.progress_tracker;
        progress_tracker.entry_hash_jobs_in_flight = progress_tracker
            .entry_hash_jobs_in_flight
            .checked_sub(1)
            .expect("entry hash verification response without a job in flight for the block");

        self.try_complete_block(block_verification_state)
    }

    fn take_block_verification_state_by_bank_id(
        &mut self,
        bank_id: &BankId,
    ) -> Option<BlockVerificationState> {
        self.take_block_verification_state(|state| state.bank_id == *bank_id)
    }

    /// Takes the state of the first block matching `predicate` out of the scheduler state.
    fn take_block_verification_state(
        &mut self,
        predicate: impl Fn(&BlockVerificationState) -> bool,
    ) -> Option<BlockVerificationState> {
        let block_verification_state_index = self.blocks_in_progress.iter().position(predicate)?;

        Some(
            self.blocks_in_progress
                .swap_remove(block_verification_state_index),
        )
    }

    /// Sends a [`BlockVerificationOutcome::Verified`] message back to replay if every verification
    /// operation of `block_verification_state` has finished, and returns the state to the
    /// scheduler otherwise.
    fn try_complete_block(
        &mut self,
        block_verification_state: BlockVerificationState,
    ) -> Result<(), DisconnectedResponseChannel> {
        if !block_verification_state.progress_tracker.is_completed() {
            self.blocks_in_progress.push(block_verification_state);
            return Ok(());
        }

        self.send_outcome(block_verification_state, BlockVerificationOutcome::Verified)
    }

    /// Sends the outcome of the block of `block_verification_state`, which has been taken out
    /// of the scheduler state, back to replay.
    fn send_outcome(
        &mut self,
        block_verification_state: BlockVerificationState,
        verification_status: BlockVerificationOutcome,
    ) -> Result<(), DisconnectedResponseChannel> {
        self.buffered_replay_response_messages
            .push_back(BlockVerificationToReplayMessage {
                slot: block_verification_state.slot,
                bank_id: block_verification_state.bank_id,
                verification_status,
            });
        self.try_flush_buffered_replay_responses()
    }
}

struct BlockVerificationState {
    block_verification_id: VerificationSessionId,
    bank_id: BankId,
    /// The hash of the last entry submitted for the block, which the next entry is verified
    /// against. Starts as the parent bank's last entry hash.
    last_entry_hash: Hash,
    slot: Slot,
    progress_tracker: BlockVerificationStatus,
}

impl BlockVerificationState {
    fn new(
        block_verification_id: VerificationSessionId,
        bank_id: BankId,
        parent_bank_last_entry_hash: Hash,
        slot: Slot,
    ) -> Self {
        Self {
            block_verification_id,
            bank_id,
            last_entry_hash: parent_bank_last_entry_hash,
            slot,
            progress_tracker: BlockVerificationStatus::default(),
        }
    }
}

struct DisconnectedResponseChannel;

#[derive(Default)]
struct BlockVerificationStatus {
    all_entries_are_submitted: bool,
    entry_hash_jobs_in_flight: usize,
}

impl BlockVerificationStatus {
    fn is_completed(&self) -> bool {
        self.all_entries_are_submitted && self.entry_hash_jobs_in_flight == 0
    }
}
