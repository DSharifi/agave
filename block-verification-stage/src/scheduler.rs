use {
    crate::{
        config::SchedulerConfig,
        messages::{
            AbortMessage, AllEntriesSubmittedMessage, BeginMessage, BlockVerificationOutcome,
            BlockVerificationToReplayMessage, EntryHashVerificationRequest,
            EntryHashVerificationResponse, EntryMessage, ReplayToBlockVerificationMessage,
            VerificationFailureReason,
        },
        stage::BlockVerificationStage,
        utils::ShutdownOnDrop,
        workers::{VerificationQueue, VerificationWorkers, entry_hash_verification},
    },
    crossbeam_channel::{Receiver, RecvError, Sender, TrySendError, bounded, never, select},
    solana_clock::{BankId, Slot},
    solana_entry::entry::EntryVerificationData,
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

    entry_hash_verification_queue:
        VerificationQueue<EntryHashVerificationRequest, EntryHashVerificationResponse>,

    shutdown_signal: Arc<AtomicBool>,
    workers_shutdown_signal: ShutdownOnDrop,

    blocks_in_progress: Vec<BlockVerificationState>,

    entry_hash_verification_workers: VerificationWorkers,

    buffered_replay_response_messages: VecDeque<BlockVerificationToReplayMessage>,
}

impl BlockVerificationScheduler {
    pub(super) fn run_scheduler(
        scheduler_config: SchedulerConfig,
        shutdown_signal: Arc<AtomicBool>,
    ) -> BlockVerificationStage {
        let (
            entry_hash_verification_queue,
            entry_hash_verification_request_consumer,
            entry_hash_verification_response_sender,
        ) = VerificationQueue::new(scheduler_config.entry_hash_verification_queue_size);

        let replay_to_block_verification =
            bounded(scheduler_config.replay_to_block_verification_channel_size);
        let block_verification_to_replay =
            bounded(scheduler_config.block_verification_to_replay_channel_size);

        // Created before any worker is spawned, so if spawning a worker panics, the workers already
        // spawned are shut down.
        let workers_shutdown_signal = ShutdownOnDrop::from(Arc::new(AtomicBool::new(false)));

        let entry_hash_verification_workers = VerificationWorkers::spawn(
            entry_hash_verification::THREAD_NAME_PREFIX,
            workers_shutdown_signal.signal().clone(),
            scheduler_config.num_entry_hash_workers,
            entry_hash_verification_request_consumer,
            entry_hash_verification_response_sender,
            entry_hash_verification::verify_entry_hash,
        );

        let scheduler = Self::new(
            replay_to_block_verification.1,
            block_verification_to_replay.0,
            entry_hash_verification_queue,
            shutdown_signal,
            workers_shutdown_signal,
            entry_hash_verification_workers,
        );

        let scheduler_thread_join_handle =
            std::thread::spawn(move || scheduler.run_scheduler_event_loop());

        BlockVerificationStage::new(
            replay_to_block_verification.0,
            block_verification_to_replay.1,
            scheduler_thread_join_handle,
        )
    }

    fn new(
        replay_message_receiver: Receiver<ReplayToBlockVerificationMessage>,
        replay_message_sender: Sender<BlockVerificationToReplayMessage>,
        entry_hash_verification_queue: VerificationQueue<
            EntryHashVerificationRequest,
            EntryHashVerificationResponse,
        >,
        shutdown_signal: Arc<AtomicBool>,
        workers_shutdown_signal: ShutdownOnDrop,
        entry_hash_verification_workers: VerificationWorkers,
    ) -> Self {
        Self {
            replay_message_receiver,
            replay_message_sender,
            entry_hash_verification_queue,
            shutdown_signal,
            workers_shutdown_signal,
            blocks_in_progress: Vec::new(),
            buffered_replay_response_messages: VecDeque::new(),
            entry_hash_verification_workers,
        }
    }

    /// Runs until shutdown is requested, or any of the channels to replay or the workers disconnect.
    ///
    /// Blocks still in progress are dropped on exit without an outcome being sent for them.
    /// Dropping the scheduler drops its outcome sender, disconnecting the stage's receiver.
    fn run_scheduler_event_loop(mut self) -> Result<(), SchedulerExitReason> {
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            self.try_flush_buffered_replay_responses()?;

            let replay_message_receiver = if self.can_receive_replay_message() {
                &self.replay_message_receiver
            }
            // back-pressure; replay's channel fills up until a worker responds to a request in
            // flight
            else {
                &never()
            };

            select! {
                recv(self.entry_hash_verification_queue.response_receiver()) -> response => {
                    let response = response.map_err(|_: RecvError| SchedulerExitReason::EntryHashVerificationWorkersExited)?;
                    let () = self.handle_entry_hash_verification_response(response)?;
                }
                recv(replay_message_receiver) -> replay_message => {
                    let replay_message = replay_message.map_err(|_: RecvError| SchedulerExitReason::DisconnectedRequestChannel)?;
                    let () = self.handle_replay_message(replay_message)?;
                }
                default(SHUTDOWN_POLL_INTERVAL) => {},
            }
        }

        let Self {
            mut workers_shutdown_signal,
            entry_hash_verification_queue,
            entry_hash_verification_workers,
            ..
        } = self;

        // The response channel has room for every request in flight, so workers never block on
        // sending a response and will exit once they see the shutdown signal.
        workers_shutdown_signal.shutdown();
        drop(entry_hash_verification_queue);
        let () = entry_hash_verification_workers.join_workers();

        Ok(())
    }

    fn can_receive_replay_message(&self) -> bool {
        self.entry_hash_verification_queue.has_room_for_request()
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

        self.blocks_in_progress.push(BlockVerificationState::new(
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
            // The block already has its outcome
            return;
        };

        if block_verification_state.tracking_state != BlockTrackingState::InProgress {
            return;
        }

        let entry_verification_data = EntryVerificationData::from(&entry_view);

        // Every entry carries its own hash, which the next entry is verified against. So the
        // entries of a block are verified independently of each other.
        let start_hash = mem::replace(
            &mut block_verification_state.last_entry_hash,
            entry_verification_data.hash,
        );
        let progress_tracker = &mut block_verification_state.progress_tracker;

        progress_tracker.entry_hash_jobs_pending = progress_tracker
            .entry_hash_jobs_pending
            .checked_add(1)
            .expect("the jobs pending of a block are bounded by the request queue size");

        self.entry_hash_verification_queue
            .send(EntryHashVerificationRequest {
                bank_id,
                start_hash,
                entry_verification_data,
            });
    }

    fn handle_abort_message(
        &mut self,
        AbortMessage { bank_id }: AbortMessage,
    ) -> Result<(), DisconnectedResponseChannel> {
        let Some(mut block_verification_state) = self.take_block_verification_state(&bank_id)
        else {
            // The block completed or failed verification before the abort message arrived
            return Ok(());
        };

        // A block that already failed verification keeps the failure as its outcome
        if block_verification_state.tracking_state == BlockTrackingState::InProgress {
            block_verification_state.tracking_state = BlockTrackingState::Aborted;
        }

        self.try_release_block(block_verification_state)
    }

    fn handle_all_entries_submitted_message(
        &mut self,
        AllEntriesSubmittedMessage { bank_id }: AllEntriesSubmittedMessage,
    ) -> Result<(), DisconnectedResponseChannel> {
        let Some(mut block_verification_state) = self.take_block_verification_state(&bank_id)
        else {
            // The block already failed verification
            return Ok(());
        };

        block_verification_state
            .progress_tracker
            .all_entries_are_submitted = true;

        self.try_release_block(block_verification_state)
    }

    fn handle_entry_hash_verification_response(
        &mut self,
        EntryHashVerificationResponse { bank_id, verified }: EntryHashVerificationResponse,
    ) -> Result<(), DisconnectedResponseChannel> {
        self.entry_hash_verification_queue.complete_request();

        let mut block_verification_state = self
            .take_block_verification_state(&bank_id)
            .expect("a block is only released once none of its requests are in flight");

        let progress_tracker = &mut block_verification_state.progress_tracker;

        progress_tracker.entry_hash_jobs_pending = progress_tracker
            .entry_hash_jobs_pending
            .checked_sub(1)
            .expect("each response for a block answers a request counted as pending when sent");

        if !verified && block_verification_state.tracking_state == BlockTrackingState::InProgress {
            block_verification_state.tracking_state =
                BlockTrackingState::Invalidated(VerificationFailureReason::InvalidEntryHash);
        }

        self.try_release_block(block_verification_state)
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

    /// Sends the outcome of the block back to replay once it has one, and returns its state to the
    /// scheduler otherwise.
    fn try_release_block(
        &mut self,
        block_verification_state: BlockVerificationState,
    ) -> Result<(), DisconnectedResponseChannel> {
        let Some(verification_status) = block_verification_state.outcome() else {
            self.blocks_in_progress.push(block_verification_state);
            return Ok(());
        };

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
    bank_id: BankId,
    last_entry_hash: Hash,
    slot: Slot,
    tracking_state: BlockTrackingState,
    progress_tracker: BlockVerificationStatus,
}

impl BlockVerificationState {
    fn new(bank_id: BankId, parent_bank_last_entry_hash: Hash, slot: Slot) -> Self {
        Self {
            bank_id,
            last_entry_hash: parent_bank_last_entry_hash,
            slot,
            tracking_state: BlockTrackingState::InProgress,
            progress_tracker: BlockVerificationStatus::default(),
        }
    }

    /// The outcome of the block, once the block can be released.
    fn outcome(&self) -> Option<BlockVerificationOutcome> {
        if self.progress_tracker.has_jobs_pending() {
            return None;
        }

        match self.tracking_state {
            BlockTrackingState::InProgress => self
                .progress_tracker
                .all_entries_are_submitted
                .then_some(BlockVerificationOutcome::Verified),
            BlockTrackingState::Aborted => Some(BlockVerificationOutcome::Aborted),
            BlockTrackingState::Invalidated(failure_reason) => {
                Some(BlockVerificationOutcome::VerificationFailed(failure_reason))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockTrackingState {
    InProgress,
    Aborted,
    Invalidated(VerificationFailureReason),
}

struct DisconnectedResponseChannel;

#[derive(Default)]
struct BlockVerificationStatus {
    all_entries_are_submitted: bool,
    entry_hash_jobs_pending: usize,
}

impl BlockVerificationStatus {
    fn has_jobs_pending(&self) -> bool {
        self.entry_hash_jobs_pending > 0
    }
}
