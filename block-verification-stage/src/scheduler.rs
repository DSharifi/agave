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

#[derive(Debug)]
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        bytes::Bytes,
        crossbeam_channel::TryRecvError,
        shaq::mpmc,
        solana_entry::entry::{EntryView, entry_views_for_tests, next_entry_mut},
        std::num::NonZeroUsize,
        test_case::test_case,
    };

    const BOGUS_HASH: Hash = Hash::new_from_array([1; _]);
    const PARENT_BANK_LAST_ENTRY_HASH: Hash = Hash::new_from_array([2; _]);
    const BANK_ID: BankId = 123;
    const SLOT: Slot = 120;
    const ENTRY_HASH_VERIFICATION_QUEUE_SIZE: usize = 8;
    const OUTCOME_CHANNEL_SIZE: usize = 64;

    struct ManuallyDrivenScheduler {
        scheduler: BlockVerificationScheduler,
        entry_hash_verification_request_consumer: mpmc::Consumer<EntryHashVerificationRequest>,
        outcome_receiver: Receiver<BlockVerificationToReplayMessage>,
    }

    impl ManuallyDrivenScheduler {
        fn new() -> Self {
            // The handlers are called directly, so nothing is ever received on the replay message
            // channel, and the response sender is not needed.
            let (entry_hash_verification_queue, entry_hash_verification_request_consumer, _) =
                VerificationQueue::new(
                    NonZeroUsize::new(ENTRY_HASH_VERIFICATION_QUEUE_SIZE).unwrap(),
                );
            let (outcome_sender, outcome_receiver) = bounded(OUTCOME_CHANNEL_SIZE);
            let message_receiver = never();

            let scheduler = BlockVerificationScheduler::new(
                message_receiver,
                outcome_sender,
                entry_hash_verification_queue,
                Arc::new(AtomicBool::new(false)),
                ShutdownOnDrop::from(Arc::new(AtomicBool::new(false))),
                VerificationWorkers::none(),
            );

            Self {
                scheduler,
                entry_hash_verification_request_consumer,
                outcome_receiver,
            }
        }

        fn begin_block(&mut self, bank_id: BankId, slot: Slot, parent_bank_last_entry_hash: Hash) {
            self.handle_message(ReplayToBlockVerificationMessage::Begin(BeginMessage {
                bank_id,
                parent_bank_last_entry_hash,
                slot,
            }));
        }

        fn submit_entries(&mut self, bank_id: BankId, entry_views: Vec<EntryView<Bytes>>) {
            for entry_view in entry_views {
                self.handle_message(ReplayToBlockVerificationMessage::Entry(EntryMessage {
                    bank_id,
                    entry_view,
                }));
            }
        }

        fn notify_all_entries_submitted(&mut self, bank_id: BankId) {
            self.handle_message(ReplayToBlockVerificationMessage::AllEntriesSubmitted(
                AllEntriesSubmittedMessage { bank_id },
            ));
        }

        fn abort_block(&mut self, bank_id: BankId) {
            self.handle_message(ReplayToBlockVerificationMessage::Abort(AbortMessage {
                bank_id,
            }));
        }

        fn handle_message(&mut self, message: ReplayToBlockVerificationMessage) {
            assert!(
                self.scheduler.can_receive_replay_message(),
                "the event loop does not receive replay messages while the request queue is full"
            );
            self.scheduler.handle_replay_message(message).unwrap();
        }

        fn take_entry_hash_verification_requests(&mut self) -> Vec<EntryHashVerificationRequest> {
            std::iter::from_fn(|| self.entry_hash_verification_request_consumer.try_read())
                .collect()
        }

        fn verify_entry_hash_and_respond(&mut self, request: EntryHashVerificationRequest) {
            self.scheduler
                .handle_entry_hash_verification_response(
                    entry_hash_verification::verify_entry_hash(request),
                )
                .unwrap();
        }

        fn try_recv_outcome(&self) -> Option<BlockVerificationToReplayMessage> {
            match self.outcome_receiver.try_recv() {
                Ok(outcome) => Some(outcome),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    panic!("the outcome sender is owned by the scheduler")
                }
            }
        }

        fn requests_in_flight(&self) -> usize {
            self.scheduler
                .entry_hash_verification_queue
                .requests_in_flight()
        }
    }

    fn ticks_following(start_hash: Hash, num_ticks: usize) -> Vec<EntryView<Bytes>> {
        let mut hash = start_hash;
        let ticks = (0..num_ticks)
            .map(|_| next_entry_mut(&mut hash, 1, vec![]))
            .collect();
        entry_views_for_tests(ticks)
    }

    fn outcome(
        bank_id: BankId,
        slot: Slot,
        verification_status: BlockVerificationOutcome,
    ) -> Option<BlockVerificationToReplayMessage> {
        Some(BlockVerificationToReplayMessage {
            slot,
            bank_id,
            verification_status,
        })
    }

    #[derive(Debug)]
    enum LastStep {
        AllEntriesSubmitted,
        LastResponse,
    }

    #[test_case(LastStep::AllEntriesSubmitted; "all_entries_submitted_last")]
    #[test_case(LastStep::LastResponse; "last_response_last")]
    fn block_is_verified_once_all_entries_are_submitted_and_verified(last_step: LastStep) {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, ticks_following(PARENT_BANK_LAST_ENTRY_HASH, 3));
        let [first_request, second_request, third_request] = scheduler
            .take_entry_hash_verification_requests()
            .try_into()
            .unwrap();
        scheduler.verify_entry_hash_and_respond(third_request);
        scheduler.verify_entry_hash_and_respond(second_request);

        match last_step {
            LastStep::AllEntriesSubmitted => {
                scheduler.verify_entry_hash_and_respond(first_request);
                assert_eq!(
                    scheduler.try_recv_outcome(),
                    None,
                    "a block must not complete before all its entries are submitted"
                );
                scheduler.notify_all_entries_submitted(BANK_ID);
            }
            LastStep::LastResponse => {
                scheduler.notify_all_entries_submitted(BANK_ID);
                assert_eq!(
                    scheduler.try_recv_outcome(),
                    None,
                    "a block must not complete while verifications are in flight"
                );
                scheduler.verify_entry_hash_and_respond(first_request);
            }
        }
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(BANK_ID, SLOT, BlockVerificationOutcome::Verified)
        );
    }

    #[derive(Debug)]
    enum FinishBlock {
        Abort,
        FailVerification,
    }

    #[test_case(FinishBlock::Abort; "aborted")]
    #[test_case(FinishBlock::FailVerification; "failed")]
    fn finished_block_is_released_once_its_requests_in_flight_are_answered(
        finish_block: FinishBlock,
    ) {
        let mut scheduler = ManuallyDrivenScheduler::new();

        // Corrupting the hash of the second entry fails both its own verification and that of the
        // third entry, which is verified against the corrupted hash. The first entry is valid.
        let mut entries = ticks_following(PARENT_BANK_LAST_ENTRY_HASH, 4);
        let entry_submitted_after_failure = entries.pop().unwrap();
        entries[1].hash = BOGUS_HASH;

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, entries);
        let mut requests_in_flight = scheduler.take_entry_hash_verification_requests();

        let finished_block_outcome = match finish_block {
            FinishBlock::Abort => {
                scheduler.abort_block(BANK_ID);
                BlockVerificationOutcome::Aborted
            }
            FinishBlock::FailVerification => {
                let first_invalid_entry_request = requests_in_flight.remove(1);
                scheduler.verify_entry_hash_and_respond(first_invalid_entry_request);

                scheduler.submit_entries(BANK_ID, vec![entry_submitted_after_failure]);
                assert!(
                    scheduler.take_entry_hash_verification_requests().is_empty(),
                    "the entries of a block that failed verification must not be verified"
                );
                BlockVerificationOutcome::VerificationFailed(
                    VerificationFailureReason::InvalidEntryHash,
                )
            }
        };

        // The last request in flight is for an invalid entry, which must not change the outcome.
        let last_request = requests_in_flight.pop().unwrap();
        for request in requests_in_flight {
            scheduler.verify_entry_hash_and_respond(request);
        }
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not be released while requests for it are in flight"
        );

        scheduler.verify_entry_hash_and_respond(last_request);
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(BANK_ID, SLOT, finished_block_outcome),
            "a block must be released once the last request for it is answered"
        );
        assert_eq!(scheduler.requests_in_flight(), 0);

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, ticks_following(PARENT_BANK_LAST_ENTRY_HASH, 1));
        scheduler.notify_all_entries_submitted(BANK_ID);
        for request in scheduler.take_entry_hash_verification_requests() {
            scheduler.verify_entry_hash_and_respond(request);
        }
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(BANK_ID, SLOT, BlockVerificationOutcome::Verified),
            "a later block must be able to reuse the bank id and slot of a released block"
        );
    }

    #[test]
    fn aborting_block_that_failed_verification_keeps_the_failure_as_its_outcome() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        // Both entries fail verification, the second against the corrupted hash of the first.
        let mut entries = ticks_following(PARENT_BANK_LAST_ENTRY_HASH, 2);
        entries[0].hash = BOGUS_HASH;

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, entries);
        let [first_request, second_request] = scheduler
            .take_entry_hash_verification_requests()
            .try_into()
            .unwrap();
        scheduler.verify_entry_hash_and_respond(first_request);
        scheduler.abort_block(BANK_ID);
        assert_eq!(scheduler.try_recv_outcome(), None);

        scheduler.verify_entry_hash_and_respond(second_request);
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(
                BANK_ID,
                SLOT,
                BlockVerificationOutcome::VerificationFailed(
                    VerificationFailureReason::InvalidEntryHash
                )
            )
        );
    }

    #[test]
    fn replay_messages_are_not_received_while_requests_in_flight_fill_the_request_queue() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        let mut entries = ticks_following(
            PARENT_BANK_LAST_ENTRY_HASH,
            ENTRY_HASH_VERIFICATION_QUEUE_SIZE + 1,
        );
        let last_entry = entries.pop().unwrap();

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, entries);
        assert!(
            !scheduler.scheduler.can_receive_replay_message(),
            "replay messages must not be received while the request queue is full"
        );

        let mut requests = scheduler.take_entry_hash_verification_requests();
        assert!(
            !scheduler.scheduler.can_receive_replay_message(),
            "a request taken by a worker must stay in flight until the worker responds"
        );

        scheduler.verify_entry_hash_and_respond(requests.pop().unwrap());
        assert!(
            scheduler.scheduler.can_receive_replay_message(),
            "a response must free up room for a request"
        );

        scheduler.submit_entries(BANK_ID, vec![last_entry]);
        for request in requests
            .into_iter()
            .chain(scheduler.take_entry_hash_verification_requests())
        {
            scheduler.verify_entry_hash_and_respond(request);
        }
        scheduler.notify_all_entries_submitted(BANK_ID);
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(BANK_ID, SLOT, BlockVerificationOutcome::Verified)
        );
    }
}
