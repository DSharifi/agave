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

        let scheduler = Self::new(
            replay_to_block_verification.1,
            block_verification_to_replay.0,
            entry_hash_verification_request_producer,
            entry_hash_verification_response_receiver,
            shutdown_signal,
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
        entry_hash_verification_request_producer: mpmc::Producer<EntryHashVerificationRequest>,
        entry_hash_verification_response_receiver: Receiver<EntryHashVerificationResponse>,
        shutdown_signal: Arc<AtomicBool>,
        entry_hash_verification_workers: EntryHashVerificationWorkers,
    ) -> Self {
        Self {
            replay_message_receiver,
            replay_message_sender,
            entry_hash_verification_request_producer,
            entry_hash_verification_response_receiver,
            shutdown_signal,
            verification_session_id_generator: VerificationSessionIdGenerator::new(),
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

#[derive(Debug)]
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

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::messages::EntryMessage,
        bytes::Bytes,
        crossbeam_channel::{TryRecvError, never},
        solana_entry::entry::{
            EntryVerificationData, EntryView, entry_views_for_tests, next_entry_mut,
        },
        test_case::test_case,
    };

    const BOGUS_HASH: Hash = Hash::new_from_array([1; _]);
    const PARENT_BANK_LAST_ENTRY_HASH: Hash = Hash::new_from_array([2; _]);
    const BANK_ID: BankId = 123;
    const SLOT: Slot = 120;

    struct ManuallyDrivenScheduler {
        scheduler: BlockVerificationScheduler,
        entry_hash_verification_request_consumer: mpmc::Consumer<EntryHashVerificationRequest>,
        outcome_receiver: Receiver<BlockVerificationToReplayMessage>,
    }

    impl ManuallyDrivenScheduler {
        fn new() -> Self {
            let (
                entry_hash_verification_request_producer,
                entry_hash_verification_request_consumer,
            ) = mpmc::pair(64).unwrap();
            let (outcome_sender, outcome_receiver) = bounded(64);

            // The handlers are called directly, so nothing is ever received on these channels.
            let message_receiver = never();
            let entry_hash_verification_response_receiver = never();

            let scheduler = BlockVerificationScheduler::new(
                message_receiver,
                outcome_sender,
                entry_hash_verification_request_producer,
                entry_hash_verification_response_receiver,
                Arc::new(AtomicBool::new(false)),
                EntryHashVerificationWorkers::none(),
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
            self.scheduler.handle_replay_message(message).unwrap();
        }

        fn take_entry_hash_verification_requests(&mut self) -> Vec<EntryHashVerificationRequest> {
            std::iter::from_fn(|| self.entry_hash_verification_request_consumer.try_read())
                .collect()
        }

        fn verify_entry_hash_and_respond(&mut self, request: EntryHashVerificationRequest) {
            let verified =
                EntryVerificationData::from(&request.entry_view).verify(&request.start_hash);
            self.scheduler
                .handle_entry_hash_verification_response(EntryHashVerificationResponse {
                    block_verification_id: request.block_verification_id,
                    verified,
                })
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

    #[test]
    fn all_entries_submitted_does_not_complete_block_with_verifications_in_flight() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        // Only the first entry is invalid, as the chain does not follow the parent bank's hash.
        scheduler.begin_block(BANK_ID, SLOT, BOGUS_HASH);
        scheduler.submit_entries(BANK_ID, ticks_following(PARENT_BANK_LAST_ENTRY_HASH, 3));
        let [invalid_entry_request, second_request, third_request] = scheduler
            .take_entry_hash_verification_requests()
            .try_into()
            .unwrap();

        scheduler.verify_entry_hash_and_respond(third_request);
        scheduler.verify_entry_hash_and_respond(second_request);
        scheduler.notify_all_entries_submitted(BANK_ID);
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a block must not complete while verifications are in flight"
        );

        scheduler.verify_entry_hash_and_respond(invalid_entry_request);
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(BANK_ID, SLOT, BlockVerificationOutcome::VerificationFailed),
            "an invalid entry must fail the block regardless of response order"
        );
    }

    #[test]
    fn responses_in_flight_for_failed_block_are_ignored() {
        let mut scheduler = ManuallyDrivenScheduler::new();

        // Corrupting the hash of the first entry fails both its own verification and that of the
        // second entry, which is verified against the corrupted hash. The third entry is valid.
        let mut entries = ticks_following(PARENT_BANK_LAST_ENTRY_HASH, 3);
        entries[0].hash = BOGUS_HASH;

        scheduler.begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH);
        scheduler.submit_entries(BANK_ID, entries);
        let [
            first_invalid_entry_request,
            second_invalid_entry_request,
            valid_entry_request,
        ] = scheduler
            .take_entry_hash_verification_requests()
            .try_into()
            .unwrap();

        scheduler.verify_entry_hash_and_respond(first_invalid_entry_request);
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(BANK_ID, SLOT, BlockVerificationOutcome::VerificationFailed),
            "a block must fail without waiting for verifications in flight"
        );

        scheduler.verify_entry_hash_and_respond(second_invalid_entry_request);
        scheduler.verify_entry_hash_and_respond(valid_entry_request);
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "a failed block must not get another outcome from its remaining verifications"
        );
    }

    // The responses of jobs in flight for an aborted block arrive after a later block reusing its
    // bank id and slot has begun. Both a valid and an invalid response for the aborted block must
    // be ignored, rather than completing or failing the later block.
    #[test_case(PARENT_BANK_LAST_ENTRY_HASH, BOGUS_HASH, BlockVerificationOutcome::VerificationFailed; "valid_aborted_response")]
    #[test_case(BOGUS_HASH, PARENT_BANK_LAST_ENTRY_HASH, BlockVerificationOutcome::Verified; "invalid_aborted_response")]
    fn responses_of_aborted_block_are_ignored_by_later_block_reusing_its_bank_id(
        aborted_block_parent_bank_last_entry_hash: Hash,
        later_block_parent_bank_last_entry_hash: Hash,
        later_block_outcome: BlockVerificationOutcome,
    ) {
        let mut scheduler = ManuallyDrivenScheduler::new();

        scheduler.begin_block(BANK_ID, SLOT, aborted_block_parent_bank_last_entry_hash);
        scheduler.submit_entries(BANK_ID, ticks_following(PARENT_BANK_LAST_ENTRY_HASH, 1));
        scheduler.abort_block(BANK_ID);
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(BANK_ID, SLOT, BlockVerificationOutcome::Aborted),
            "an abort must not wait for verifications in flight"
        );
        let [aborted_block_request] = scheduler
            .take_entry_hash_verification_requests()
            .try_into()
            .unwrap();

        scheduler.begin_block(BANK_ID, SLOT, later_block_parent_bank_last_entry_hash);
        scheduler.submit_entries(BANK_ID, ticks_following(PARENT_BANK_LAST_ENTRY_HASH, 1));
        scheduler.notify_all_entries_submitted(BANK_ID);
        let [later_block_request] = scheduler
            .take_entry_hash_verification_requests()
            .try_into()
            .unwrap();

        scheduler.verify_entry_hash_and_respond(aborted_block_request);
        assert_eq!(
            scheduler.try_recv_outcome(),
            None,
            "responses for an aborted block must not affect a later block with the same bank id"
        );

        scheduler.verify_entry_hash_and_respond(later_block_request);
        assert_eq!(
            scheduler.try_recv_outcome(),
            outcome(BANK_ID, SLOT, later_block_outcome),
            "a block's outcome must depend only on its own entries"
        );
    }
}
