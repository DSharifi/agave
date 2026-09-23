//! Replay-facing API of the block verification stage.
//!
//! A [`BlockVerificationSession`] tracks one block through the following type states:
//!
//! ```text
//!   BlockVerificationStage::begin_block(bank_id, slot, parent_hash)
//!          │
//!          │ Ok(session)
//!          ▼
//!   ┌─────────────┐  submit_entry(entry)
//!   │   Started   │◄─────────┐
//!   │             │──────────┘
//!   └─────────────┘
//!     │    │
//!     │    │ notify_all_entries_submitted()
//!     │    ▼
//!     │  ┌─────────────────────┐
//!     │  │ AllEntriesSubmitted │── Verified | VerificationFailed ──►┐
//!     │  └─────────────────────┘                                    │
//!     │    │                                                        │
//!     ├────┘ abort_block_verification()                             │
//!     │                                                             │
//!     ▼                                                             │
//!   ┌─────────────┐                                                 │
//!   │   Aborted   │── Aborted ─────────────────────────────────────►┤
//!   └─────────────┘                                                 │
//!                                      scheduler_message_receiver() ▼
//!                                                                replay
//!
//!   Outcome, sent once per block on BlockVerificationStage::scheduler_message_receiver()
//!   as BlockVerificationToReplayMessage { slot, bank_id, verification_status }:
//!     Verified | VerificationFailed  ──► all entries submitted and verification finished
//!     Aborted                        ──► aborted before verification finished
//!   An abort arriving after the outcome was sent is ignored.
//!
//!   Every transition fails with an error if the scheduler has shut down.
//!
//!   Drop:
//!     Started, AllEntriesSubmitted  ──► sends Abort
//!     Aborted                       ──► nothing
//! ```

use {
    crate::{
        messages::{
            AbortMessage, AllEntriesSubmittedMessage, BeginMessage,
            BlockVerificationToReplayMessage, EntryMessage, ReplayToBlockVerificationMessage,
        },
        utils::cancellation_token::CancellationToken,
    },
    bytes::Bytes,
    crossbeam_channel::{Receiver, SendError, Sender},
    solana_clock::{BankId, Slot},
    solana_entry::entry::EntryView,
    solana_hash::Hash,
    std::{marker::PhantomData, thread::JoinHandle},
};

/// A type-state used over [`BlockVerificationSession<Started>`] representing the state
/// where verification of the block has started.
///
/// In this state entries can be added with [`BlockVerificationSession::submit_entry`] until
/// the session is either aborted with [`BlockVerificationSession::abort_block_verification`]
/// or all its entries are submitted with
/// [`BlockVerificationSession::notify_all_entries_submitted`], which transitions it to the
/// [`Aborted`] or [`AllEntriesSubmitted`] type state respectively.
#[derive(Debug)]
pub struct Started;

/// A type-state used over [`BlockVerificationSession<AllEntriesSubmitted>`] representing the state
/// where replay has submitted every entry of the block. Verification may still be running.
///
/// In this state the [`BlockVerificationSession`] can be aborted with
/// [`BlockVerificationSession::abort_block_verification`]. Once verification finishes, the
/// outcome is sent on [`BlockVerificationStage::scheduler_message_receiver`].
#[derive(Debug)]
pub struct AllEntriesSubmitted;

/// A type-state used over [`BlockVerificationSession<Aborted>`] representing the state
/// where the verification request has been aborted.
///
/// This state is terminal. Once the scheduler has released the block,
/// [`BlockVerificationOutcome::Aborted`] is sent on
/// [`BlockVerificationStage::scheduler_message_receiver`], unless the outcome of the
/// verification was already sent.
///
/// [`BlockVerificationOutcome::Aborted`]: crate::messages::BlockVerificationOutcome::Aborted
#[derive(Debug)]
pub struct Aborted;

/// A state that can request abortion to the scheduler with [`BlockVerificationSession::abort_block_verification`].
pub trait AbortBlockVerification {}
impl AbortBlockVerification for Started {}
impl AbortBlockVerification for AllEntriesSubmitted {}

/// The block verification stage, which owns the scheduler thread.
///
/// Replay calls [`BlockVerificationStage::begin_block`] to start verifying a block, which
/// returns a [`BlockVerificationSession`] for that block. Dropping the stage stops the
/// scheduler.
#[derive(Debug)]
pub struct BlockVerificationStage {
    replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
    replay_message_receiver: Receiver<BlockVerificationToReplayMessage>,
    /// Signals the scheduler thread to exit when cancelled or dropped.
    shutdown_token: CancellationToken,
    scheduler_thread_join_handle: JoinHandle<()>,
}

impl BlockVerificationStage {
    pub(crate) fn new(
        replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
        replay_message_receiver: Receiver<BlockVerificationToReplayMessage>,
        shutdown_token: CancellationToken,
        scheduler_thread_join_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            replay_message_sender,
            replay_message_receiver,
            shutdown_token,
            scheduler_thread_join_handle,
        }
    }

    /// A channel receiver where the [`BlockVerificationToReplayMessage`] outcome for all
    /// scheduled blocks through this block verification stage handle via
    /// [`BlockVerificationStage::begin_block`] / [`BlockVerificationSession`] are sent back.
    pub fn scheduler_message_receiver(&self) -> &Receiver<BlockVerificationToReplayMessage> {
        &self.replay_message_receiver
    }

    /// Stops the scheduler and waits for its thread to exit.
    ///
    /// Blocks still in progress are dropped without an outcome being sent for them. Dropping the
    /// stage also stops the scheduler, but without waiting for the thread.
    pub fn shutdown(self) -> std::thread::Result<()> {
        self.shutdown_token.cancel();
        self.scheduler_thread_join_handle.join()
    }

    /// Starts verifying the block for `bank_id` at `slot`, returning a session to submit its
    /// entries to.
    ///
    /// Fails if the scheduler has shut down. The outcome of the block is sent on
    /// [`BlockVerificationStage::scheduler_message_receiver`].
    ///
    /// ### NB!
    /// `bank_id` and `slot` must not be used by another in-progress block. Violating this
    /// panics the scheduler thread and shuts it down.
    pub fn begin_block(
        &self,
        bank_id: BankId,
        slot: Slot,
        parent_bank_last_entry_hash: Hash,
    ) -> Result<BlockVerificationSession<Started>, BeginBlockError> {
        let begin_message = ReplayToBlockVerificationMessage::Begin(BeginMessage {
            bank_id,
            parent_bank_last_entry_hash,
            slot,
        });

        self.replay_message_sender
            .send(begin_message)
            .map_err(|_err: SendError<_>| BeginBlockError::SchedulerShutDown)?;

        Ok(BlockVerificationSession::new(
            bank_id,
            self.replay_message_sender.clone(),
        ))
    }
}

/// The verification of a single block, from [`BlockVerificationStage::begin_block`] until
/// its outcome is received.
///
/// `SchedulingState` is one of [`Started`], [`AllEntriesSubmitted`] or [`Aborted`]. Dropping
/// the [`BlockVerificationSession`] aborts verification of the block.
#[derive(Debug)]
pub struct BlockVerificationSession<SchedulingState> {
    sender: abort_on_drop::AbortOnDropSender,
    scheduling_state: PhantomData<fn() -> SchedulingState>,
}

impl<SchedulingState> BlockVerificationSession<SchedulingState>
where
    SchedulingState: AbortBlockVerification,
{
    /// Aborts verification of the block.
    pub fn abort_block_verification(
        self,
    ) -> Result<BlockVerificationSession<Aborted>, SchedulerShutdownError> {
        let mut sender = self.sender;
        sender.send_abort()?;

        Ok(BlockVerificationSession::<Aborted> {
            sender,
            scheduling_state: PhantomData,
        })
    }
}

impl BlockVerificationSession<Started> {
    pub(crate) fn new(
        bank_id: BankId,
        replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
    ) -> Self {
        Self {
            sender: abort_on_drop::AbortOnDropSender::new(bank_id, replay_message_sender),
            scheduling_state: PhantomData,
        }
    }

    pub fn submit_entry(&self, entry_view: EntryView<Bytes>) -> Result<(), SchedulerShutdownError> {
        self.sender.send_entry(entry_view)
    }

    /// Tells the scheduler that every entry of the block has been submitted.
    pub fn notify_all_entries_submitted(
        self,
    ) -> Result<BlockVerificationSession<AllEntriesSubmitted>, SchedulerShutdownError> {
        self.sender.send_all_entries_submitted()?;

        Ok(BlockVerificationSession::<AllEntriesSubmitted> {
            sender: self.sender,
            scheduling_state: PhantomData,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BeginBlockError {
    SchedulerShutDown,
    BankIdOccupied,
    SlotIdOccupied,
}

#[derive(Debug, PartialEq, Eq)]
pub struct SchedulerShutdownError;

mod abort_on_drop {
    use super::*;

    /// Sends messages for one block to the scheduler.
    ///
    /// Sends `Abort` for `bank_id` when dropped unless disarmed.
    #[derive(Debug)]
    pub(super) struct AbortOnDropSender {
        bank_id: BankId,
        replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
        armed: bool,
    }

    impl AbortOnDropSender {
        pub(super) fn new(
            bank_id: BankId,
            replay_message_sender: Sender<ReplayToBlockVerificationMessage>,
        ) -> Self {
            Self {
                bank_id,
                replay_message_sender,
                armed: true,
            }
        }

        pub(super) fn send_entry(
            &self,
            entry_view: EntryView<Bytes>,
        ) -> Result<(), SchedulerShutdownError> {
            self.send(ReplayToBlockVerificationMessage::Entry(EntryMessage {
                bank_id: self.bank_id,
                entry_view,
            }))
        }

        pub(super) fn send_all_entries_submitted(&self) -> Result<(), SchedulerShutdownError> {
            self.send(ReplayToBlockVerificationMessage::AllEntriesSubmitted(
                AllEntriesSubmittedMessage {
                    bank_id: self.bank_id,
                },
            ))
        }

        /// Sends `Abort` and disarms, so no second `Abort` is sent on drop.
        pub(super) fn send_abort(&mut self) -> Result<(), SchedulerShutdownError> {
            self.disarm();
            self.send(ReplayToBlockVerificationMessage::Abort(AbortMessage {
                bank_id: self.bank_id,
            }))?;

            Ok(())
        }

        /// Stops `Abort` from being sent on drop.
        pub(super) fn disarm(&mut self) {
            self.armed = false;
        }

        fn send(
            &self,
            message: ReplayToBlockVerificationMessage,
        ) -> Result<(), SchedulerShutdownError> {
            self.replay_message_sender
                .send(message)
                .map_err(|_err: SendError<_>| SchedulerShutdownError)
        }
    }

    impl Drop for AbortOnDropSender {
        fn drop(&mut self) {
            if self.armed {
                // An abort for a block the scheduler no longer tracks is ignored.
                let _ = self.send_abort();
            }
        }
    }
}
