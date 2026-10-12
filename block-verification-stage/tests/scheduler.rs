use {
    crate::helpers::{
        BOGUS_HASH, PARENT_BANK_LAST_ENTRY_HASH, RECV_TIMEOUT, SLOT, TEST_SCHEDULER_CONFIG,
        entry_views_for_chain, recv_outcome, submit_entries,
    },
    block_verification_stage::{
        SchedulerConfig,
        messages::{BlockVerificationOutcome, BlockVerificationToReplayMessage},
        run_scheduler,
        stage::{
            Aborted, AllEntriesSubmitted, BlockVerificationSession, SchedulerShutdownError, Started,
        },
    },
    crossbeam_channel::RecvTimeoutError,
    helpers::BANK_ID,
    solana_clock::{BankId, Slot},
    std::{
        assert_matches,
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    },
    test_case::test_case,
};

pub mod helpers;

#[test]
fn block_with_no_entries_notifying_that_all_entries_are_submitted_passes_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn aborting_block_before_all_entries_submitted_sends_aborted_and_releases_bank_id_and_slot() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<Aborted> = session.abort_block_verification().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Aborted,
        }
    );

    // The bank id and slot are released once the block is aborted, so they can be reused.
    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// The block with no entries is verified as soon as all its entries are submitted, so the
// scheduler no longer tracks it when the abort arrives. The abort is then ignored, and only the verified
// outcome is sent.
#[test]
fn aborting_block_after_it_completed_is_ignored() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<Aborted> = session
        .notify_all_entries_submitted()
        .unwrap()
        .abort_block_verification()
        .unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    // The bank id and slot are released once the block is verified, so they can be reused. The
    // next message being for this block also shows no message was sent for the ignored abort.
    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn outcomes_of_concurrent_blocks_are_sent_in_completion_order() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    const FIRST_BANK_ID: BankId = 1;
    const FIRST_SLOT: Slot = 120;
    const SECOND_BANK_ID: BankId = 2;
    const SECOND_SLOT: Slot = 121;
    const THIRD_BANK_ID: BankId = 3;
    const THIRD_SLOT: Slot = 122;

    let first_session: BlockVerificationSession<Started> = stage
        .begin_block(FIRST_BANK_ID, FIRST_SLOT, BOGUS_HASH)
        .unwrap();
    let second_session: BlockVerificationSession<Started> = stage
        .begin_block(SECOND_BANK_ID, SECOND_SLOT, BOGUS_HASH)
        .unwrap();

    let _second_session: BlockVerificationSession<AllEntriesSubmitted> =
        second_session.notify_all_entries_submitted().unwrap();
    let _first_session: BlockVerificationSession<Aborted> =
        first_session.abort_block_verification().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: SECOND_SLOT,
            bank_id: SECOND_BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );
    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: FIRST_SLOT,
            bank_id: FIRST_BANK_ID,
            verification_status: BlockVerificationOutcome::Aborted,
        }
    );

    // Outcomes are sent in order, so a following block's outcome being next shows no other
    // message was sent for the two blocks.
    let third_session: BlockVerificationSession<Started> = stage
        .begin_block(THIRD_BANK_ID, THIRD_SLOT, BOGUS_HASH)
        .unwrap();
    let _third_session: BlockVerificationSession<AllEntriesSubmitted> =
        third_session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT)
            .unwrap(),
        BlockVerificationToReplayMessage {
            slot: THIRD_SLOT,
            bank_id: THIRD_BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// Beginning a block whose bank id or slot is still in progress violates a scheduler invariant,
// which panics the scheduler thread.
#[test_case(1, 120, 1, 121; "reused_bank_id")]
#[test_case(1, 120, 2, 120; "reused_slot")]
#[test_case(1, 120, 1, 120; "reused_slot_and_bank_id")]
#[should_panic = "internal invariant violated. A duplicate"]
fn reusing_bank_id_or_slot_in_progress_panics_scheduler(
    first_bank_id: BankId,
    first_slot: Slot,
    second_bank_id: BankId,
    second_slot: Slot,
) {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    let _first_session: BlockVerificationSession<Started> = stage
        .begin_block(first_bank_id, first_slot, BOGUS_HASH)
        .unwrap();

    let _second_session: BlockVerificationSession<Started> = stage
        .begin_block(second_bank_id, second_slot, BOGUS_HASH)
        .unwrap();

    // The panic drops the scheduler's sender for outcomes, disconnecting the receiver.
    assert_matches!(
        stage
            .scheduler_message_receiver()
            .recv_timeout(RECV_TIMEOUT),
        Err(RecvTimeoutError::Disconnected)
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    stage
        .join()
        .expect_err("the scheduler thread panicked due to a reuse of slot/bank");
}

#[test]
fn shutdown_stops_scheduler_with_block_in_progress() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();

    // The session still holds a sender, so only the shutdown signal stops the scheduler.
    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));

    assert_matches!(
        session.notify_all_entries_submitted(),
        Err(SchedulerShutdownError)
    );
}

#[test]
fn block_with_valid_entries_passes_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    // More entries than jobs can be in flight, so replay messages are received in several rounds
    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(
        &session,
        entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, 100, 3),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn block_not_following_parent_bank_last_entry_hash_fails_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();

    assert_ne!(
        BOGUS_HASH, PARENT_BANK_LAST_ENTRY_HASH,
        "sanity check for test failed. these hashes must not be the same for this test scenario"
    );

    submit_entries(
        &session,
        entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, 10, 3),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

#[test]
fn block_with_invalid_entry_fails_verification_before_all_entries_are_submitted() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(TEST_SCHEDULER_CONFIG, shutdown_flag.clone());

    const SECOND_BANK_ID: BankId = BANK_ID + 1;
    const SECOND_SLOT: Slot = SLOT + 1;

    let mut entry_views = entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, 10, 3);
    entry_views[5].hash = BOGUS_HASH;
    let entry_views_after_failure = entry_views.split_off(8);

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    submit_entries(&session, entry_views);

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed,
        }
    );

    // Entries submitted after the block failed verification are ignored, as is the
    // notification that all its entries are submitted.
    submit_entries(&session, entry_views_after_failure);
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    // Outcomes are sent in order, so a following block's outcome being next shows no other
    // message was sent for the failed block.
    let second_session: BlockVerificationSession<Started> = stage
        .begin_block(SECOND_BANK_ID, SECOND_SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();
    let _second_session: BlockVerificationSession<AllEntriesSubmitted> =
        second_session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SECOND_SLOT,
            bank_id: SECOND_BANK_ID,
            verification_status: BlockVerificationOutcome::Verified,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}

// TODO: This test is prone to race conditions. One potential fix is that the scheduler injects
// the channels instead of the scheduler constructing the worker and creating channel handlers.
// That way we can drive the actual completions of it. Maybe using a mock here would let us also control
// the flow and expect what's happening.
//
//
// The responses of jobs still in flight for an aborted block must not be counted towards a later
// block reusing its bank id. With a single worker, the jobs of the aborted block run before the
// job of the later block, so their responses arrive while the later block is in progress.
#[test]
fn aborting_block_with_entries_in_flight_ignores_their_verification() {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let stage = run_scheduler(
        SchedulerConfig {
            num_entry_hash_workers: NonZeroUsize::new(1).unwrap(),
            ..TEST_SCHEDULER_CONFIG
        },
        shutdown_flag.clone(),
    );

    let session: BlockVerificationSession<Started> = stage
        .begin_block(BANK_ID, SLOT, PARENT_BANK_LAST_ENTRY_HASH)
        .unwrap();

    submit_entries(
        &session,
        entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, 32, 1000),
    );
    let _session: BlockVerificationSession<Aborted> = session.abort_block_verification().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::Aborted,
        }
    );

    // The only entry of the later block fails verification, which a successful response of the
    // aborted block would otherwise hide by completing the later block first.
    let session: BlockVerificationSession<Started> =
        stage.begin_block(BANK_ID, SLOT, BOGUS_HASH).unwrap();
    submit_entries(
        &session,
        entry_views_for_chain(PARENT_BANK_LAST_ENTRY_HASH, 1, 3),
    );
    let _session: BlockVerificationSession<AllEntriesSubmitted> =
        session.notify_all_entries_submitted().unwrap();

    assert_eq!(
        recv_outcome(&stage),
        BlockVerificationToReplayMessage {
            slot: SLOT,
            bank_id: BANK_ID,
            verification_status: BlockVerificationOutcome::VerificationFailed,
        }
    );

    shutdown_flag.store(true, Ordering::Relaxed);
    assert_matches!(stage.join(), Ok(()));
}
