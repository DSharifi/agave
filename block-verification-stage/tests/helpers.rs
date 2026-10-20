use {
    block_verification_stage::SchedulerConfig,
    solana_clock::{BankId, Slot},
    solana_hash::Hash,
    std::time::Duration,
};

pub const TEST_SCHEDULER_CONFIG: SchedulerConfig = SchedulerConfig {
    replay_to_block_verification_channel_size: 10,
    block_verification_to_replay_channel_size: 10,
};
pub const BOGUS_HASH: Hash = Hash::new_from_array([1; _]);
pub const BANK_ID: BankId = 123;
pub const SLOT: Slot = 120;
pub const RECV_TIMEOUT: Duration = Duration::from_secs(5);
