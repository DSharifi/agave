use std::num::NonZeroUsize;

const DEFAULT_REPLAY_TO_BLOCK_VERIFICATION_CHANNEL_SIZE: usize = 2048;
const DEFAULT_BLOCK_VERIFICATION_TO_REPLAY_CHANNEL_SIZE: usize = 2048;
const DEFAULT_NUM_ENTRY_HASH_WORKERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
const DEFAULT_MAX_ENTRY_HASH_JOBS_IN_FLIGHT: usize = 4096;

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub replay_to_block_verification_channel_size: usize,
    pub block_verification_to_replay_channel_size: usize,
    /// The number of worker threads verifying entry hashes.
    pub num_entry_hash_workers: NonZeroUsize,
    /// The maximum number of entry hash verification jobs sent to the workers whose response the
    /// scheduler has not handled yet.
    pub max_entry_hash_jobs_in_flight: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            replay_to_block_verification_channel_size:
                DEFAULT_REPLAY_TO_BLOCK_VERIFICATION_CHANNEL_SIZE,
            block_verification_to_replay_channel_size:
                DEFAULT_BLOCK_VERIFICATION_TO_REPLAY_CHANNEL_SIZE,
            num_entry_hash_workers: DEFAULT_NUM_ENTRY_HASH_WORKERS,
            max_entry_hash_jobs_in_flight: DEFAULT_MAX_ENTRY_HASH_JOBS_IN_FLIGHT,
        }
    }
}
