pub use {config::SchedulerConfig, stage::BlockVerificationStage};
use {
    solana_runtime::bank_forks::BankForks,
    std::{
        panic::resume_unwind,
        sync::{Arc, RwLock, atomic::AtomicBool},
        thread::JoinHandle,
    },
};

mod config;
mod scheduler;
pub(crate) mod utils;
mod workers;

pub mod messages;
pub mod stage;

pub fn run_scheduler(
    scheduler_config: SchedulerConfig,
    shutdown_signal: Arc<AtomicBool>,
    bank_forks: Arc<RwLock<BankForks>>,
) -> BlockVerificationStage {
    scheduler::BlockVerificationScheduler::run_scheduler(
        scheduler_config,
        shutdown_signal,
        bank_forks,
    )
}

pub(crate) fn join_thread<T>(join_handle: JoinHandle<T>) -> T {
    match join_handle.join() {
        Ok(exit_reason) => exit_reason,
        // propagate panic
        Err(error) => resume_unwind(error),
    }
}
