pub use {config::SchedulerConfig, stage::BlockVerificationStage};

mod config;
mod scheduler;

pub mod messages;
pub mod stage;

pub fn run_scheduler(scheduler_config: SchedulerConfig) -> BlockVerificationStage {
    scheduler::BlockVerificationScheduler::run_scheduler(scheduler_config)
}
