use {
    crate::{
        join_thread,
        messages::{EntryHashVerificationRequest, EntryHashVerificationResponse},
    },
    crossbeam_channel::Sender,
    shaq::{error::WaitError, mpmc},
    solana_entry::entry::EntryVerificationData,
    std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::Duration,
    },
};

/// The timeout used on waiting for requests before checking whether to exit
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) struct EntryHashVerificationWorkers {
    worker_handles: Vec<JoinHandle<()>>,
}

impl EntryHashVerificationWorkers {
    /// Spawns `num_workers` threads, which drop their response senders once they exit.
    pub(crate) fn spawn(
        shutdown_signal: Arc<AtomicBool>,
        num_workers: NonZeroUsize,
        request_consumer: mpmc::Consumer<EntryHashVerificationRequest>,
        response_sender: Sender<EntryHashVerificationResponse>,
    ) -> Self {
        let worker_handles = (0..num_workers.get())
            .map(|index| {
                let request_consumer = request_consumer.clone();
                let response_sender = response_sender.clone();
                let exit = shutdown_signal.clone();
                thread::Builder::new()
                    .name(format!("solBlkVerHash{index}"))
                    .spawn(move || run_worker(&request_consumer, &response_sender, &exit))
                    .expect("thread can be spawned")
            })
            .collect();

        Self { worker_handles }
    }

    /// No worker threads, for tests that act as the workers themselves.
    #[cfg(test)]
    pub(crate) fn none() -> Self {
        Self {
            worker_handles: Vec::new(),
        }
    }

    pub(crate) fn join_workers(self) {
        for worker_handle in self.worker_handles {
            let () = join_thread(worker_handle);
        }
    }
}

fn run_worker(
    request_consumer: &mpmc::Consumer<EntryHashVerificationRequest>,
    response_sender: &Sender<EntryHashVerificationResponse>,
    shutdown_signal: &AtomicBool,
) {
    while !shutdown_signal.load(Ordering::Relaxed) {
        let verification_request = match request_consumer.read_timeout(EXIT_POLL_INTERVAL) {
            Ok(request) => request,
            Err(WaitError::Timeout) => continue,
        };

        let EntryHashVerificationRequest {
            block_verification_id,
            start_hash,
            entry_view,
        } = verification_request;

        let verified = EntryVerificationData::from(&entry_view).verify(&start_hash);

        let verification_response = EntryHashVerificationResponse {
            block_verification_id,
            verified,
        };

        let send_result = response_sender.send(verification_response);

        match send_result {
            Ok(()) => {}
            Err(crossbeam_channel::SendError(_)) => {
                return;
            }
        }
    }
}
