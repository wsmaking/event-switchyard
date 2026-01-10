//! FastPath drain worker (drops orders without exchange).
//!
//! Used for throughput testing when no exchange is configured.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// キューから注文を取り出して破棄
pub fn start_worker(
    queue: Arc<gateway_core::FastPathQueue>,
    worker_id: usize,
) -> thread::JoinHandle<()> {
    thread::spawn(move || loop {
        if queue.pop().is_some() {
            // drop
        } else {
            thread::sleep(Duration::from_micros(100));
        }
        let _ = worker_id;
    })
}
