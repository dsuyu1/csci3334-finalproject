//! The worker pool.
//!
//! Each worker owns one channel receiver. The dispatcher pushes a task
//! onto a worker's channel when it has chosen which worker should run
//! it. A worker shuts down when the dispatcher drops its sender, which
//! makes the channel `recv()` return `Err`.
//!
//! Why per-worker channels (a "push" model from the dispatcher) instead
//! of a shared receiver workers pull from?
//!   - Pull from a shared queue is simpler but it makes "reserve N
//!     workers for IO" or "send the next task to the least-loaded
//!     worker" awkward. The scheduling decision wants to *pick* a
//!     worker, not let workers race for tasks.
//!   - With a push model, the dispatcher's policy code is the only
//!     thing that decides where work goes. Workers stay dumb.

use crate::metrics::{Completion, WorkerBusy};
use crate::task::Task;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Handle returned by `spawn`. The dispatcher uses `sender` to push
/// tasks; dropping all senders signals shutdown to that worker.
pub struct WorkerHandle {
    pub id: usize,
    pub sender: Sender<Task>,
    pub join: JoinHandle<WorkerBusy>,
}

/// Spawn a single worker. The worker:
///  1. Receives a task on its channel.
///  2. Simulates running it (CPU = busy spin; IO = sleep).
///  3. Sends a Completion record to the metrics thread.
///  4. Loops until its channel closes.
///
/// `active_workers` is an atomic counter shared with the monitor thread;
/// the worker increments it when a task starts and decrements when done.
pub fn spawn(
    id: usize,
    metrics_tx: Sender<Completion>,
    active_workers: Arc<AtomicUsize>,
) -> WorkerHandle {
    let (task_tx, task_rx): (Sender<Task>, Receiver<Task>) = std::sync::mpsc::channel();

    let join = thread::Builder::new()
        .name(format!("worker-{id}"))
        .spawn(move || run_worker(id, task_rx, metrics_tx, active_workers))
        .expect("failed to spawn worker thread");

    WorkerHandle {
        id,
        sender: task_tx,
        join,
    }
}

fn run_worker(
    id: usize,
    task_rx: Receiver<Task>,
    metrics_tx: Sender<Completion>,
    active_workers: Arc<AtomicUsize>,
) -> WorkerBusy {
    let mut total_busy = Duration::ZERO;

    while let Ok(mut task) = task_rx.recv() {
        let start = Instant::now();
        task.start_time = Some(start);

        // Signal the monitor that this worker is now active.
        active_workers.fetch_add(1, Ordering::Relaxed);

        // Simulate the task. Real CPU vs. IO matters here:
        //   - CPU tasks burn the worker thread with a tight arithmetic
        //     loop. They contribute to load and can't be parallelized
        //     onto other workers.
        //   - IO tasks just sleep, freeing the OS scheduler to do other
        //     things (in real life this is where we'd be waiting on
        //     disk or network).
        match task.kind {
            crate::task::TaskKind::Cpu => simulate_cpu(task.duration),
            crate::task::TaskKind::Io => thread::sleep(task.duration),
        }

        // Signal the monitor that this worker is now idle.
        active_workers.fetch_sub(1, Ordering::Relaxed);

        let busy = start.elapsed();
        total_busy += busy;

        // wait = enqueue -> start; turnaround = enqueue -> finish.
        let wait = task
            .enqueue_time
            .map(|e| start.duration_since(e))
            .unwrap_or_default();
        let turnaround = wait + busy;

        let completion = Completion {
            id: task.id,
            kind: task.kind,
            worker_id: id,
            wait,
            turnaround,
            busy,
        };

        let _ = metrics_tx.send(completion);
    }

    WorkerBusy {
        worker_id: id,
        total_busy,
    }
}

/// Tight arithmetic loop calibrated to consume roughly `target` real
/// time. We can't just sleep, because that would model an IO task,
/// not a CPU one — the whole point of the CPU/IO distinction is that
/// CPU tasks actually compete for cores.
///
/// `black_box` prevents the optimizer from realizing the result is
/// thrown away and deleting the loop entirely.
fn simulate_cpu(target: Duration) {
    use std::hint::black_box;
    let start = Instant::now();
    let mut acc: u64 = 0xcbf2_9ce4_8422_2325;
    while start.elapsed() < target {
        for i in 0..5_000u64 {
            acc = acc.wrapping_mul(0x100000001b3).wrapping_add(i);
        }
        black_box(acc);
    }
}