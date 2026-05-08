//! The dispatcher: where the scheduling policy lives.
//!=//! Threads in this system:
//!   - 1 generator thread: walks the prebuilt task list and pushes each
//!     task into the appropriate ready queue when its arrival time
//!     comes up.
//!   - 1 dispatcher thread: this file. Picks tasks off the ready queues
//!     and routes them to specific workers per the policy.
//!   - N worker threads: simulate execution.
//!   - 1 metrics thread: collects Completion records.
//!
//! Why a separate dispatcher thread instead of letting workers pull
//! from a shared queue?
//!   - The policy considers BOTH queues when deciding what runs next
//!     (weighted dispatch, IO reservation, aging). That's a global
//!     decision and belongs to one component.
//!   - Workers stay simple: they just execute whatever they're handed.
//!
//! Channels vs. shared state in this design:
//!   - Channels handle one-way handoffs of ownership: generator->queues
//!     (well, via a method call that uses a Mutex internally), and
//!     dispatcher->worker.
//!   - The shared state (the dual queues) is behind a Mutex+Condvar
//!     because the dispatcher needs to *examine* the state, not just
//!     receive a value. A channel can't express "tell me whether queue
//!     A has older tasks than queue B."

use crate::queues::ReadyQueues;
use crate::task::{Task, TaskKind};
use crate::worker::WorkerHandle;
use std::sync::Arc;
use std::sync::mpsc::{SendError, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Configurable knobs for the scheduling policy. Defaults give a
/// reasonable mix; experiments override them to demonstrate behavior.
#[derive(Debug, Clone, Copy)]
pub struct PolicyConfig {
    /// Number of workers reserved exclusively for IO tasks. Prevents
    /// a flood of CPU work from fully occupying the pool and starving
    /// IO. The remaining workers can serve either kind.
    pub io_reserved_workers: usize,

    /// CPU vs. IO weight when both queues are non-empty. The dispatcher
    /// picks CPU `cpu_weight` times in a row, then IO `io_weight` times,
    /// then repeats. Set both to 1 for plain alternation.
    pub cpu_weight: u32,
    pub io_weight: u32,

    /// If the head of a queue has been waiting longer than this, the
    /// dispatcher drops the weighting and serves that queue immediately.
    /// This is the "aging" mechanism that prevents starvation if one
    /// class somehow dominates the dispatch decision.
    pub aging_threshold_ms: u64,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            io_reserved_workers: 1,
            cpu_weight: 2,
            io_weight: 1,
            aging_threshold_ms: 200,
        }
    }
}

/// Run the dispatcher to completion. Returns its JoinHandle so main
/// can wait for shutdown.
pub fn spawn(
    queues: Arc<ReadyQueues>,
    workers: Vec<WorkerHandle>,
    policy: PolicyConfig,
) -> JoinHandle<Vec<crate::metrics::WorkerBusy>> {
    thread::Builder::new()
        .name("dispatcher".into())
        .spawn(move || run_dispatcher(queues, workers, policy))
        .expect("failed to spawn dispatcher thread")
}

fn run_dispatcher(
    queues: Arc<ReadyQueues>,
    workers: Vec<WorkerHandle>,
    policy: PolicyConfig,
) -> Vec<crate::metrics::WorkerBusy> {
    // Split workers into two pools:
    //   - reserved IO workers: only handle IO tasks
    //   - general workers: handle whatever the policy says
    //
    // Concretely we just hold the senders, plus the join handles to
    // collect at the end.
    let total = workers.len();
    let reserved_count = policy.io_reserved_workers.min(total.saturating_sub(1));

    let mut io_only_senders: Vec<Sender<Task>> = Vec::new();
    let mut general_senders: Vec<Sender<Task>> = Vec::new();
    let mut joins = Vec::with_capacity(total);

    for (i, w) in workers.into_iter().enumerate() {
        if i < reserved_count {
            io_only_senders.push(w.sender);
        } else {
            general_senders.push(w.sender);
        }
        joins.push((w.id, w.join));
    }

    // Round-robin pointers so we spread work across workers in each pool.
    // Without this, we'd always hand work to worker 0 and the rest would idle.
    let mut next_io_only = 0usize;
    let mut next_general = 0usize;

    // Track the weighted-dispatch counter. Increments after each
    // dispatch and resets when it crosses cpu_weight + io_weight.
    // If both weights are 0 (pathological), force a period of 1 so we
    // don't divide by zero. choose_queue() handles that case anyway.
    let mut weight_tick: u32 = 0;
    let weight_period = (policy.cpu_weight + policy.io_weight).max(1);

    loop {
        // Block until *something* is available or we're closed.
        let snap = queues.wait_until_ready();

        if snap.cpu_len == 0 && snap.io_len == 0 {
            // Empty + closed -> done.
            if snap.closed {
                break;
            } else {
                // Spurious wakeup; loop and re-wait.
                continue;
            }
        }

        // Decide which queue to pull from this round.
        let chosen = choose_queue(snap, &policy, weight_tick, weight_period, &queues);

        let task = match queues.try_pop(chosen) {
            Some(t) => t,
            None => {
                // Race: queue went empty between snapshot and pop.
                // Loop and re-evaluate.
                continue;
            }
        };

        weight_tick = (weight_tick + 1) % weight_period;

        // Pick a worker. IO tasks may go to either pool; CPU tasks may
        // only go to the general pool (so the IO-reserved workers stay
        // available for IO bursts).
        let send_result = match task.kind {
            TaskKind::Io => {
                // Prefer the reserved pool first if it's there.
                if !io_only_senders.is_empty() {
                    let idx = next_io_only % io_only_senders.len();
                    next_io_only = next_io_only.wrapping_add(1);
                    io_only_senders[idx].send(task)
                } else {
                    let idx = next_general % general_senders.len();
                    next_general = next_general.wrapping_add(1);
                    general_senders[idx].send(task)
                }
            }
            TaskKind::Cpu => {
                if general_senders.is_empty() {
                    // Pathological config: all workers reserved for IO.
                    // Fall back to IO-only senders so we don't deadlock.
                    let idx = next_io_only % io_only_senders.len();
                    next_io_only = next_io_only.wrapping_add(1);
                    io_only_senders[idx].send(task)
                } else {
                    let idx = next_general % general_senders.len();
                    next_general = next_general.wrapping_add(1);
                    general_senders[idx].send(task)
                }
            }
        };

        // A SendError means the worker thread died. Log and keep going;
        // dropping tasks beats deadlocking the rest of the system.
        if let Err(SendError(dropped)) = send_result {
            eprintln!(
                "dispatcher: worker channel closed unexpectedly while sending task {} ({:?})",
                dropped.id, dropped.kind
            );
        }
    }

    // Shutdown:
    //   - Drop all task senders so each worker's recv() returns Err and
    //     the worker exits its loop. This is the "clean shutdown" path.
    drop(io_only_senders);
    drop(general_senders);

    // Join all workers and collect their busy-time totals.
    let mut busy_records = Vec::with_capacity(joins.len());
    for (id, j) in joins {
        match j.join() {
            Ok(busy) => busy_records.push(busy),
            Err(_) => {
                eprintln!("dispatcher: worker {id} panicked during shutdown");
            }
        }
    }
    busy_records
}

/// Decide which queue to dispatch from given the current snapshot.
///
/// Priority of considerations:
///   1. Aging: if one queue's head has been waiting too long, serve it.
///   2. If only one queue has work, that's our answer.
///   3. Weighted alternation between the two.
fn choose_queue(
    snap: crate::queues::QueueSnapshot,
    policy: &PolicyConfig,
    tick: u32,
    period: u32,
    queues: &ReadyQueues,
) -> TaskKind {
    // Step 1: aging override. We re-check the actual head wait times
    // because the snapshot is slightly stale by the time we get here.
    let aging = Duration::from_millis(policy.aging_threshold_ms);
    let cpu_head = queues.head_wait(TaskKind::Cpu);
    let io_head = queues.head_wait(TaskKind::Io);
    match (cpu_head, io_head) {
        (Some(cw), Some(iw)) => {
            if cw > aging && cw >= iw {
                return TaskKind::Cpu;
            }
            if iw > aging && iw > cw {
                return TaskKind::Io;
            }
        }
        (Some(cw), None) if cw > aging => return TaskKind::Cpu,
        (None, Some(iw)) if iw > aging => return TaskKind::Io,
        _ => {}
    }

    // Step 2: only one side has work.
    if snap.cpu_len == 0 {
        return TaskKind::Io;
    }
    if snap.io_len == 0 {
        return TaskKind::Cpu;
    }

    // Step 3: weighted alternation. Within one period, the first
    // `cpu_weight` ticks go to CPU and the rest go to IO.
    if tick < policy.cpu_weight && period > 0 {
        TaskKind::Cpu
    } else {
        TaskKind::Io
    }
}