//! The dual-queue shared state.
//!
//! There are two ready queues: one for CPU tasks and one for IO tasks.
//! The dispatcher decides which queue to pull from next. Workers do not
//! talk to the queues directly; they receive their next task on a
//! per-worker channel from the dispatcher. That keeps the scheduling
//! decision in one place.
//!
//! Both queues live behind a single `Mutex` because the policy needs to
//! examine both at once (e.g., "is the IO queue empty? then pull CPU").
//! Two separate mutexes would make a coherent decision impossible
//! without holding both at once anyway, which is just one lock with
//! more steps and worse failure modes.

use crate::task::{Task, TaskKind};
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::Instant;

/// Shared ready queues plus a notification mechanism so the dispatcher
/// thread can park instead of busy-waiting when both queues are empty.
pub struct ReadyQueues {
    inner: Mutex<Inner>,
    cvar: Condvar,
}

struct Inner {
    cpu: VecDeque<Task>,
    io: VecDeque<Task>,
    /// Once this is true and both queues are drained, the dispatcher exits.
    /// Set by the generator after it has pushed the last task.
    closed: bool,

    // Sampled queue-length stats, updated on each push/pop.
    cpu_max_len: usize,
    io_max_len: usize,
    cpu_len_sum: u64,
    io_len_sum: u64,
    samples: u64,
}

impl Default for ReadyQueues {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadyQueues {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                cpu: VecDeque::new(),
                io: VecDeque::new(),
                closed: false,
                cpu_max_len: 0,
                io_max_len: 0,
                cpu_len_sum: 0,
                io_len_sum: 0,
                samples: 0,
            }),
            cvar: Condvar::new(),
        }
    }

    /// Push an arrived task into the appropriate queue. Stamps the
    /// enqueue time so we can compute wait time later, and wakes the
    /// dispatcher in case it was parked on an empty system.
    pub fn push(&self, mut task: Task) {
        task.enqueue_time = Some(Instant::now());
        let kind = task.kind;
        let mut g = self.inner.lock().expect("queues mutex poisoned");
        match kind {
            TaskKind::Cpu => g.cpu.push_back(task),
            TaskKind::Io => g.io.push_back(task),
        }
        // Sample queue lengths after the push.
        g.cpu_max_len = g.cpu_max_len.max(g.cpu.len());
        g.io_max_len = g.io_max_len.max(g.io.len());
        g.cpu_len_sum += g.cpu.len() as u64;
        g.io_len_sum += g.io.len() as u64;
        g.samples += 1;
        // One waiter is enough — only the dispatcher waits on this cvar.
        self.cvar.notify_one();
    }

    /// Mark the input stream as closed. The dispatcher will exit once
    /// both queues drain after this flag flips.
    pub fn close(&self) {
        let mut g = self.inner.lock().expect("queues mutex poisoned");
        g.closed = true;
        self.cvar.notify_all();
    }

    /// Block until the dispatcher has *something* to look at: either a
    /// non-empty queue or a `closed` flag. Returns the queue lengths
    /// and the closed flag so the dispatcher can decide what to do next
    /// without a second lock acquire.
    pub fn wait_until_ready(&self) -> QueueSnapshot {
        let mut g = self.inner.lock().expect("queues mutex poisoned");
        // Standard cvar loop: re-check the predicate after each wakeup,
        // because spurious wakeups are allowed.
        while g.cpu.is_empty() && g.io.is_empty() && !g.closed {
            g = self.cvar.wait(g).expect("queues mutex poisoned");
        }
        QueueSnapshot {
            cpu_len: g.cpu.len(),
            io_len: g.io.len(),
            closed: g.closed,
        }
    }

    /// Pop a task of the given kind, if one is available. Returns None
    /// if that queue is empty (without blocking).
    pub fn try_pop(&self, kind: TaskKind) -> Option<Task> {
        let mut g = self.inner.lock().expect("queues mutex poisoned");
        let popped = match kind {
            TaskKind::Cpu => g.cpu.pop_front(),
            TaskKind::Io => g.io.pop_front(),
        };
        if popped.is_some() {
            g.cpu_len_sum += g.cpu.len() as u64;
            g.io_len_sum += g.io.len() as u64;
            g.samples += 1;
        }
        popped
    }

    /// Look at the head of a queue without removing it. Used by aging
    /// to check how long the longest-waiting task has been sitting.
    pub fn head_wait(&self, kind: TaskKind) -> Option<std::time::Duration> {
        let g = self.inner.lock().expect("queues mutex poisoned");
        let q = match kind {
            TaskKind::Cpu => &g.cpu,
            TaskKind::Io => &g.io,
        };
        q.front()
            .and_then(|t| t.enqueue_time.map(|e| e.elapsed()))
    }

    pub fn stats(&self) -> QueueStats {
        let g = self.inner.lock().expect("queues mutex poisoned");
        let avg_cpu = if g.samples == 0 { 0.0 } else { g.cpu_len_sum as f64 / g.samples as f64 };
        let avg_io = if g.samples == 0 { 0.0 } else { g.io_len_sum as f64 / g.samples as f64 };
        QueueStats {
            cpu_max_len: g.cpu_max_len,
            io_max_len: g.io_max_len,
            cpu_avg_len: avg_cpu,
            io_avg_len: avg_io,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QueueSnapshot {
    pub cpu_len: usize,
    pub io_len: usize,
    pub closed: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct QueueStats {
    pub cpu_max_len: usize,
    pub io_max_len: usize,
    pub cpu_avg_len: f64,
    pub io_avg_len: f64,
}