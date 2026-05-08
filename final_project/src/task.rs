//! Task model and reproducible workload generation.

use rand_chacha::ChaCha8Rng;
use std::time::{Duration, Instant};

/// What kind of work the task is doing. CPU tasks burn the worker thread;
/// IO tasks mostly sleep, simulating waiting on disk or network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Cpu,
    Io,
}

impl TaskKind {
    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            TaskKind::Cpu => "CPU",
            TaskKind::Io => "IO",
        }
    }
}

/// A unit of work flowing through the system.
///
/// `arrival_offset` is when the task is supposed to enter the system,
/// measured from the start of the simulation. The generator uses that
/// to pace the input stream.
///
/// `enqueue_time` and `start_time` are filled in as the task moves
/// through the dispatcher. They are `Option` so that an unprocessed
/// task is distinguishable from one that finished at t=0.
///
/// `priority` and `start_time` are read by extensions/instrumentation;
/// the allow lets the base build compile clean.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Task {
    pub id: u64,
    pub kind: TaskKind,
    pub arrival_offset: Duration,
    pub duration: Duration,
    pub priority: u8,

    pub enqueue_time: Option<Instant>,
    pub start_time: Option<Instant>,
}

impl Task {
    pub fn new(id: u64, kind: TaskKind, arrival_offset: Duration, duration: Duration, priority: u8) -> Self {
        Self {
            id,
            kind,
            arrival_offset,
            duration,
            priority,
            enqueue_time: None,
            start_time: None,
        }
    }
}

/// How a workload should be shaped. Each experiment builds one of these.
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub name: &'static str,
    pub total_tasks: u64,
    /// Fraction of tasks that are CPU-bound. The rest are IO.
    pub cpu_fraction: f64,
    /// Average arrival gap between tasks, in milliseconds.
    /// Smaller = bursty / overloaded; larger = relaxed.
    pub mean_interarrival_ms: f64,
    /// Range of CPU task durations in milliseconds (inclusive low, exclusive high).
    pub cpu_duration_ms: (u64, u64),
    /// Range of IO task durations in milliseconds.
    pub io_duration_ms: (u64, u64),
    /// If true, all tasks arrive in a tight cluster at the very start
    /// (used by the stressed experiment to force queue buildup).
    pub burst: bool,
    pub seed: u64,
}

/// Generate the full task list up front. Real arrival pacing happens
/// inside the generator thread, which sleeps based on `arrival_offset`.
///
/// Doing generation up front (rather than streaming in the thread)
/// keeps the random seed authoritative: re-running with the same
/// config produces the exact same task stream.
pub fn generate_workload(cfg: &WorkloadConfig) -> Vec<Task> {
    use rand_core::SeedableRng;
    let mut rng = ChaCha8Rng::seed_from_u64(cfg.seed);

    let mut tasks = Vec::with_capacity(cfg.total_tasks as usize);
    let mut clock_ms: f64 = 0.0;

    for id in 0..cfg.total_tasks {
        // Decide kind from the configured CPU/IO mix.
        let kind = if rng.r#gen::<f64>() < cfg.cpu_fraction {
            TaskKind::Cpu
        } else {
            TaskKind::Io
        };

        // Duration drawn uniformly from the configured range for this kind.
        let (lo, hi) = match kind {
            TaskKind::Cpu => cfg.cpu_duration_ms,
            TaskKind::Io => cfg.io_duration_ms,
        };
        let duration = Duration::from_millis(rng.gen_range(lo..hi.max(lo + 1)));

        // Priority: most tasks normal, a small fraction high. The dispatcher
        // can use this if we extend the policy; right now it shows up in metrics.
        let priority: u8 = if rng.r#gen::<f64>() < 0.1 { 2 } else { 1 };

        // Arrival pacing.
        //
        // Burst mode: stack everything at t=0 so the scheduler sees the
        // entire workload as a single instantaneous flood. This is the
        // adversarial case for queueing — all 600 tasks land before any
        // worker has finished even one. Otherwise, use exponential
        // interarrival gaps (Poisson process) for realistic random arrivals.
        let arrival_offset = if cfg.burst {
            Duration::ZERO
        } else {
            // Inverse-CDF sampling for an exponential distribution. Clamp the
            // log argument away from zero so we don't get -inf on a 0.0 draw.
            let u: f64 = rng.r#gen::<f64>().max(1e-9);
            let gap = -u.ln() * cfg.mean_interarrival_ms;
            clock_ms += gap;
            Duration::from_micros((clock_ms * 1000.0) as u64)
        };

        tasks.push(Task::new(id, kind, arrival_offset, duration, priority));
    }

    // For burst mode the offsets aren't sorted; sort so the generator
    // can sleep monotonically. For the Poisson case they're already
    // in order, but sorting is cheap and defensive.
    tasks.sort_by_key(|t| t.arrival_offset);
    tasks
}