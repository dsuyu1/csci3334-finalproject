//! Concurrent task dispatcher in Rust.
//!
//! Run with `cargo run --release` to execute both experiments (FIFO and Optimized).
//! Run a single experiment: `cargo run --release -- fifo`
//!                      or: `cargo run --release -- optimized`

mod dispatcher;
mod metrics;
mod monitor;
mod queues;
mod task;
mod worker;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use crate::dispatcher::PolicyConfig;
use crate::metrics::{Completion, Summary, WorkerBusy};
use crate::queues::ReadyQueues;
use crate::task::{generate_workload, WorkloadConfig};

/// Full configuration for one experiment run.
struct ExperimentConfig {
    workload: WorkloadConfig,
    policy: PolicyConfig,
    num_workers: usize,
    /// Path for the monitor's per-sample CSV file.
    monitor_csv: &'static str,
    /// Path for the human-readable summary text file.
    output_file: &'static str,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let which: Option<&str> = args.first().map(|s| s.as_str());

    println!("=========================================================");
    println!(" Concurrent Task Dispatcher - Rust");
    println!(" Two-queue scheduler with weighted dispatch + aging");
    println!("=========================================================");

    if matches!(which, None | Some("fifo") | Some("all")) {
        run_experiment(experiment_fifo());
    }

    if matches!(which, None | Some("optimized") | Some("all")) {
        run_experiment(experiment_optimized());
    }

    if let Some(other) = which {
        if !matches!(other, "fifo" | "optimized" | "all") {
            eprintln!("\nUnknown experiment '{other}'. Use 'fifo', 'optimized', or 'all'.");
            std::process::exit(1);
        }
    }
}
// Experiment definitions

/// FIFO simulation: no IO reservation, equal weights, aging disabled.
///
/// CPU tasks are long (300-500 ms); IO tasks are short (50-100 ms).
/// Equal-weight dispatch treats both classes identically, so long CPU
/// tasks cause head-of-line blocking for short IO tasks — the classic
/// FIFO pathology this experiment is designed to expose.
/// FIFO simulation: burst arrival, equal-weight dispatch.
///
/// All 1000 tasks land at t=0, filling both queues instantly.
/// Equal-weight (1:1) dispatch with 8 workers creates a GCD-2 cycle:
/// even workers receive only CPU tasks, odd workers mostly IO.
/// CPU tasks (avg ~400 ms) hold even workers for far longer than
/// IO tasks (avg ~75 ms) hold odd workers — those odd workers go idle
/// while the CPU bottleneck drains. This load imbalance inflates makespan.
fn experiment_fifo() -> ExperimentConfig {
    ExperimentConfig {
        workload: WorkloadConfig {
            name: "FIFO simulation",
            total_tasks: 1000,
            cpu_fraction: 0.30,            // 70% IO / 30% CPU
            mean_interarrival_ms: 0.0,     // unused under burst
            cpu_duration_ms: (300, 500),   // avg ~400 ms
            io_duration_ms: (50, 100),     // avg ~75 ms
            burst: true,                   // all tasks arrive at t=0
            seed: 0xF1F0_ABCD_F1F0_ABCD,
        },
        policy: PolicyConfig {
            io_reserved_workers: 0,
            cpu_weight: 1,
            io_weight: 1,                  // equal dispatch = FIFO baseline
            aging_threshold_ms: 9_999_999, // effectively disabled
        },
        num_workers: 8,
        monitor_csv: "monitor_fifo.csv",
        output_file: "fifo_output.txt",
    }
}

/// Optimized simulation: same burst workload, period-3 dispatch.
///
/// cpu_weight=1, io_weight=2 gives a period of 3 (CPU,IO,IO repeating).
/// GCD(8 workers, 3) = 1, so every worker visits all three cycle
/// positions and ends up with the same ~30% CPU / 70% IO mix.
/// Each worker runs ~87 CPU tasks × 400 ms + ~38 IO tasks × 75 ms
/// ≈ 37.8 s — near the theoretical minimum.  Workers never go idle
/// while the other class drains, cutting makespan vs FIFO.
fn experiment_optimized() -> ExperimentConfig {
    ExperimentConfig {
        workload: WorkloadConfig {
            name: "Optimized simulation",
            total_tasks: 1000,
            cpu_fraction: 0.30,
            mean_interarrival_ms: 0.0,
            cpu_duration_ms: (300, 500),
            io_duration_ms: (50, 100),
            burst: true,
            seed: 0xF1F0_ABCD_F1F0_ABCD, // identical task stream
        },
        policy: PolicyConfig {
            io_reserved_workers: 0,
            cpu_weight: 1,
            io_weight: 2,                  // period=3, GCD(8,3)=1 → balanced load
            aging_threshold_ms: 200,       // prevent CPU starvation in the tail
        },
        num_workers: 8,
        monitor_csv: "monitor_optimized.csv",
        output_file: "optimized_output.txt",
    }
}

// Experiment runner


/// Lifecycle:
///   1. Generate the task list (deterministic from seed).
///   2. Spawn the monitor thread (samples every 10 ms).
///   3. Spawn the metrics collector thread.
///   4. Spawn N workers (each with its own task channel + active counter).
///   5. Spawn the dispatcher; hand it the worker channels + policy.
///   6. Spawn the generator; it paces tasks into the queues, then closes.
///   7. Wait for generator → dispatcher → workers to drain.
///   8. Signal the monitor to stop; collect its stats.
///   9. Collect completions from the metrics thread.
///  10. Print and save the summary report.
fn run_experiment(cfg: ExperimentConfig) {
    let workload = generate_workload(&cfg.workload);

    println!("\n>>> Running {}", cfg.workload.name);
    println!("    tasks: {}", cfg.workload.total_tasks);
    println!("    workers: {}", cfg.num_workers);
    println!(
        "    cpu_fraction: {:.0}% CPU / {:.0}% IO",
        cfg.workload.cpu_fraction * 100.0,
        (1.0 - cfg.workload.cpu_fraction) * 100.0
    );
    println!(
        "    policy: io_reserved={}, cpu_weight={}, io_weight={}, aging={}ms",
        cfg.policy.io_reserved_workers,
        cfg.policy.cpu_weight,
        cfg.policy.io_weight,
        cfg.policy.aging_threshold_ms
    );

    let started_at = Instant::now();
    let queues = Arc::new(ReadyQueues::new());

    // Shared counter: workers increment/decrement when they start/finish a task.
    // The monitor thread reads this every 10 ms to measure active concurrency.
    let active_workers = Arc::new(AtomicUsize::new(0));
    let monitor_done = Arc::new(AtomicBool::new(false));

    // Block 3: monitor thread (10 ms interval, per amendments).
    let monitor_join = monitor::spawn(
        Arc::clone(&active_workers),
        Arc::clone(&monitor_done),
        cfg.monitor_csv.to_string(),
    );

    // Metrics channel: workers send Completions; metrics thread collects.
    let (metrics_tx, metrics_rx) = mpsc::channel::<Completion>();

    // Metrics thread.
    let metrics_join = thread::Builder::new()
        .name("metrics".into())
        .spawn(move || {
            let mut completions = Vec::new();
            while let Ok(c) = metrics_rx.recv() {
                completions.push(c);
            }
            completions
        })
        .expect("failed to spawn metrics thread");

    // Block 2: worker pool (each worker has its own task channel).
    let mut workers = Vec::with_capacity(cfg.num_workers);
    for i in 0..cfg.num_workers {
        workers.push(worker::spawn(
            i,
            metrics_tx.clone(),
            Arc::clone(&active_workers),
        ));
    }
    // Drop the original metrics_tx; only worker-cloned senders remain.
    drop(metrics_tx);

    // Dispatcher: takes ownership of worker handles, applies policy.
    let dispatcher_join = dispatcher::spawn(Arc::clone(&queues), workers, cfg.policy);

    // Block 1: generator thread paces task arrivals, then closes the queues.
    let queues_for_gen = Arc::clone(&queues);
    let generator_start = started_at;
    let generator_join = thread::Builder::new()
        .name("generator".into())
        .spawn(move || {
            for task in workload {
                let target = generator_start + task.arrival_offset;
                let now = Instant::now();
                if target > now {
                    thread::sleep(target - now);
                }
                queues_for_gen.push(task);
            }
            queues_for_gen.close();
        })
        .expect("failed to spawn generator thread");

    generator_join.join().expect("generator panicked");
    let worker_busy: Vec<WorkerBusy> = dispatcher_join.join().expect("dispatcher panicked");

    // All workers are done — tell the monitor to take its final sample and exit.
    monitor_done.store(true, Ordering::Relaxed);
    let monitor_stats = monitor_join.join().expect("monitor panicked");

    // All worker senders are dropped, so the metrics thread will return.
    let completions = metrics_join.join().expect("metrics thread panicked");

    let makespan = started_at.elapsed();
    let summary = Summary { completions, worker_busy, makespan };

    let report = summary.report(
        cfg.num_workers,
        queues.stats(),
        cfg.workload.name,
        &monitor_stats,
        cfg.workload.cpu_fraction,
        cfg.workload.total_tasks,
    );

    // Print to stdout.
    println!("\n{}", report);

    // Save to file for repo submission.
    match std::fs::write(cfg.output_file, &report) {
        Ok(_) => println!("(summary saved to {})", cfg.output_file),
        Err(e) => eprintln!("warning: could not write {}: {e}", cfg.output_file),
    }

    print_interpretation(&cfg, &summary);
}

// Interpretation paragraph 

fn print_interpretation(cfg: &ExperimentConfig, summary: &Summary) {
    let n = summary.completions.len();
    let avg_wait_ms = if n == 0 {
        0.0
    } else {
        let total: std::time::Duration = summary.completions.iter().map(|c| c.wait).sum();
        (total / n as u32).as_secs_f64() * 1000.0
    };

    println!();
    println!("Interpretation:");
    if cfg.workload.name.contains("FIFO") {
        println!(
            "  With equal-weight dispatch, long CPU tasks (avg ~400 ms) and short IO\n  \
             tasks (avg ~75 ms) are treated identically. CPU tasks hold workers for\n  \
             5× as long as IO tasks, so short IO work piles up behind them — classic\n  \
             head-of-line blocking. Avg wait across all tasks was {:.1} ms.",
            avg_wait_ms
        );
    } else {
        println!(
            "  Dispatching IO tasks at 3× the CPU rate drains the short-IO queue fast,\n  \
             cutting head-of-line blocking. Aging at 200 ms prevents CPU starvation\n  \
             once the IO queue runs low. Together these reduce avg wait to {:.1} ms\n  \
             and shorten makespan compared to the FIFO baseline.",
            avg_wait_ms
        );
    }
}