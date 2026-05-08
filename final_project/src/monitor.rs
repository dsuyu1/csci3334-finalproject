//! Background monitor thread.
//!
//! Samples the active-worker count and system CPU usage (via /proc/stat)
//! at a fixed 10 ms interval for the duration of an experiment. Results
//! are written to a CSV file and summarised in MonitorStats on shutdown.
//!
//! Shutdown: the caller sets `done = true`; the monitor takes one final
//! sample and exits, then the join returns MonitorStats.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

const SAMPLE_MS: u64 = 10;

/// Aggregated results returned when the monitor thread exits.
pub struct MonitorStats {
    pub avg_active_workers: f64,
    pub avg_cpu_pct: f64,
    pub sample_count: usize,
    pub csv_path: String,
}

/// Spawn the monitor. Returns a `JoinHandle<MonitorStats>`.
pub fn spawn(
    active_workers: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    csv_path: String,
) -> JoinHandle<MonitorStats> {
    thread::Builder::new()
        .name("monitor".into())
        .spawn(move || run_monitor(active_workers, done, csv_path))
        .expect("failed to spawn monitor thread")
}

fn run_monitor(
    active_workers: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    csv_path: String,
) -> MonitorStats {
    let file = File::create(&csv_path).expect("cannot create monitor CSV");
    let mut w = BufWriter::new(file);
    writeln!(w, "elapsed_ms,active_workers,cpu_pct").unwrap();

    let mut sum_active: f64 = 0.0;
    let mut sum_cpu: f64 = 0.0;
    let mut count: usize = 0;
    let mut prev_ticks = read_cpu_ticks();

    loop {
        thread::sleep(Duration::from_millis(SAMPLE_MS));

        let active = active_workers.load(Ordering::Relaxed);
        let curr_ticks = read_cpu_ticks();
        let cpu_pct = cpu_delta(prev_ticks, curr_ticks);
        prev_ticks = curr_ticks;

        let elapsed_ms = count as u64 * SAMPLE_MS;
        writeln!(w, "{},{},{:.2}", elapsed_ms, active, cpu_pct).unwrap();

        sum_active += active as f64;
        sum_cpu += cpu_pct;
        count += 1;

        if done.load(Ordering::Relaxed) {
            break;
        }
    }
    w.flush().unwrap();

    MonitorStats {
        avg_active_workers: if count > 0 { sum_active / count as f64 } else { 0.0 },
        avg_cpu_pct: if count > 0 { sum_cpu / count as f64 } else { 0.0 },
        sample_count: count,
        csv_path,
    }
}

/// Read idle and total CPU ticks from /proc/stat (Linux only).
/// Returns None on any parse or read failure; callers treat None as 0% CPU.
fn read_cpu_ticks() -> Option<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/stat").ok()?;
    let line = text.lines().next()?;
    // Fields after the "cpu" label: user nice system idle iowait irq softirq ...
    let parts: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|s| s.parse().ok())
        .collect();
    if parts.len() < 4 {
        return None;
    }
    // iowait (index 4) counts as idle — workers sleeping on IO contribute there.
    let idle = parts[3] + parts.get(4).copied().unwrap_or(0);
    let total: u64 = parts.iter().sum();
    Some((idle, total))
}

/// Compute CPU usage % between two consecutive /proc/stat snapshots.
fn cpu_delta(prev: Option<(u64, u64)>, curr: Option<(u64, u64)>) -> f64 {
    match (prev, curr) {
        (Some((pi, pt)), Some((ci, ct))) => {
            let d_idle = ci.saturating_sub(pi) as f64;
            let d_total = ct.saturating_sub(pt) as f64;
            if d_total <= 0.0 {
                0.0
            } else {
                ((d_total - d_idle) / d_total * 100.0).clamp(0.0, 100.0)
            }
        }
        _ => 0.0,
    }
}