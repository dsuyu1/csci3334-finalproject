//! Per-task completion records and the rollup summary.
//!
//! Workers send a `Completion` to the metrics thread when they finish
//! a task. The metrics thread is the single writer to `Vec<Completion>`,
//! which means no lock is needed on the storage itself — the channel
//! provides the synchronization.

use crate::monitor::MonitorStats;
use crate::task::TaskKind;
use std::time::Duration;

/// Per-task completion record.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub struct Completion {
    pub id: u64,
    pub kind: TaskKind,
    pub worker_id: usize,
    /// Time spent waiting in the ready queue between enqueue and worker pickup.
    pub wait: Duration,
    /// Wait + execution time (end-to-end through the system).
    pub turnaround: Duration,
    /// How long the worker was actually busy on this task.
    pub busy: Duration,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WorkerBusy {
    pub worker_id: usize,
    pub total_busy: Duration,
}

/// Everything the summary printer needs at the end of a run.
pub struct Summary {
    pub completions: Vec<Completion>,
    pub worker_busy: Vec<WorkerBusy>,
    pub makespan: Duration,
}

impl Summary {
    /// Build the formatted report as a `String`. Callers can print it,
    /// save it to a file, or both.
    pub fn report(
        &self,
        num_workers: usize,
        queue_stats: crate::queues::QueueStats,
        label: &str,
        monitor: &MonitorStats,
        cpu_fraction: f64,
        total_tasks: u64,
    ) -> String {
        let n = self.completions.len();
        if n == 0 {
            return format!("[{label}] No tasks completed.\n");
        }

        let total_wait: Duration = self.completions.iter().map(|c| c.wait).sum();
        let total_turn: Duration = self.completions.iter().map(|c| c.turnaround).sum();
        let max_wait_c = self
            .completions
            .iter()
            .max_by_key(|c| c.wait)
            .unwrap();

        let cpu_done: Vec<&Completion> = self
            .completions
            .iter()
            .filter(|c| c.kind == TaskKind::Cpu)
            .collect();
        let io_done: Vec<&Completion> = self
            .completions
            .iter()
            .filter(|c| c.kind == TaskKind::Io)
            .collect();
        let avg_wait_cpu = avg_dur(&cpu_done.iter().map(|c| c.wait).collect::<Vec<_>>());
        let avg_wait_io = avg_dur(&io_done.iter().map(|c| c.wait).collect::<Vec<_>>());

        // Fairness gap between task classes (smaller = fairer dispatch).
        let fairness_gap = if avg_wait_cpu > avg_wait_io {
            avg_wait_cpu - avg_wait_io
        } else {
            avg_wait_io - avg_wait_cpu
        };

        // Worker utilization: fraction of makespan each worker was busy.
        let total_busy_all: Duration = self.worker_busy.iter().map(|w| w.total_busy).sum();
        let denom = self.makespan.as_secs_f64() * num_workers as f64;
        let utilization_pct = if denom > 0.0 {
            total_busy_all.as_secs_f64() / denom * 100.0
        } else {
            0.0
        };

        let makespan_ms = self.makespan.as_millis();
        let io_pct = ((1.0 - cpu_fraction) * 100.0) as u32;
        let cpu_pct_label = (cpu_fraction * 100.0) as u32;

        let mut s = String::new();

        // Header
        s.push_str(&format!("== {} ==\n", label));
        s.push_str(&format!(
            "{} tasks, {}% IO / {}% CPU, {} workers\n",
            total_tasks, io_pct, cpu_pct_label, num_workers
        ));
        s.push('\n');
        s.push_str("— results —\n");
        s.push_str(&format!("total runtime        : {} ms\n", makespan_ms));
        s.push_str(&format!("makespan             : {} ms\n", makespan_ms));
        s.push_str(&format!(
            "tasks completed      : {}  (IO={}, CPU={})\n",
            n,
            io_done.len(),
            cpu_done.len()
        ));
        s.push_str(&format!(
            "avg wait time        : {:.2} ms\n",
            (total_wait / n as u32).as_secs_f64() * 1000.0
        ));
        s.push_str(&format!(
            "avg wait (IO only)   : {:.2} ms\n",
            avg_wait_io.as_secs_f64() * 1000.0
        ));
        s.push_str(&format!(
            "avg wait (CPU only)  : {:.2} ms\n",
            avg_wait_cpu.as_secs_f64() * 1000.0
        ));
        s.push_str(&format!(
            "avg turnaround time  : {:.2} ms\n",
            (total_turn / n as u32).as_secs_f64() * 1000.0
        ));
        s.push_str(&format!(
            "max wait time        : {:.0} ms  (task #{})\n",
            max_wait_c.wait.as_secs_f64() * 1000.0,
            max_wait_c.id
        ));
        s.push_str(&format!("avg CPU usage        : {:.2} %\n", monitor.avg_cpu_pct));
        s.push_str(&format!(
            "avg workers active   : {:.2} / {}\n",
            monitor.avg_active_workers, num_workers
        ));
        s.push_str(&format!("worker utilization   : {:.1} %\n", utilization_pct));
        s.push_str(&format!("monitor samples      : {}\n", monitor.sample_count));
        s.push_str(&format!("monitor csv          : {}\n", monitor.csv_path));

        // Queue stats
        s.push('\n');
        s.push_str("— queue statistics —\n");
        s.push_str(&format!("CPU queue  max / avg : {} / {:.2}\n", queue_stats.cpu_max_len, queue_stats.cpu_avg_len));
        s.push_str(&format!("IO  queue  max / avg : {} / {:.2}\n", queue_stats.io_max_len, queue_stats.io_avg_len));
        s.push_str(&format!(
            "fairness gap         : {:.2} ms  (|avg_wait_CPU - avg_wait_IO|)\n",
            fairness_gap.as_secs_f64() * 1000.0
        ));

        // Per-worker breakdown
        s.push('\n');
        s.push_str("— per-worker busy time —\n");
        let mut sorted = self.worker_busy.clone();
        sorted.sort_by_key(|w| w.worker_id);
        for w in &sorted {
            let pct = if self.makespan.as_secs_f64() > 0.0 {
                w.total_busy.as_secs_f64() / self.makespan.as_secs_f64() * 100.0
            } else {
                0.0
            };
            s.push_str(&format!(
                "  worker {:>2}          : {:>8.2} ms ({:>5.1}%)\n",
                w.worker_id,
                w.total_busy.as_secs_f64() * 1000.0,
                pct
            ));
        }

        s
    }
}

fn avg_dur(xs: &[Duration]) -> Duration {
    if xs.is_empty() {
        Duration::ZERO
    } else {
        let total: Duration = xs.iter().copied().sum();
        total / xs.len() as u32
    }
}