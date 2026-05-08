# Concurrent Task Dispatcher — Rust

A multi-threaded task scheduler.

## Run instructions

```bash
cd final_project
cargo build --release # compiles the project
cargo run --release # runs both experiments
cargo run --release -- fifo # runs FIFO only
cargo run --release -- optimized # runs Optimized only

# Results printed to terminal and saved to fifo_output.txt / optimized_output.txt
# Monitor data saved to monitor_fifo.csv / monitor_optimized.csv
```

## Example Output

```
== FIFO simulation ==
1000 tasks, 70% IO / 30% CPU, 8 workers
makespan             : 34017 ms
worker utilization   : 62.5 %

== Optimized simulation ==
1000 tasks, 70% IO / 30% CPU, 8 workers
makespan             : 22198 ms
worker utilization   : 95.9 %
```

Full output is saved to `fifo_output.txt` and `optimized_output.txt`. Per-sample monitor data goes to `monitor_fifo.csv` and `monitor_optimized.csv`.

## Design Summary

### Architecture

Thread roles:

| Thread | Role |
|---|---|
| **generator** | Walks the pre-built task list; paces arrivals via `thread::sleep`; pushes into `ReadyQueues`; calls `close()` when done |
| **dispatcher** | Loops on `ReadyQueues`; applies the scheduling policy; pushes tasks to per-worker channels via round-robin |
| **workers (×8)** | Each owns one `mpsc` channel receiver; simulates CPU (tight loop) or IO (sleep); reports `Completion` to metrics thread |
| **monitor** | Samples active-worker count + `/proc/stat` CPU% every 10 ms; writes `monitor_*.csv` |
| **metrics** | Collects `Completion` records via channel; returns `Vec<Completion>` when all workers exit |

### Shared State

The only shared mutable state is `ReadyQueues`, which holds a `Mutex<Inner>` containing both VecDeques and a `Condvar`. Using a single lock for both queues lets the dispatcher examine both atomically. A second shared piece is `Arc<AtomicUsize>` (active worker count), which is lock-free and written by workers, read by the monitor.

Everything else moves through channels: tasks flow generator → queues → dispatcher → worker; completions flow worker → metrics thread.

### Scheduling Policy

You can configure two "knobs."

- **Weighted dispatch**: the dispatcher pulls `cpu_weight` tasks from the CPU queue per `io_weight` from the IO queue. Setting `(1, 1)` is FIFO. `(1, 2)` gives `CPU, IO, IO` dispatch cycles.
- **Aging**: if the head of a queue has waited longer than `aging_threshold_ms`, that queue is served immediately regardless of the weight counter, preventing starvation.

The FIFO experiment uses `(1, 1)` with no aging. Due to the GCD relationship between the 8-worker round-robin and the period-2 cycle, even-indexed workers receive only CPU tasks while odd-indexed workers receive only IO tasks. Because CPU tasks are ~5× longer, odd workers sit idle after ~9 s while even workers remain busy until ~34 s — 62.5% overall utilization.

The Optimized experiment uses `(1, 2)` (period 3). GCD(8, 3) = 1, so every worker visits all three positions in the cycle and converges to the same 30% CPU / 70% IO mix. All workers finish at roughly the same time, achieving 95.9% utilization and a 34.7% shorter makespan.

### Experiments

| Metric | FIFO | Optimized | Change |
|---|---|---|---|
| Makespan | 34,017 ms | 22,198 ms | **−34.7 %** |
| Worker utilization | 62.5 % | 95.9 % | +33.4 pp |
| Avg workers active | 5.01 / 8 | 7.66 / 8 | +53 % |
| Avg wait time | 12,539 ms | 10,829 ms | −13.6 % |
| Fairness gap | 3,325 ms | 818 ms | −75 % |

## Tool Use Disclosure

**Tools used:** Claude Code (Anthropic) as a coding assistant throughout development. Here is some of it's advice I thought were useful and not so useful:curl https://sh.rustup.rs -sSf | sh


**Advice accepted:** The suggestion to use `Arc<AtomicUsize>` for the active-worker counter instead of a `Mutex<usize>` — the monitor polls this every 10 ms, so lock-free reads are a meaningful win.

**Advice rejected:** An early suggestion to use `sync_channel(0)` to force the dispatcher to wait for an idle worker before each dispatch. While theoretically cleaner for load balancing, it would have serialized the dispatcher and prevented pipelining. The round-robin push model with per-worker channels is faster and the policy layer handles balance explicitly.

Also, Claude helped me make the report prettier. Making code blocks in Google Docs is a nightmare. Claude was great for getting cool experimental data rather than just making a project and struggling to measure the results. Lastly, it made my comments readable and traceable. If you'd like, hopefully the comments help tracing the program better.