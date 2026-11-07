# Checksum queue performance baseline

## Run on Ubuntu / WSL2

From the repository root with the virtual environment active and the
application role provisioned:

```bash
docker compose up -d --wait --wait-timeout 120
out=$(mktemp -d /tmp/rwl-performance.XXXXXX)
python -m retry_window_lab.benchmark --output "$out/baseline" &&
python -m retry_window_lab.benchmark --slots 1 --dispatch-poll-ms 500 \
  --output "$out/dispatch-control"
```

The command starts its own loopback task API, a dispatcher and one worker
process per run. The worker process uses one, two or four execution slots.
Each run creates its own PostgreSQL schema and Redis stream and removes them
on exit. Keep other demos and heavy workloads idle while measuring.

## Workload

Each configuration runs three times. The order rotates between repeats. Each
run sends 24 warm-up tasks and then 120 measured tasks, with up to eight
concurrent HTTP submissions. Each task hashes a deterministic 1,024-byte ASCII
payload. The driver opens one HTTP connection per submission and verifies the
SHA-256 result and exactly one successful attempt for every task. A failed
check aborts the run.

The checksum workload exercises HTTP admission, the durable outbox, Redis
Streams delivery, task claim, execution and persisted completion. The
benchmark uses an explicit 100 ms dispatcher poll interval. The 500 ms
control holds the worker slot count at one and changes only the interval.
Compare it with the one-slot row in the baseline.

## Read the reports

| Metric | Start and end |
| --- | --- |
| Throughput | First measured batch start to observation of all durable completions, divided into the task count |
| HTTP submission | Client POST to receipt of the complete HTTP response |
| Durable end-to-end latency | PostgreSQL task creation timestamp to attempt finish timestamp |
| Claim wait | PostgreSQL task creation to attempt start |
| Attempt time | Attempt start to finish, including checksum execution and completion persistence work up to the finish timestamp |

The driver polls for completions every 25 ms after all submission responses
have arrived. Throughput uses real monotonic elapsed time. Latencies use
nearest-rank percentiles pooled across measured tasks. The summary shows
median, minimum and maximum throughput across repeats. The JSON also includes
individual samples, worker completion counts, settings and software versions.

Compare the claim-wait and attempt percentiles to locate where time accumulates.
Use the one-slot control to check how sensitive this workload is to dispatcher
polling. Claim wait also includes submission transaction time, outbox processing,
Redis delivery and worker pickup. The control estimates the combined effect
of changing dispatcher cadence within that path.

These are finite-burst measurements on a single host. The API, load driver,
dispatcher and worker share that host with PostgreSQL and Redis. The reported
latencies are wall-clock measurements. Phase 10 uses simulated provider time.

## Verification

```bash
python -m pytest -q tests/unit/test_benchmark.py
python -m pytest -q
```

The benchmark adds two unit tests. All 369 tests passed on Ubuntu / WSL2 with Docker Compose.

## Verified WSL2 results

Python 3.12.3, PostgreSQL 17.11 (Debian 17.11-1.pgdg12+2), Redis 7.4.12, 12 logical CPUs. Platform `Linux-6.6.87.2-microsoft-standard-WSL2-x86_64-with-glibc2.39`.

### 100 ms dispatcher baseline

120 measured tasks and 24 warm-up tasks per run.
8 concurrent submitters, 1024-byte checksum inputs, 100 ms dispatcher polling interval.

| Slots | Runs | Completed | Median tasks/s | Min–max tasks/s | Durable p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3 | 360 | 11.498 | 11.385–11.833 | 4034.333 | 7044.707 | 7370.857 |
| 2 | 3 | 360 | 19.356 | 17.161–20.554 | 2005.048 | 3010.786 | 3288.679 |
| 4 | 3 | 360 | 28.408 | 23.818–29.289 | 696.822 | 992.974 | 1102.168 |

| Slots | HTTP submission p95 ms | Created to claim p95 ms | Claim to finish p95 ms |
| ---: | ---: | ---: | ---: |
| 1 | 248.776 | 7004.343 | 47.268 |
| 2 | 295.717 | 2965.785 | 60.307 |
| 4 | 316.574 | 912.423 | 89.61 |

### 500 ms dispatcher control

120 measured tasks and 24 warm-up tasks per run.
8 concurrent submitters, 1024-byte checksum inputs, 500 ms dispatcher polling interval.

| Slots | Runs | Completed | Median tasks/s | Min–max tasks/s | Durable p50 ms | p95 ms | p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3 | 360 | 11.764 | 10.914–11.985 | 4303.145 | 7251.97 | 8066.277 |

| Slots | HTTP submission p95 ms | Created to claim p95 ms | Claim to finish p95 ms |
| ---: | ---: | ---: | ---: |
| 1 | 250.486 | 7216.536 | 47.306 |

Four slots delivered 2.47 times the median throughput and reduced durable p95 by 86% relative to one slot. Most one-slot latency accumulated before the worker claim. The one-slot throughput ranges overlap across dispatcher intervals, so this run gives limited evidence for changing the poll interval.
