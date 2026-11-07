# Release verification

Retry Window Lab 0.1.0 packages the queue, simulator, experiments and local
performance harness. Verify it from a fresh checkout on Ubuntu or WSL2 with
Python 3.12 and Docker Compose. On WSL2, keep Docker Desktop running with
Ubuntu integration enabled.

```bash
git clone --branch v0.1.0 https://github.com/Sharon-25/retry-window-lab.git
cd retry-window-lab
bash scripts/verify_release.sh
```

The script creates a fresh virtual environment and installs
`requirements.lock`. It starts a disposable PostgreSQL and Redis Compose
project with generated credentials and applies the database migrations.
It then runs the full suite and the core demonstrations.

| Check | Expected result |
| --- | --- |
| Dependency consistency | `pip check` reports no broken requirements |
| Full suite | 369 tests pass |
| Reservation recovery | Six cases, each with one observed provider effect |
| Concurrency and Redis replay | Eight tasks complete and two interrupted attempts enter review |
| Policy comparison | 24 trials with five, four and zero extra reservations for fresh-key, same-key and evidence policies |
| Benchmark smoke run | 12 measured checksum tasks complete with verified results |

Look for `Release verification passed.` and a successful exit. The script
prints the path to its logs and the comparison and benchmark reports. It
stops the provider and removes its disposable Compose project, volumes and
virtual environment on exit. On failure, inspect the logs in the printed
directory.

The verification stack uses loopback ports 55433, 56380 and 18001 for
PostgreSQL, Redis and the provider simulator. If any are in use, set
`RWL_VERIFY_PG_PORT`, `RWL_VERIFY_REDIS_PORT` or `RWL_VERIFY_PROVIDER_PORT` to
an available port when running the script. The verification project has
its own volumes and leaves the regular queue's data in place.

For the full performance matrix, follow [Performance baseline](PERFORMANCE.md).
The smoke run checks the measurement path on a small workload.

## Scope and limitations

- The task API, provider simulator and dashboard are intended for loopback
  use in a development environment. The database roles separate application
  and provider storage. API authentication and multi-tenant isolation are
  outside the release scope.
- Recovery relies on the provider evidence modeled by the simulator. When the
  queue exhausts that evidence, it can preserve uncertainty in `NEEDS_REVIEW`.
  The comparison's zero extra effects apply to the eight tested scenarios.
- PostgreSQL holds the durable task state. Redis can be rebuilt from the
  outbox and current task state. Recovery after losing PostgreSQL data
  depends on restoring that database.
- The benchmark measures finite checksum bursts on one host. The recorded
  throughput and latency describe that workload and environment.
- `requirements.lock` pins the Python dependencies. Compose uses PostgreSQL
  17 and Redis 7.4 release-series tags. Those tags may receive image updates.
