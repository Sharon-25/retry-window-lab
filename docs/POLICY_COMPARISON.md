# Retry policy comparison

## Run

From the repository root with the virtual environment active, start the
provider simulator in one terminal.

```bash
docker compose up -d --wait --wait-timeout 120
python -m retry_window_lab.provision
python -m uvicorn retry_window_lab.provider_api:app --host 127.0.0.1 --port 8001
```

In a second terminal, activate the same environment and generate a report.

```bash
report_dir=$(mktemp -d /tmp/rwl-comparison.XXXXXX)
python -m retry_window_lab.comparison --seed 42 --output "$report_dir/results"
cat "$report_dir/results/comparison.md"
```

Pass `--base-url` if the local simulator is listening on another port.
The command prints the aggregate results and writes a Markdown summary
and a JSON report with the seed, scenario settings, trace and ledger data.

## Compared policies

| Policy | Behavior after an uncertain outcome |
| --- | --- |
| Fresh key | Resends with a new provider idempotency key |
| Same key | Resends with the original provider idempotency key |
| Evidence | Uses the queue's durable recovery decisions, provider lookup and bounded retry after proven no-effect outcomes |

The two baseline policies use a budget of three mutations. They stop for
review when a resend is still processing or the provider returns a conflict.
The evidence policy uses the existing recovery and ownership repositories.
The harness drives their durable claims from PostgreSQL outbox rows directly
to control fault timing. Redis delivery and worker process failures are
covered by the existing integration suite.

Each trial uses a fresh provider experiment with 100 units per SKU and a
60-second protection window. The experiment clock starts at zero. The harness
advances it at explicit fault checkpoints. PostgreSQL schedules retries and
observations using its regular clock. The seed controls retry jitter.
The comparison drops its temporary queue schema and keeps the provider
experiments available for audit.

## Business scenarios

| Case | Setup | First follow-up, provider seconds |
| --- | --- | ---: |
| E01 | Normal reservation | — |
| E02 | Response lost after a committed reservation | 1 |
| E03 | Response lost, followed by protection expiry | 61 |
| E04 | Response lost, with the reservation hidden until second 63 | 61 |
| E05 | Response lost, with unavailable lookup | 61 |
| E10 | First mutation rejected with a settled no-effect result | 1 |
| E11 | Response lost, with a resend queued at 59 and delivered at 61 | 59 |
| E12 | Requested quantity exceeds available stock | — |

The harness settles scheduled requests through the same provider-time horizon
for all policies in each scenario. Provider ledger inspection happens after the
policy decision and is excluded from the policy's observations and call counts.
An unexpected service outage aborts the comparison.

## Read the results

- A confirmed completion requires a successful decision and exactly one
  matching provider reservation at the horizon.
- Extra effects count reservations beyond the first per operation.
- An unresolved outcome is a task left in `NEEDS_REVIEW`. An avoidable hold
  requires exactly one matching effect visible to the policy at the horizon.
- Virtual confirmation latency is measured in provider seconds. Wall-clock
  decision time includes local polling and scheduling delays and is recorded
  separately from the Phase 11 performance measurements.

The seed 42 matrix covers eight scenarios per policy.

| Policy | Confirmed single effects | Extra effects | Unresolved | Permanent failures | Mutations | Lookups |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fresh key | 2 / 8 | 5 | 1 | 1 | 14 | 0 |
| Same key | 3 / 8 | 4 | 1 | 1 | 14 | 0 |
| Evidence | 6 / 8 | 0 | 1 | 1 | 9 | 11 |

In E11, the same-key resend arrives after expiry and creates another reservation.
The evidence policy confirms the original with a lookup. In E05, the evidence
policy exhausts its lookup budget and leaves the operation for review.

## Verify

```bash
python -m pytest -q tests/integration/test_comparison.py
python -m pytest -q
```

The comparison adds three integration tests. The full repository suite passed all 367 tests on Ubuntu / WSL2 with
Docker Compose. The live seed 42 run completed all 24 trials and
generated both reports with the results shown above.
