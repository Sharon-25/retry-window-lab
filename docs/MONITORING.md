# Queue monitoring and correlated recovery history

## Open the dashboard

Start the task API using the Phase 8 database schema and provisioned runtime role.

```bash
python -m uvicorn retry_window_lab.api:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000/dashboard. The root path redirects here. The task API
reference remains available at http://127.0.0.1:8000/docs. The simulator is a
separate API served on port 8001.

The dashboard is a read-only page hosted by FastAPI. It polls its JSON endpoints
every four seconds and includes a manual refresh control. A failed summary
request keeps the last successful values on screen with an unavailable status.

| Read endpoint | Contents |
| --- | --- |
| GET /monitoring/summary | Queue capacity, durable task states, worker activity, recovery counters and the latest 24 updated tasks |
| GET /monitoring/tasks/{task_id} | Task metadata, attempts, provider dispatch markers, observation metadata and recovery decisions |

Select a recent task or enter its full UUID to inspect its history. Each attempt
shows its worker, purpose, lease token and outcome. Its timeline groups provider
dispatches, observations and decisions under the same attempt. The API includes
the decision and observation IDs for exact correlation. The monitoring responses
omit task payloads, submission keys and raw provider request and response bodies.

## Interpret the metrics

Each response reads a repeatable, read-only PostgreSQL snapshot.

- Outstanding includes every nonterminal task, including reservations waiting
  for context and tasks in NEEDS_REVIEW. New submissions return HTTP 429 when
  the outstanding count meets the configured limit.
- Ready counts QUEUED checksums and QUEUED reservations with provider context.
  Awaiting publish counts their current unpublished outbox events.
- Active leases count current attempts with a valid lease at snapshot time.
  Worker activity shows up to 20 worker and purpose pairs with an active lease
  or attempt activity in the last 15 minutes. It describes observed attempts.
- Retry decisions, reconciliation attempts and expired leases are totals across
  stored history. A retry decision records scheduling intent.
- Task states count durable tasks, including completed and failed work. Ready
  work is counted from PostgreSQL because Redis delivers notifications that
  can be replayed and duplicated.

The summary aggregates stored rows on each refresh. Performance measurements
and optimization remain part of the later performance phase.

## Follow the structured logs

The task API, dispatcher, workers and recovery command emit UTC timestamped
JSON events. Use task_id to follow a task across processes. Worker claim and
outcome events also include the attempt ID, worker ID, delivery generation and
notification ID. A reconciliation claim records the attempt and worker.

A recovery_decision event includes the durable decision ID, attempt and
observation references, reason and policy version. Decision events are emitted
after the database transaction commits. The history endpoint provides the
durable record if an event is lost after commit. HTTP 429 submission events
record rejection reason and status.

## Demonstrate with live provider evidence

Start the provider in one terminal.

```bash
python -m uvicorn retry_window_lab.provider_api:app --host 127.0.0.1 --port 8001
```

In a second terminal, activate the project environment and run the demo.

```bash
python -m retry_window_lab.monitoring_demo
```

Wait until the dashboard URL is printed and Uvicorn reports startup. Open
http://127.0.0.1:8002/dashboard. The demo uses an isolated queue schema and
Redis stream. It records a confirmed retry, a successful reconciliation and
an unavailable lookup that exhausts the observation budget. It then fills
the demo's outstanding capacity with queued checksums.

The dashboard should show four of four outstanding slots, three ready tasks,
one task needing review, one retry decision and six reconciliation attempts.
Select the review task and confirm that its final decision is Evidence exhausted.

Ctrl+C shuts down the demo and removes its queue schema and stream. The
simulator keeps the isolated experiment records.

## Verification

All 364 tests passed on Ubuntu / WSL2 with Docker Compose. The browser
demo showed four of four outstanding slots, three ready tasks, one task
needing review, one retry decision and six reconciliation attempts.
The final Evidence exhausted decision matched the structured log by
decision ID, attempt ID and observation ID.
