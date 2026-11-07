"""Compare recovery policies against fresh provider simulator experiments."""

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import platform
from random import Random
from statistics import median
from time import monotonic, sleep
from uuid import uuid4

from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.db import connect, table
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.migrate import migrate
from retry_window_lab.ownership import Notification, OwnershipRepository
from retry_window_lab.provider import ProviderRepository
from retry_window_lab.provider_client import ProviderClient, fingerprint, lookup_evidence, mutation_evidence
from retry_window_lab.provision import grant_application_tables
from retry_window_lab.recovery import RecoveryRepository, RecoveryService
from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.storage import TaskRepository

POLICIES = ("fresh-key", "same-key", "evidence")
WINDOW = 60
SKU = "SKU-BOOK-001"
FINAL_STATES = {"SUCCEEDED", "FAILED", "NEEDS_REVIEW"}


@dataclass(frozen=True)
class Scenario:
    id: str
    title: str
    fault: str
    initial_evidence: str
    followup_at: int = 0
    profile: str = "authoritative"
    visibility_delay: int = 10
    quantity: int = 2
    retry_delivery_delay: int = 0

    @property
    def evaluation_at(self):
        return self.followup_at + 4 if self.followup_at else 0


SCENARIOS = (
    Scenario("E01", "Normal reservation", "NONE", "EFFECT_CONFIRMED"),
    Scenario("E02", "Response lost inside protection", "RESPONSE_LOST", "UNKNOWN", 1),
    Scenario("E03", "Response lost after protection expires", "RESPONSE_LOST", "UNKNOWN", 61),
    Scenario("E04", "Existing effect becomes visible late", "RESPONSE_LOST", "UNKNOWN",
             61, "delayed", visibility_delay=63),
    Scenario("E05", "Expired protection with unavailable lookup", "RESPONSE_LOST", "UNKNOWN",
             61, "unavailable"),
    Scenario("E10", "Provider proves no effect", "REJECT_NO_EFFECT", "SETTLED_NO_EFFECT", 1),
    Scenario("E11", "Retry sent at 59 and delivered at 61", "RESPONSE_LOST", "UNKNOWN",
             59, retry_delivery_delay=2),
    Scenario("E12", "Insufficient stock", "NONE", "PERMANENT_REJECTION", quantity=101),
)


class Trial:
    def __init__(self, operator, client, scenario, operation_id):
        self.operator = operator
        self.client = client
        self.scenario = scenario
        self.experiment_id = operator.create_experiment(
            scenario.profile, WINDOW, scenario.visibility_delay,
        )
        self.context = {
            "experiment_id": self.experiment_id, "profile": scenario.profile,
            "profile_version": 1, "window_seconds": WINDOW,
        }
        self.body = {
            "experiment_id": str(self.experiment_id), "operation_id": str(operation_id),
            "idempotency_key": str(operation_id), "sku": SKU, "quantity": scenario.quantity,
        }
        self.second = 0
        self.mutations = 0
        self.lookups = 0
        self.trace = []
        self.decisions = []

    def advance_to(self, second):
        if second > self.second:
            self.operator.advance(self.experiment_id, second - self.second)
            self.second = second

    def reserve(self, body):
        if any(body[key] != self.body[key] for key in self.body if key != "idempotency_key"):
            raise RuntimeError("Mutation escaped its trial")
        self.mutations += 1
        if self.mutations == 1:
            self.operator.inject(self.experiment_id, self.scenario.fault)
        elif self.mutations == 2 and self.scenario.retry_delivery_delay:
            self.operator.inject(self.experiment_id, "DELAY_DELIVERY", self.scenario.retry_delivery_delay)
        reply = self.client.reserve(body)
        lost_response = self.mutations == 1 and self.scenario.fault == "RESPONSE_LOST"
        if reply.error or (reply.status not in (200, 202, 409) and not lost_response):
            raise RuntimeError("Unexpected provider mutation response")
        reason = mutation_evidence(reply, self.context, body)
        self.trace.append({
            "action": "mutation", "provider_second": self.second, "http_status": reply.status,
            "evidence": reason, "key": body["idempotency_key"],
            "provider_status": (reply.body or {}).get("status"),
        })
        if self.mutations == 1:
            if reason != self.scenario.initial_evidence:
                raise RuntimeError(f"{self.scenario.id} did not reach its initial fault checkpoint")
            if self.scenario.fault == "RESPONSE_LOST" and (
                reply.status != 503 or (reply.body or {}).get("detail") != "simulated_response_lost"
            ):
                raise RuntimeError("Expected simulated response loss after commit")
        elif self.mutations == 2 and self.scenario.retry_delivery_delay:
            if reply.status != 202 or (reply.body or {}).get("status") != "PROCESSING":
                raise RuntimeError("Expected the retry to remain pending until provider time advances")
        return reply

    def lookup(self, experiment_id, operation_id):
        if (str(experiment_id) != str(self.experiment_id)
                or str(operation_id) != self.body["operation_id"]):
            raise RuntimeError("Lookup escaped its trial")
        reply = self.client.lookup(experiment_id, operation_id)
        expected = (reply.status == 503 and (reply.body or {}).get("detail") == "lookup_unavailable"
                    if self.scenario.profile == "unavailable" else reply.status == 200)
        if reply.error or not expected:
            raise RuntimeError("Unexpected provider lookup response")
        self.lookups += 1
        self.trace.append({
            "action": "lookup", "provider_second": self.second, "http_status": reply.status,
            "evidence": lookup_evidence(reply, self.context, self.body),
        })
        return reply


def baseline(trial, policy):
    body = trial.body.copy()
    uncertain = False
    for attempt in range(1, 4):
        reply = trial.reserve(body)
        reason = mutation_evidence(reply, trial.context, body)
        if reason == "EFFECT_CONFIRMED":
            return "SUCCEEDED", reason
        if reason == "PERMANENT_REJECTION":
            return ("NEEDS_REVIEW" if uncertain else "FAILED"), reason
        if reply.status == 202 or reason == "PROVIDER_CONFLICT":
            return "NEEDS_REVIEW", "REQUEST_PENDING" if reply.status == 202 else reason
        uncertain |= reason == "UNKNOWN"
        if attempt < 3:
            trial.advance_to(trial.scenario.followup_at if attempt == 1 else trial.second + 1)
            if policy == "fresh-key":
                body["idempotency_key"] = str(uuid4())
    if uncertain:
        return "NEEDS_REVIEW", "MUTATION_BUDGET_EXHAUSTED"
    return "FAILED", "MUTATION_BUDGET_EXHAUSTED"


def wait_for(operation, pending):
    deadline = monotonic() + 10
    while monotonic() < deadline:
        result = operation()
        if result != pending:
            return result
        sleep(0.03)
    raise RuntimeError("The database did not make the comparison action due")


def evidence(trial, task, conninfo, schema, seed):
    ownership = OwnershipRepository(conninfo, schema)
    recovery = RecoveryRepository(conninfo, schema, rng=Random(f"{seed}:{trial.scenario.id}"))
    recovery.bind(task.task_id, trial.experiment_id, trial.client.base_url)
    scanner = RecoveryService(recovery, lambda _url: trial, worker_id="comparison-recovery")

    def mutate():
        with connect(conninfo) as connection:
            event = connection.execute(sql.SQL("""
                SELECT event_id, task_id, delivery_generation FROM {}
                WHERE task_id = %s ORDER BY delivery_generation DESC LIMIT 1
            """).format(table(schema, "outbox")), (task.task_id,)).fetchone()
        notification = Notification(event["event_id"], event["task_id"], event["delivery_generation"])
        status, claim = ownership.claim(notification, "comparison-worker")
        if status != "claimed":
            raise RuntimeError(f"Evidence trial could not claim its task: {status}")
        dispatch = recovery.begin_dispatch(claim)
        if dispatch is None:
            raise RuntimeError("Evidence trial lost its dispatch claim")
        reply = trial.reserve(dispatch.body)
        return recovery.complete_mutation(claim, dispatch, reply)

    state = mutate()
    if state not in FINAL_STATES:
        trial.advance_to(trial.scenario.followup_at)
    for _ in range(7):  # At most two more mutations and five observations.
        if state in FINAL_STATES:
            break
        if state == "RETRY_WAIT":
            wait_for(lambda: recovery.schedule_due(limit=1), 0)
            state = mutate()
        elif state == "RECONCILING":
            state = wait_for(scanner.run_once, "idle")
        else:
            raise RuntimeError(f"Unexpected comparison task state: {state}")
        if state not in FINAL_STATES:
            trial.advance_to(trial.second + 1)
    if state not in FINAL_STATES:
        raise RuntimeError("Evidence trial did not reach a terminal or review state")
    audit = recovery.audit(task.task_id)
    trial.decisions = [
        {"reason": row["reason"], "policy_version": row["policy_version"],
         "details": row["details"]} for row in audit["decisions"]
    ]
    return state, trial.decisions[-1]["reason"]


def evaluate(trial, state, reason, decision_second, elapsed_ms):
    # Each tick settles delayed delivery at its intended second before inspection.
    for second in range(trial.second + 1, trial.scenario.evaluation_at + 1):
        trial.advance_to(second)
    if trial.second != trial.scenario.evaluation_at:
        raise RuntimeError("Policy exceeded the shared scenario horizon")
    args = (trial.experiment_id, trial.body["operation_id"])
    with connect(trial.operator.conninfo) as connection:
        requests = connection.execute(sql.SQL("""
            SELECT status, due_at, accepted_at FROM {} WHERE experiment_id = %s
            AND operation_id = %s ORDER BY due_at, request_id
        """).format(table(trial.operator.schema, "requests")), args).fetchall()
        reservations = connection.execute(sql.SQL("""
            SELECT fingerprint, accepted_at, visible_at FROM {} WHERE experiment_id = %s
            AND operation_id = %s ORDER BY accepted_at, reservation_id
        """).format(table(trial.operator.schema, "reservations")), args).fetchall()
    if len(requests) != trial.mutations or any(row["status"] == "PROCESSING" for row in requests):
        raise RuntimeError("Provider ledger did not settle all trial requests")
    if trial.scenario.fault == "RESPONSE_LOST" and not any(
        row["accepted_at"] == 0 for row in reservations
    ):
        raise RuntimeError("Response-loss trial lacked its initial committed effect")
    effects = len(reservations)
    matched = effects == 1 and reservations[0]["fingerprint"] == fingerprint(trial.body)
    confirmed = state == "SUCCEEDED" and matched
    return {
        "state": state, "final_reason": reason, "decision_provider_second": decision_second,
        "evaluation_provider_second": trial.second, "decision_wall_ms": elapsed_ms,
        "confirmed_single_effect": confirmed,
        "confirmed_latency_provider_seconds": decision_second if confirmed else None,
        "effects": effects, "duplicate_effects": max(0, effects - 1),
        "unresolved": state == "NEEDS_REVIEW",
        "known_permanent_failure": state == "FAILED" and reason == "PERMANENT_REJECTION" and effects == 0,
        "avoidable_hold_at_horizon": (
            state == "NEEDS_REVIEW" and matched and trial.scenario.profile != "unavailable"
            and reservations[0]["visible_at"] <= trial.second
        ),
        "mutation_calls": trial.mutations, "lookup_calls": trial.lookups,
        "reservation_acceptance_seconds": [row["accepted_at"] for row in reservations],
        "settled_requests": requests, "evidence_decisions": trial.decisions,
        "trace": trial.trace,
    }


def totals(results):
    summary = {}
    for policy in POLICIES:
        rows = [row for row in results if row["policy"] == policy]
        confirmed = sum(row["confirmed_single_effect"] for row in rows)
        latencies = [row["confirmed_latency_provider_seconds"] for row in rows
                     if row["confirmed_single_effect"]]
        summary[policy] = {
            "trials": len(rows), "reported_successes": sum(row["state"] == "SUCCEEDED" for row in rows),
            "confirmed_single_effect_completions": confirmed,
            "confirmed_completion_rate": confirmed / len(rows),
            "median_confirmed_latency_provider_seconds": median(latencies) if latencies else None,
            **{field: sum(row[field] for row in rows) for field in (
                "duplicate_effects", "unresolved", "known_permanent_failure",
                "avoidable_hold_at_horizon", "mutation_calls", "lookup_calls",
            )},
        }
    return summary


def compare(admin, app, base_url, seed, scenarios=SCENARIOS, *, operator=None, client=None):
    if not scenarios:
        raise ValueError("Select at least one scenario")
    operator = operator or ProviderRepository(admin)
    client = client or ProviderClient(base_url)
    schema = f"rwl_compare_{uuid4().hex}"
    role = conninfo_to_dict(app)["user"]
    results = []
    try:
        migrate(admin, schema)
        with connect(admin) as connection, connection.transaction():
            grant_application_tables(connection, schema, role)
        store = TaskRepository(app, schema)
        for scenario in scenarios:
            for policy in POLICIES:
                task = None
                if policy == "evidence":
                    task = store.submit(TaskDefinition("reserve_inventory", scenario.id, {
                        "sku": SKU, "quantity": scenario.quantity, "provider_profile": scenario.profile,
                    })).task
                trial = Trial(operator, client, scenario, task.operation_id if task else uuid4())
                started = monotonic()
                if task is None:
                    state, reason = baseline(trial, policy)
                else:
                    state, reason = evidence(trial, task, app, schema, seed)
                elapsed_ms = round((monotonic() - started) * 1000, 3)
                results.append({
                    "scenario": scenario.id, "policy": policy,
                    "experiment_id": str(trial.experiment_id),
                    "operation_id": trial.body["operation_id"],
                    **evaluate(trial, state, reason, trial.second, elapsed_ms),
                })
    finally:
        with connect(admin) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            ))
    with connect(admin) as connection:
        postgres = connection.execute("SHOW server_version").fetchone()["server_version"]
    return {
        "format_version": 1, "seed": seed, "window_seconds": WINDOW, "initial_stock_per_sku": 100,
        "environment": {"python": platform.python_version(), "postgresql": postgres},
        "scenarios": [asdict(scenario) | {"evaluation_at": scenario.evaluation_at}
                      for scenario in scenarios],
        "summary": totals(results), "trials": results,
    }


def markdown(report):
    lines = [
        "# Retry policy comparison", "",
        f"Seed {report['seed']}. Protection window {report['window_seconds']} provider seconds.",
        "Each policy used a fresh experiment with 100 units per SKU.", "",
        "| Policy | Reported success | Confirmed single effect | Extra effects | Unresolved | Permanent failures | Mutations | Lookups |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for policy, row in report["summary"].items():
        lines.append(
            f"| {policy} | {row['reported_successes']} | {row['confirmed_single_effect_completions']}"
            f" / {row['trials']} | {row['duplicate_effects']} | {row['unresolved']} | "
            f"{row['known_permanent_failure']} | {row['mutation_calls']} | {row['lookup_calls']} |"
        )
    lines += [
        "", "Confirmed completion requires a successful task outcome and exactly one matching"
        " reservation in the provider ledger. The JSON includes individual fault traces,"
        " virtual confirmation latency and separately recorded wall-clock decision times.", "",
        "| Scenario | Policy | Task outcome | Final reason | Effects | Decision time (provider s) |",
        "| --- | --- | --- | --- | ---: | ---: |",
    ]
    for row in report["trials"]:
        lines.append(
            f"| {row['scenario']} | {row['policy']} | {row['state']} | {row['final_reason']} |"
            f" {row['effects']} | {row['decision_provider_second']} |"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="A new directory for the report")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--base-url")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("--output must be a new directory")
    admin = postgres_conninfo()
    base = args.base_url or os.environ.get("RWL_PROVIDER_URL", "http://127.0.0.1:8001")
    report = compare(admin, application_conninfo(), base, args.seed)
    args.output.mkdir(parents=True)
    (args.output / "comparison.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (args.output / "comparison.md").write_text(markdown(report), encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))
    print(f"Reports written to {args.output}")


if __name__ == "__main__":
    main()
