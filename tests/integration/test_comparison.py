from uuid import uuid4

from fastapi.testclient import TestClient
import pytest
from psycopg import sql

from retry_window_lab.comparison import SCENARIOS, Trial, compare, markdown
from retry_window_lab.config import postgres_conninfo
from retry_window_lab.db import connect
from retry_window_lab.provider import ProviderRepository
from retry_window_lab.provider_api import create_provider_app
from retry_window_lab.provider_client import Reply
from retry_window_lab.provider_migrate import migrate

pytestmark = pytest.mark.integration


@pytest.fixture
def simulator():
    conninfo = postgres_conninfo()
    schema = f"rwl_test_{uuid4().hex}"
    try:
        migrate(conninfo, schema)
        operator = ProviderRepository(conninfo, schema)
        with TestClient(create_provider_app(operator)) as api:
            class Client:
                base_url = "http://127.0.0.1:8001"

                def reserve(self, body):
                    response = api.post("/reservations", json=body)
                    return Reply(response.status_code, response.json())

                def lookup(self, experiment_id, operation_id):
                    response = api.get(f"/experiments/{experiment_id}/operations/{operation_id}")
                    return Reply(response.status_code, response.json())

            yield conninfo, operator, Client()
    finally:
        with connect(conninfo) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            ))


def test_comparison_matrix_checks_actual_provider_effects(simulator):
    conninfo, operator, client = simulator
    report = compare(conninfo, conninfo, client.base_url, 42, operator=operator, client=client)
    trials = report["trials"]
    assert len(trials) == len({row["experiment_id"] for row in trials}) == 24
    by_case = {(row["scenario"], row["policy"]): row for row in trials}
    assert by_case["E02", "fresh-key"]["duplicate_effects"] == 1
    assert by_case["E02", "same-key"]["confirmed_single_effect"] is True
    assert by_case["E03", "same-key"]["duplicate_effects"] == 1
    assert [event["provider_second"] for event in by_case["E04", "evidence"]["trace"]] == [0, 61, 62, 63]
    unavailable = by_case["E05", "evidence"]
    assert unavailable["state"] == "NEEDS_REVIEW" and unavailable["effects"] == 1
    assert unavailable["lookup_calls"] == 5 and not unavailable["avoidable_hold_at_horizon"]
    late = by_case["E11", "same-key"]
    assert late["trace"][-1]["provider_second"] == 59
    assert late["trace"][-1]["http_status"] == 202
    assert late["reservation_acceptance_seconds"] == [0, 61]
    assert by_case["E11", "evidence"]["mutation_calls"] == 1
    assert [
        (row["confirmed_single_effect_completions"], row["duplicate_effects"],
         row["unresolved"], row["known_permanent_failure"])
        for row in report["summary"].values()
    ] == [(2, 5, 1, 1), (3, 4, 1, 1), (6, 0, 1, 1)]
    assert "| evidence | 6 | 6 / 8 | 0 | 1 | 1 | 9 | 11 |" in markdown(report)


def test_seed_reproduces_no_effect_retry_decision(simulator):
    conninfo, operator, client = simulator
    selected = tuple(case for case in SCENARIOS if case.id == "E10")
    runs = [compare(conninfo, conninfo, client.base_url, 42, selected,
                    operator=operator, client=client) for _ in range(2)]
    assert runs[0]["summary"] == runs[1]["summary"]
    first, second = (report["trials"][-1] for report in runs)
    assert first["experiment_id"] != second["experiment_id"]
    assert first["evidence_decisions"] == second["evidence_decisions"]
    assert first["evidence_decisions"][0]["reason"] == "SETTLED_NO_EFFECT"
    assert 0 <= first["evidence_decisions"][0]["details"]["retry_delay_seconds"] <= 1


def test_unexpected_service_outage_aborts_the_fault_trial(simulator):
    _, operator, _ = simulator

    class UnavailableClient:
        def reserve(self, body):
            return Reply(503, {"detail": "provider_database_unavailable"})

    trial = Trial(operator, UnavailableClient(), SCENARIOS[1], uuid4())
    with pytest.raises(RuntimeError, match="simulated response loss after commit"):
        trial.reserve(trial.body)
