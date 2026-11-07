from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from retry_window_lab.benchmark import aggregate, task_samples


def test_pooled_percentiles_and_throughput_use_different_units_of_observation():
    trials = []
    for rate, values in ((4, [1, 2]), (8, [3, 10])):
        trials.append({
            "slots": 2, "completed": 2, "throughput_tasks_per_second": rate,
            "samples": [{"submission_ms": value, "durable_latency_ms": value,
                         "claim_wait_ms": value, "attempt_ms": value} for value in values],
        })
    row = aggregate(trials)["2"]
    assert row["median_throughput_tasks_per_second"] == 6
    assert row["completed"] == 4
    assert row["pooled_latency_ms"]["durable_latency_ms"] == {"p50": 2, "p95": 10, "p99": 10}


def test_samples_require_one_successful_attempt_and_matching_checksum():
    task_id = uuid4()
    created = datetime.now(UTC)
    expected = {"sha256": "expected", "input_bytes": 1024}
    row = {
        "task_id": task_id, "worker_id": "benchmark/slot-1", "state": "SUCCEEDED",
        "outcome": "SUCCEEDED", "result": expected, "created_at": created,
        "started_at": created + timedelta(milliseconds=20),
        "finished_at": created + timedelta(milliseconds=23),
    }
    submitted = {task_id: (10, expected)}
    sample, = task_samples([row], submitted)
    assert sample["durable_latency_ms"] == 23
    assert sample["claim_wait_ms"] == 20
    assert sample["attempt_ms"] == 3
    with pytest.raises(RuntimeError, match="exactly one attempt"):
        task_samples([row, row], submitted)
    with pytest.raises(RuntimeError, match="did not complete correctly"):
        task_samples([row | {"result": {}}], submitted)
