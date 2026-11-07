"""Measure an isolated checksum workload through the task API and queue."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import hashlib
import json
from math import ceil
import os
from pathlib import Path
import platform
import socket
from statistics import median
import subprocess
import sys
from tempfile import TemporaryFile
from threading import Event, Thread
from time import monotonic, sleep
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from redis import Redis
import uvicorn

from retry_window_lab.api import create_app
from retry_window_lab.capacity import set_limit
from retry_window_lab.config import postgres_conninfo, redis_url
from retry_window_lab.db import connect, table
from retry_window_lab.dispatcher import OutboxDispatcher
from retry_window_lab.migrate import migrate
from retry_window_lab.provision import grant_application_tables
from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.storage import TaskRepository
from retry_window_lab.worker import MAX_CONCURRENCY


METRICS = ("submission_ms", "durable_latency_ms", "claim_wait_ms", "attempt_ms")
COMPLETION_POLL_SECONDS = 0.025


def percentiles(samples):
    ordered = sorted(samples)
    return {f"p{p}": round(ordered[ceil(p * len(ordered) / 100) - 1], 3)
            for p in (50, 95, 99)}


@contextmanager
def api_server(store):
    server = uvicorn.Server(uvicorn.Config(create_app(store), access_log=False, log_level="warning"))
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        base_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        thread = Thread(target=server.run, kwargs={"sockets": [listener]}, name="benchmark-api")
        thread.start()
        try:
            deadline = monotonic() + 10
            while thread.is_alive() and monotonic() < deadline:
                try:
                    with urlopen(f"{base_url}/health", timeout=0.5) as response:
                        if json.load(response) == {"status": "ok"}:
                            break
                except OSError:
                    sleep(0.025)
            else:
                raise RuntimeError("Benchmark task API did not become ready")
            yield base_url
        finally:
            server.should_exit = True
            thread.join()


@contextmanager
def worker_process(schema, stream, slots):
    with TemporaryFile() as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "retry_window_lab.worker", "--schema", schema,
             "--stream", stream, "--concurrency", str(slots), "--worker-id", "benchmark"],
            stdin=subprocess.DEVNULL, stdout=log, stderr=log,
        )
        try:
            yield process
        except Exception:
            if process.poll() is not None:
                log.seek(max(0, log.seek(0, os.SEEK_END) - 4096))
                print(log.read().decode(errors="replace"), file=sys.stderr)
            raise
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


@contextmanager
def dispatch_loop(dispatcher, poll_seconds):
    stop = Event()

    def run():
        while not stop.is_set():
            dispatcher.dispatch_once()
            stop.wait(poll_seconds)

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="benchmark-dispatcher") as executor:
        future = executor.submit(run)
        try:
            yield future
        finally:
            stop.set()


def submit(base_url, prefix, index, payload_bytes, deadline):
    text = f"{index:08d}".ljust(payload_bytes, "x")
    body = {"kind": "checksum", "submission_key": f"{prefix}-{index}", "payload": {"text": text}}
    request = Request(f"{base_url}/tasks", data=json.dumps(body).encode(),
                      headers={"Content-Type": "application/json"})
    start = monotonic()
    if start >= deadline:
        raise TimeoutError("Benchmark submission exceeded the batch deadline")
    with urlopen(request, timeout=min(10, deadline - start)) as response:
        status = response.status
        result = json.load(response)
    elapsed_ms = (monotonic() - start) * 1000
    if status != 202 or result["created"] is not True:
        raise RuntimeError("Expected HTTP 202 for a new benchmark task")
    expected = {"sha256": hashlib.sha256(text.encode()).hexdigest(), "input_bytes": payload_bytes}
    return UUID(result["task"]["task_id"]), (elapsed_ms, expected)


def task_samples(rows, submissions):
    if len(rows) != len(submissions) or {row["task_id"] for row in rows} != set(submissions):
        raise RuntimeError("Expected exactly one attempt for every checksum task")
    samples = []
    for row in rows:
        submission_ms, expected = submissions[row["task_id"]]
        if row["state"] != "SUCCEEDED" or row["outcome"] != "SUCCEEDED" or row["result"] != expected:
            raise RuntimeError(f"Checksum task {row['task_id']} did not complete correctly")
        wait_ms = (row["started_at"] - row["created_at"]).total_seconds() * 1000
        attempt_ms = (row["finished_at"] - row["started_at"]).total_seconds() * 1000
        if wait_ms < 0 or attempt_ms < 0:
            raise RuntimeError("PostgreSQL timestamps were out of order during the run")
        samples.append({
            "task_id": str(row["task_id"]), "worker_id": row["worker_id"],
            "submission_ms": submission_ms, "durable_latency_ms": wait_ms + attempt_ms,
            "claim_wait_ms": wait_ms, "attempt_ms": attempt_ms,
        })
    return samples


def batch(base_url, app, schema, prefix, count, settings, check_services):
    started = monotonic()
    deadline = started + settings.timeout
    with ThreadPoolExecutor(max_workers=settings.clients) as executor:
        futures = [executor.submit(submit, base_url, prefix, index, settings.payload_bytes, deadline)
                   for index in range(count)]
        submissions = dict(future.result() for future in futures)
    if len(submissions) != count:
        raise RuntimeError("API returned duplicate task IDs")
    with connect(app) as connection:
        query = sql.SQL("""
            SELECT t.task_id, t.state, t.result, t.created_at,
                   a.worker_id, a.started_at, a.finished_at, a.outcome
            FROM {} t LEFT JOIN {} a ON t.task_id = a.task_id
            WHERE t.task_id = ANY(%s) ORDER BY t.submission_key, a.number
        """).format(table(schema, "tasks"), table(schema, "attempts"))
        while monotonic() < deadline:
            check_services()
            rows = connection.execute(query, (list(submissions),)).fetchall()
            if any(row["state"] in ("FAILED", "NEEDS_REVIEW") for row in rows):
                raise RuntimeError("A benchmark checksum task failed or required review")
            if len(rows) >= count and all(row["state"] == "SUCCEEDED" for row in rows):
                observed = monotonic()
                if observed > deadline:
                    break
                samples = task_samples(rows, submissions)
                elapsed = observed - started
                return {
                    "completed": count, "wall_seconds": round(elapsed, 6),
                    "throughput_tasks_per_second": round(count / elapsed, 3),
                    "worker_completions": dict(sorted(Counter(
                        sample["worker_id"] for sample in samples
                    ).items())),
                    "latency_ms": {metric: percentiles([sample[metric] for sample in samples])
                                   for metric in METRICS},
                    "samples": samples,
                }
            sleep(COMPLETION_POLL_SECONDS)
    raise TimeoutError(f"Benchmark {prefix} batch did not finish within {settings.timeout} seconds")


def trial(admin, app, client, settings, slots):
    token = uuid4().hex
    schema = f"rwl_bench_{token}"
    stream = f"rwl:benchmark:{token}"
    try:
        migrate(admin, schema)
        set_limit(admin, settings.tasks + settings.warmup, schema)
        with connect(admin) as connection, connection.transaction():
            grant_application_tables(connection, schema, conninfo_to_dict(app)["user"])
        store = TaskRepository(app, schema)
        dispatcher = OutboxDispatcher(app, client, schema, stream)
        with ExitStack() as stack:
            base_url = stack.enter_context(api_server(store))
            worker = stack.enter_context(worker_process(schema, stream, slots))
            dispatch = stack.enter_context(dispatch_loop(dispatcher, settings.dispatch_poll_ms / 1000))

            def check_services():
                if worker.poll() is not None:
                    raise RuntimeError(f"Benchmark worker exited with code {worker.returncode}")
                if dispatch.done():
                    dispatch.result()
                    raise RuntimeError("Benchmark dispatcher exited")

            batch(base_url, app, schema, "warmup", settings.warmup, settings, check_services)
            result = batch(base_url, app, schema, "measured", settings.tasks, settings, check_services)
            check_services()
            return result
    finally:
        try:
            client.delete(stream)
        finally:
            with connect(admin) as connection:
                connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema)
                ))


def aggregate(trials):
    summary = {}
    for slots in sorted({row["slots"] for row in trials}):
        selected = [row for row in trials if row["slots"] == slots]
        rates = [row["throughput_tasks_per_second"] for row in selected]
        summary[str(slots)] = {
            "runs": len(selected),
            "completed": sum(row["completed"] for row in selected),
            "median_throughput_tasks_per_second": round(median(rates), 3),
            "min_throughput_tasks_per_second": min(rates),
            "max_throughput_tasks_per_second": max(rates),
            "pooled_latency_ms": {
                metric: percentiles([sample[metric] for row in selected for sample in row["samples"]])
                for metric in METRICS
            },
        }
    return summary


def markdown(report):
    settings = report["settings"]
    lines = [
        "# Checksum queue benchmark", "",
        f"{settings['tasks']} measured tasks and {settings['warmup']} warm-up tasks per run.",
        f"{settings['clients']} concurrent submitters, {settings['payload_bytes']}-byte checksum inputs, "
        f"{settings['dispatch_poll_ms']} ms dispatcher polling interval.", "",
        "| Slots | Runs | Completed | Median tasks/s | Min–max tasks/s | Durable p50 ms | p95 ms | p99 ms |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for slots, row in report["summary"].items():
        p = row["pooled_latency_ms"]["durable_latency_ms"]
        lines.append(
            f"| {slots} | {row['runs']} | {row['completed']} | {row['median_throughput_tasks_per_second']} | "
            f"{row['min_throughput_tasks_per_second']}–{row['max_throughput_tasks_per_second']} | "
            f"{p['p50']} | {p['p95']} | {p['p99']} |"
        )
    lines += [
        "", "| Slots | HTTP submission p95 ms | Created to claim p95 ms | Claim to finish p95 ms |",
        "| ---: | ---: | ---: | ---: |",
    ]
    for slots, row in report["summary"].items():
        stats = row["pooled_latency_ms"]
        lines.append(f"| {slots} | {stats['submission_ms']['p95']} | "
                     f"{stats['claim_wait_ms']['p95']} | {stats['attempt_ms']['p95']} |")
    return "\n".join(lines) + "\n"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New report directory")
    parser.add_argument("--slots", type=int, nargs="+", default=[1, 2, 4],
                        choices=range(1, MAX_CONCURRENCY + 1))
    parser.add_argument("--tasks", type=int, default=120)
    parser.add_argument("--warmup", type=int, default=24)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--dispatch-poll-ms", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=60, help="Deadline in seconds per batch")
    settings = parser.parse_args()
    for name, minimum, maximum in (
        ("tasks", 1, 5000), ("warmup", 1, 1000), ("clients", 1, 32), ("repeats", 1, 10),
        ("payload_bytes", 8, 16384), ("dispatch_poll_ms", 100, 2000), ("timeout", 1, 600),
    ):
        if not minimum <= getattr(settings, name) <= maximum:
            parser.error(f"--{name.replace('_', '-')} must be between {minimum} and {maximum}")
    if len(set(settings.slots)) != len(settings.slots):
        parser.error("--slots must contain distinct values")
    if settings.output.exists():
        parser.error("--output must be a new directory")
    return settings


def main():
    settings = parse_args()
    admin, app = postgres_conninfo(), application_conninfo()
    with connect(app) as connection:
        postgres = connection.execute("SHOW server_version").fetchone()["server_version"]
    trials = []
    with Redis.from_url(redis_url(), decode_responses=True, socket_connect_timeout=2,
                        socket_timeout=2) as client:
        environment = {
            "python": platform.python_version(), "platform": platform.platform(),
            "logical_cpus": os.cpu_count(), "postgresql": postgres,
            "redis": client.info("server")["redis_version"],
        }
        for index in range(settings.repeats):
            # Rotate the order to spread the effect of machine warm-up across configurations.
            offset = index % len(settings.slots)
            for slots in settings.slots[offset:] + settings.slots[:offset]:
                print(f"Repeat {index + 1}/{settings.repeats}, {slots} worker slots", flush=True)
                result = trial(admin, app, client, settings, slots)
                trials.append({"repeat": index + 1, "slots": slots, **result})
    report = {
        "format_version": 1, "environment": environment,
        "settings": {name: value for name, value in vars(settings).items() if name != "output"},
        "completion_poll_ms": COMPLETION_POLL_SECONDS * 1000,
        "summary": aggregate(trials), "trials": trials,
    }
    settings.output.mkdir(parents=True)
    (settings.output / "benchmark.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (settings.output / "benchmark.md").write_text(markdown(report), encoding="utf-8")
    print(markdown(report), end="")
    print(f"Reports written to {settings.output}")


if __name__ == "__main__":
    main()
