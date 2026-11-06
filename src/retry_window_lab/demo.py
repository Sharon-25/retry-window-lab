import json

from retry_window_lab.domain import TaskDefinition, TaskKind
from retry_window_lab.local import HandlerRegistry, LocalExecutor, checksum_handler


def main() -> None:
    registry = HandlerRegistry()
    registry.register(TaskKind.CHECKSUM, checksum_handler)
    executor = LocalExecutor(registry)
    definition = TaskDefinition("checksum", "demo-checksum-001", {"text": "abc"})
    task = executor.submit_and_run(definition)
    repeated = executor.submit_and_run(definition)
    print(json.dumps({
        "task_id": str(task.task_id),
        "operation_id": str(task.operation_id),
        "state": task.state.value,
        "attempt_count": len(task.attempts),
        "attempt_outcome": task.attempts[0].outcome.value,
        "result": task.result,
        "resubmission_reused_task": repeated.task_id == task.task_id,
    }, indent=2))


if __name__ == "__main__":
    main()
