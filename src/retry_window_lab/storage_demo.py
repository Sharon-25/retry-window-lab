import json

from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.domain import TaskDefinition
from retry_window_lab.storage import TaskRepository


def main() -> None:
    conninfo = application_conninfo()
    definition = TaskDefinition("checksum", "durable-checksum-demo-001", {"text": "abc"})
    first = TaskRepository(conninfo).submit(definition)
    second = TaskRepository(conninfo).submit(definition)
    reread = TaskRepository(conninfo).get(first.task.task_id)
    print(json.dumps({
        "created_on_this_call": first.created,
        "matching_resubmission_reused_task": second.task.task_id == first.task.task_id,
        "readable_through_new_connection": reread == first.task,
        "task": first.task.as_dict(),
        "execution_note": "Accepted durably; no worker execution or Redis publication yet.",
    }, indent=2))


if __name__ == "__main__":
    main()
