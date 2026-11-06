from types import SimpleNamespace
from uuid import uuid4

import pytest
from redis.exceptions import ConnectionError, ResponseError

from retry_window_lab.domain import TaskDefinition
from retry_window_lab.local import KnownTaskFailure
from retry_window_lab.ownership import Claim, Notification, bounded_int, worker_identity
from retry_window_lab.worker import LeaseHeartbeat, Worker, execute_claim


def message():
    return {'protocol_version': '1', 'event_id': str(uuid4()), 'task_id': str(uuid4()),
            'delivery_generation': '1', 'event_type': 'TASK_READY'}


def claim():
    return Claim(uuid4(), uuid4(), 1, TaskDefinition('checksum', 'unit', {'text': 'abc'}))


@pytest.mark.parametrize('field,value', [
    ('protocol_version', '2'), ('event_id', 'bad'), ('task_id', 'bad'),
    ('delivery_generation', '0'), ('delivery_generation', '-1'),
    ('delivery_generation', '1.0'), ('delivery_generation', ' 1'),
    ('delivery_generation', '١'), ('delivery_generation', str(2**63)),
    ('delivery_generation', '9'*100), ('event_type', 'OTHER'),
    ('task_id', b'bytes'), ('extra', 'x'),
])
def test_bad_notifications(field, value):
    fields = message()
    fields[field] = value
    with pytest.raises(ValueError):
        Notification.parse(fields)


def test_missing_field():
    fields = message()
    del fields['task_id']
    with pytest.raises(ValueError):
        Notification.parse(fields)


def test_valid_notification():
    fields = message()
    parsed = Notification.parse(fields)
    assert str(parsed.task_id) == fields['task_id']
    assert str(parsed.event_id) == fields['event_id']
    assert parsed.generation == 1


@pytest.mark.parametrize('value', [True, 0, -1, 3601, 3.0, '30'])
def test_bad_lease(value):
    with pytest.raises(ValueError):
        bounded_int(value, 3, 3600, 'lease')


@pytest.mark.parametrize('value', ['', ' ', 'x'*129, None])
def test_bad_worker_identity(value):
    with pytest.raises(ValueError):
        worker_identity(value)


class Repository:
    def __init__(self):
        self.calls = []
        self.claim_result = ('claimed', claim())
        self.finish_result = True

    def claim(self, *args):
        self.calls.append('claim')
        return self.claim_result

    def renew(self, *args):
        return True

    def finish(self, current, outcome, result):
        self.calls.append(('finish', outcome, result))
        return self.finish_result


class Client:
    def __init__(self, repository, fields=None):
        self.repository = repository
        self.fields = fields if fields is not None else message()
        self.reclaimed = []
        self.new = [('0-1', self.fields)]
        self.acked = []

    def xautoclaim(self, *args, **kwargs):
        return ['0-0', self.reclaimed, []]

    def xreadgroup(self, *args, **kwargs):
        return [('stream', self.new)] if self.new else []

    def xack(self, stream, group, message_id):
        self.repository.calls.append('ack')
        self.acked.append(message_id)
        return 1


@pytest.mark.parametrize('status', ['obsolete', 'represented'])
def test_durable_nonclaim_acknowledged(status):
    repo = Repository()
    repo.claim_result = status, None
    client = Client(repo)
    assert Worker(repo, client, 'unit').tick()['status'] == status
    assert repo.calls == ['claim', 'ack']


def test_claim_is_acknowledged_before_execution():
    repo = Repository()
    assert Worker(repo, Client(repo), 'unit').tick()['status'] == 'SUCCEEDED'
    assert repo.calls[:2] == ['claim', 'ack']
    assert repo.calls[2][0:2] == ('finish', 'SUCCEEDED')
    assert repo.calls[2][2]['input_bytes'] == 3


def test_unsupported_not_acknowledged():
    repo = Repository()
    repo.claim_result = 'unsupported', None
    client = Client(repo)
    assert Worker(repo, client, 'unit').tick()['status'] == 'unsupported'
    assert client.acked == []


def test_invalid_notification_ack_without_database():
    repo = Repository()
    client = Client(repo, {'invalid': 'data'})
    assert Worker(repo, client, 'unit').tick()['status'] == 'invalid_notification'
    assert repo.calls == ['ack']


def test_database_error_keeps_pending():
    repo = Repository()
    def fail(*args):
        raise RuntimeError('database failure')
    repo.claim = fail
    client = Client(repo)
    with pytest.raises(RuntimeError):
        Worker(repo, client, 'unit').tick()
    assert client.acked == []


def test_ack_error_does_not_start_handler():
    repo = Repository()
    client = Client(repo)
    def fail(*args):
        raise ConnectionError('ack failure')
    client.xack = fail
    with pytest.raises(ConnectionError):
        Worker(repo, client, 'unit').tick()
    assert repo.calls == ['claim']


def test_idle():
    repo = Repository()
    client = Client(repo)
    client.new = []
    assert Worker(repo, client, 'unit').tick()['status'] == 'idle'
    assert not repo.calls


def test_reclaimed_notifications_are_revalidated():
    repo = Repository()
    repo.claim_result = 'represented', None
    client = Client(repo)
    client.reclaimed = [('0-2', message())]
    assert Worker(repo, client, 'unit').tick()['status'] == 'represented'
    assert client.acked == ['0-2']


@pytest.mark.parametrize('failure,expected', [
    (KnownTaskFailure('sensitive'), 'KNOWN_FAILURE'),
    (ValueError('sensitive'), 'UNKNOWN'),
])
def test_failure_classification(failure, expected):
    repo = Repository()
    def handler(payload):
        raise failure
    assert execute_claim(repo, claim(), 30, handler) == expected
    assert repo.calls == [('finish', expected, None)]


@pytest.mark.parametrize('result', [[], {'x': float('nan')}, {'x': object()}, {'x': '\ud800'}])
def test_invalid_result_is_uncertain(result):
    repo = Repository()
    assert execute_claim(repo, claim(), 30, lambda _: result) == 'UNKNOWN'
    assert repo.calls == [('finish', 'UNKNOWN', None)]


def test_base_exception_leaves_attempt_running():
    repo = Repository()
    def interrupt(payload):
        raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        execute_claim(repo, claim(), 30, interrupt)
    assert not repo.calls


def test_stale_completion():
    repo = Repository()
    repo.finish_result = False
    assert execute_claim(repo, claim(), 30) == 'stale_completion'


@pytest.mark.parametrize('raises', [False, True])
def test_heartbeat_stops_on_failed_renewal(raises):
    repo = Repository()
    def renew(*args):
        if raises:
            raise RuntimeError('database lost')
        return False
    repo.renew = renew
    # Deterministic single heartbeat iteration without wall-clock sleeps.
    heartbeat = LeaseHeartbeat(repo, claim(), 30)
    heartbeat.stop = SimpleNamespace(wait=lambda _: False)
    heartbeat._run()
    assert heartbeat.lost.is_set()


def test_group_already_exists_is_expected():
    repo = Repository()
    client = Client(repo)
    client.xgroup_create = lambda *args, **kwargs: (_ for _ in ()).throw(ResponseError('BUSYGROUP exists'))
    Worker(repo, client, 'unit').ensure_group()
    client.xgroup_create = lambda *args, **kwargs: (_ for _ in ()).throw(ResponseError('NOPERM denied'))
    with pytest.raises(ResponseError):
        Worker(repo, client, 'unit').ensure_group()
