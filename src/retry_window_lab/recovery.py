"""Evidence policy v1. Only proven no-effect executions may be retried."""
from dataclasses import dataclass
import random
from uuid import UUID,uuid4

from psycopg import sql
from psycopg.types.json import Jsonb

from retry_window_lab.db import DEFAULT_SCHEMA,connect,table,validate_schema
from retry_window_lab.domain import TaskDefinition,TaskState,validate_transition
from retry_window_lab.ownership import Claim,OwnershipRepository,bounded_int,worker_identity
from retry_window_lab.provider_client import ProviderClient,lookup_evidence,mutation_evidence,provider_url


class ContextConflict(ValueError):
    pass


@dataclass(frozen=True)
class Dispatch:
    dispatch_id: UUID
    context: dict
    body: dict


class RecoveryRepository:
    def __init__(self,conninfo,schema=DEFAULT_SCHEMA,rng=None):
        self.conninfo,self.schema=conninfo,validate_schema(schema)
        self.rng=random.Random() if rng is None else rng
        self.ownership=OwnershipRepository(conninfo,schema)

    def t(self,name):
        return table(self.schema,name)

    def delay(self,attempt):
        return self.rng.uniform(0,min(8,2**max(0,attempt-1)))

    def bind(self,task_id,experiment_id,base_url='http://127.0.0.1:8001',window_seconds=60,profile_version=1):
        if not isinstance(task_id,UUID) or not isinstance(experiment_id,UUID):
            raise ValueError('Task and experiment must be UUIDs')
        base_url=provider_url(base_url)
        bounded_int(window_seconds,1,3600,'window_seconds')
        bounded_int(profile_version,1,1,'profile_version')
        with connect(self.conninfo) as c,c.transaction():
            task=c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s FOR UPDATE').format(self.t('tasks')),(task_id,)).fetchone()
            if task is None:
                raise LookupError('Unknown task')
            if task['kind']!='reserve_inventory':
                raise ValueError('Only reservation tasks have provider context')
            definition=TaskDefinition(task['kind'],task['submission_key'],task['payload'])
            values=(experiment_id,base_url,definition.payload['provider_profile'],profile_version,window_seconds)
            existing=c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(self.t('provider_context')),(task_id,)).fetchone()
            if existing:
                observed=tuple(existing[key] for key in ('experiment_id','provider_url','profile','profile_version','window_seconds'))
                if observed!=values:
                    raise ContextConflict('Provider context is immutable')
                return False
            if task['state']!='QUEUED' or task['attempt_count']!=0:
                raise ContextConflict('Bind context before claiming a task')
            c.execute(sql.SQL('''INSERT INTO {}(task_id,experiment_id,provider_url,profile,profile_version,window_seconds)
                VALUES(%s,%s,%s,%s,%s,%s)''').format(self.t('provider_context')),(task_id,*values))
            generation=task['delivery_generation']+1
            c.execute(sql.SQL('UPDATE {} SET delivery_generation=%s,updated_at=clock_timestamp() WHERE task_id=%s')
                      .format(self.t('tasks')),(generation,task_id))
            c.execute(sql.SQL("INSERT INTO {}(event_id,task_id,delivery_generation,event_type) VALUES(%s,%s,%s,'TASK_READY')")
                      .format(self.t('outbox')),(uuid4(),task_id,generation))
            return True

    def _context(self,c,task_id):
        return c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(self.t('provider_context')),(task_id,)).fetchone()

    def _body(self,task,context):
        return {'experiment_id':str(context['experiment_id']),'operation_id':str(task['operation_id']),
                'idempotency_key':str(task['operation_id']),'sku':task['payload']['sku'],
                'quantity':task['payload']['quantity']}

    def begin_dispatch(self,claim):
        with connect(self.conninfo) as c,c.transaction():
            if not self.ownership._current(c,claim):
                return None
            task=c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(self.t('tasks')),(claim.task_id,)).fetchone()
            context=self._context(c,claim.task_id)
            if task['state']!='RUNNING' or task['kind']!='reserve_inventory' or context is None:
                return None
            existing=c.execute(sql.SQL('SELECT 1 FROM {} WHERE attempt_id=%s').format(self.t('provider_dispatches')),(claim.attempt_id,)).fetchone()
            if existing:
                return None  # A dispatch marker is never reused to send another POST.
            dispatch=Dispatch(uuid4(),context,self._body(task,context))
            c.execute(sql.SQL('''INSERT INTO {}(dispatch_id,task_id,attempt_id,lease_token,request_body)
                VALUES(%s,%s,%s,%s,%s)''').format(self.t('provider_dispatches')),
                (dispatch.dispatch_id,claim.task_id,claim.attempt_id,claim.token,Jsonb(dispatch.body)))
            c.execute(sql.SQL('UPDATE {} SET first_dispatch_at=COALESCE(first_dispatch_at,clock_timestamp()) WHERE task_id=%s')
                      .format(self.t('provider_context')),(claim.task_id,))
            return dispatch

    def _observation(self,c,claim,source,reply):
        observation_id=uuid4()
        c.execute(sql.SQL('''INSERT INTO {}(observation_id,task_id,attempt_id,source,http_status,error_code,body)
            VALUES(%s,%s,%s,%s,%s,%s,%s)''').format(self.t('provider_observations')),
            (observation_id,claim.task_id,claim.attempt_id,source,reply.status,reply.error,
             None if reply.body is None else Jsonb(reply.body)))
        return observation_id

    def _decision(self,c,task_id,attempt_id,reason,observation_id=None,details=None):
        c.execute(sql.SQL('''INSERT INTO {}(decision_id,task_id,attempt_id,observation_id,reason,details)
            VALUES(%s,%s,%s,%s,%s,%s)''').format(self.t('recovery_decisions')),
            (uuid4(),task_id,attempt_id,observation_id,reason,Jsonb({} if details is None else details)))

    def _close(self,c,task,claim,state,outcome,code,reason,observation_id=None,result=None,delay=0):
        if task['state']!=state:
            validate_transition(TaskState(task['state']),TaskState(state))
        changed=c.execute(sql.SQL("""UPDATE {} SET outcome=%s,error_code=%s,finished_at=clock_timestamp()
            WHERE attempt_id=%s AND task_id=%s AND lease_token=%s AND outcome='RUNNING'""").format(self.t('attempts')),
            (outcome,code,claim.attempt_id,claim.task_id,claim.token))
        if changed.rowcount!=1:
            raise RuntimeError('Current attempt missing')
        c.execute(sql.SQL('''UPDATE {} SET state=%s,result=%s,active_attempt_id=NULL,lease_until=NULL,
            updated_at=clock_timestamp(),
            retry_due_at=CASE WHEN %s THEN clock_timestamp()+(%s * interval '1 second') ELSE NULL END,
            recovery_due_at=CASE WHEN %s THEN clock_timestamp()+interval '1 second' ELSE NULL END
            WHERE task_id=%s''').format(self.t('tasks')),
            (state,None if result is None else Jsonb(result),state=='RETRY_WAIT',delay,state=='RECONCILING',claim.task_id))
        self._decision(c,claim.task_id,claim.attempt_id,reason,observation_id,
                       {'mutation_attempts':task['mutation_attempts'],'observation_count':task['observation_count'],
                        'retry_delay_seconds':delay if state=='RETRY_WAIT' else None})

    def _retry(self,c,task):
        unresolved=c.execute(sql.SQL('SELECT 1 FROM {} WHERE task_id=%s AND NOT settled_no_effect LIMIT 1')
                             .format(self.t('provider_dispatches')),(task['task_id'],)).fetchone()
        if unresolved:
            return 'NEEDS_REVIEW','PROVIDER_UNKNOWN',0
        if task['mutation_attempts']>=3:
            return 'FAILED','MUTATION_BUDGET_EXHAUSTED',0
        return 'RETRY_WAIT','SETTLED_NO_EFFECT',self.delay(task['mutation_attempts'])

    def complete_mutation(self,claim,dispatch,reply):
        reason=mutation_evidence(reply,dispatch.context,dispatch.body)
        with connect(self.conninfo) as c,c.transaction():
            if not self.ownership._current(c,claim):
                return 'stale_completion'
            task=c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(self.t('tasks')),(claim.task_id,)).fetchone()
            if task['state']!='RUNNING':
                return 'stale_completion'
            marker=c.execute(sql.SQL('SELECT * FROM {} WHERE dispatch_id=%s AND attempt_id=%s AND task_id=%s')
                             .format(self.t('provider_dispatches')),
                             (dispatch.dispatch_id,claim.attempt_id,claim.task_id)).fetchone()
            if marker is None or marker['request_body']!=dispatch.body:
                raise ValueError('Dispatch does not match durable request')
            observation=self._observation(c,claim,'MUTATION',reply)
            state,outcome,code,result,delay='RECONCILING','UNKNOWN','PROVIDER_UNKNOWN',None,0
            if reason=='EFFECT_CONFIRMED':
                state,outcome,code='SUCCEEDED','SUCCEEDED',None
                result=self._result(reply.body,'MUTATION')
                c.execute(sql.SQL('UPDATE {} SET accepted_provider_at=%s,protection_expires_at=%s WHERE task_id=%s')
                          .format(self.t('provider_context')),
                          (reply.body['accepted_at'],reply.body['protection_expires_at'],claim.task_id))
            elif reason in ('PERMANENT_REJECTION','SETTLED_NO_EFFECT'):
                c.execute(sql.SQL('UPDATE {} SET settled_no_effect=TRUE WHERE dispatch_id=%s')
                          .format(self.t('provider_dispatches')),(dispatch.dispatch_id,))
                outcome,code='KNOWN_FAILURE',reason
                if reason=='PERMANENT_REJECTION':
                    state='FAILED'
                else:
                    state,reason,delay=self._retry(c,task)
            elif reason=='PROVIDER_CONFLICT':
                state,code='NEEDS_REVIEW','PROVIDER_CONFLICT'
            else:
                reason='PROVIDER_UNKNOWN'
            self._close(c,task,claim,state,outcome,code,reason,observation,result,delay)
            return state

    def _result(self,data,source):
        return {key:data[key] for key in ('reservation_id','operation_id','sku','quantity','fingerprint')} | {'evidence_source':source}

    def expire_locked(self,c,task):
        context=self._context(c,task['task_id'])
        if context is None:
            return False
        definition=TaskDefinition(task['kind'],task['submission_key'],task['payload'])
        claim=Claim(task['task_id'],task['active_attempt_id'],task['lease_token'],definition)
        state,reason,delay='RECONCILING','LEASE_EXPIRED',0
        outcome='UNKNOWN'
        if task['state']=='RUNNING':
            marker=c.execute(sql.SQL('SELECT 1 FROM {} WHERE attempt_id=%s')
                             .format(self.t('provider_dispatches')),(claim.attempt_id,)).fetchone()
            if marker is None:
                state,reason,delay=self._retry(c,task)
                if reason=='SETTLED_NO_EFFECT':
                    reason='DISPATCH_NOT_STARTED'
                outcome='KNOWN_FAILURE'
        elif task['observation_count']>=5:
            state,reason='NEEDS_REVIEW','EVIDENCE_EXHAUSTED'
        self._close(c,task,claim,state,outcome,'LEASE_EXPIRED',reason,delay=delay)
        return True

    def claim_reconciliation(self,worker_id='recovery-scanner',lease_seconds=30):
        worker_identity(worker_id)
        bounded_int(lease_seconds,3,3600,'lease_seconds')
        with connect(self.conninfo) as c,c.transaction():
            task=c.execute(sql.SQL("""SELECT * FROM {} WHERE state='RECONCILING'
                AND active_attempt_id IS NULL AND recovery_due_at<=clock_timestamp()
                ORDER BY recovery_due_at,task_id LIMIT 1 FOR UPDATE SKIP LOCKED""").format(self.t('tasks'))).fetchone()
            if task is None:
                return None
            if task['observation_count']>=5:
                c.execute(sql.SQL("UPDATE {} SET state='NEEDS_REVIEW',recovery_due_at=NULL,updated_at=clock_timestamp() WHERE task_id=%s")
                          .format(self.t('tasks')),(task['task_id'],))
                self._decision(c,task['task_id'],None,'EVIDENCE_EXHAUSTED')
                return None
            context=self._context(c,task['task_id'])
            if context is None:
                raise RuntimeError('Reconciliation context missing')
            claim=Claim(task['task_id'],uuid4(),task['lease_token']+1,
                        TaskDefinition(task['kind'],task['submission_key'],task['payload']))
            c.execute(sql.SQL('''UPDATE {} SET lease_token=%s,active_attempt_id=%s,attempt_count=attempt_count+1,
                observation_count=observation_count+1,lease_until=clock_timestamp()+(%s * interval '1 second'),
                updated_at=clock_timestamp() WHERE task_id=%s''').format(self.t('tasks')),
                (claim.token,claim.attempt_id,lease_seconds,claim.task_id))
            c.execute(sql.SQL("""INSERT INTO {}(attempt_id,task_id,number,lease_token,worker_id,purpose)
                VALUES(%s,%s,%s,%s,%s,'RECONCILIATION')""").format(self.t('attempts')),
                (claim.attempt_id,claim.task_id,task['attempt_count']+1,claim.token,worker_id))
            return claim,context,self._body(task,context)

    def complete_lookup(self,claim,context,body,reply):
        reason=lookup_evidence(reply,context,body)
        with connect(self.conninfo) as c,c.transaction():
            if not self.ownership._current(c,claim):
                return 'stale_completion'
            task=c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(self.t('tasks')),(claim.task_id,)).fetchone()
            if task['state']!='RECONCILING':
                return 'stale_completion'
            observation=self._observation(c,claim,'LOOKUP',reply)
            state,outcome,code,result='RECONCILING','UNKNOWN','LOOKUP_INCONCLUSIVE',None
            if reason=='EFFECT_CONFIRMED':
                state,outcome,code='SUCCEEDED','SUCCEEDED',None
                result=self._result(reply.body['reservations'][0],'LOOKUP')
            elif reason in ('DUPLICATE_EFFECT','INVALID_EVIDENCE'):
                state,code='NEEDS_REVIEW',reason
            elif task['observation_count']>=5:
                state,reason,code='NEEDS_REVIEW','EVIDENCE_EXHAUSTED','EVIDENCE_EXHAUSTED'
            self._close(c,task,claim,state,outcome,code,reason,observation,result)
            return state

    def schedule_due(self,limit=100,*,due_before=None):
        bounded_int(limit,1,1000,'limit')
        with connect(self.conninfo) as c,c.transaction():
            tasks=c.execute(sql.SQL("""SELECT * FROM {} WHERE state='RETRY_WAIT'
                AND retry_due_at<=LEAST(clock_timestamp(), %s::timestamptz)
                ORDER BY retry_due_at,task_id
                LIMIT %s FOR UPDATE SKIP LOCKED""").format(self.t('tasks')),(due_before,limit)).fetchall()
            for task in tasks:
                if task['mutation_attempts']>=3:
                    raise RuntimeError('Exhausted mutation entered retry wait')
                generation=task['delivery_generation']+1
                c.execute(sql.SQL("""UPDATE {} SET state='QUEUED',delivery_generation=%s,
                    retry_due_at=NULL,updated_at=clock_timestamp() WHERE task_id=%s""").format(self.t('tasks')),
                    (generation,task['task_id']))
                c.execute(sql.SQL("""INSERT INTO {}(event_id,task_id,delivery_generation,event_type)
                    VALUES(%s,%s,%s,'TASK_READY')""").format(self.t('outbox')),
                    (uuid4(),task['task_id'],generation))
            return len(tasks)

    def audit(self,task_id):
        with connect(self.conninfo) as c:
            task=c.execute(sql.SQL('SELECT * FROM {} WHERE task_id=%s').format(self.t('tasks')),(task_id,)).fetchone()
            if task is None:
                raise LookupError('Unknown task')
            context=self._context(c,task_id)
            decisions=c.execute(sql.SQL('SELECT reason,policy_version,details,created_at FROM {} WHERE task_id=%s ORDER BY created_at,decision_id')
                                .format(self.t('recovery_decisions')),(task_id,)).fetchall()
            return {'task_id':str(task_id),'state':task['state'],'mutation_attempts':task['mutation_attempts'],
                    'observation_count':task['observation_count'],'retry_due_at':task['retry_due_at'],
                    'recovery_due_at':task['recovery_due_at'],'context':context,'decisions':decisions}


class RecoveryService:
    def __init__(self,repository,client_factory=ProviderClient,worker_id='recovery-scanner'):
        self.repository,self.client_factory,self.worker_id=repository,client_factory,worker_id

    def run_once(self):
        job=self.repository.claim_reconciliation(self.worker_id)
        if job is None:
            return 'idle'
        claim,context,body=job
        reply=self.client_factory(context['provider_url']).lookup(context['experiment_id'],body['operation_id'])
        return self.repository.complete_lookup(claim,context,body,reply)
