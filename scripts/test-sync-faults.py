"""Additional acceptance against fixtures from test-sync.py. Restarts only the managed backend PID."""
import importlib.util
import json
from pathlib import Path
import subprocess
import time
import uuid
import psutil

spec=importlib.util.spec_from_file_location('sync_test',Path(__file__).with_name('test-sync.py'))
t=importlib.util.module_from_spec(spec);spec.loader.exec_module(t)
f=json.loads(t.STATE.read_text(encoding='utf-8'));checks=[];schedule_evidence=[]
def passed(name):
    checks.append(name);print('PASS '+name,flush=True)
    (t.ROOT/'.runtime/sync-fault-acceptance.json').write_text(json.dumps(dict(checks=checks,workspaceId=f['workspaceId'],scheduleEvidence=schedule_evidence),ensure_ascii=False,indent=2),encoding='utf-8')
def until(run,predicate,seconds=60):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        r=t.call('GET','/runs/'+run['id'])
        if predicate(r):return r
        if r['status'] in ('FAILED','SUCCESS','CANCELLED','SKIPPED'):raise AssertionError(r)
        time.sleep(.2)
    raise AssertionError(r)
def start(node):return t.call('POST','/runs',dict(objectId=node['id'],expectedVersion=node['version'],businessDate='2026-09-29'),expected=(200,201,202))
def restart():
    pidfile=t.ROOT/'.runtime/sync-backend.pid';pid=int(pidfile.read_text(encoding='utf-8-sig').strip());p=psutil.Process(pid)
    assert any('data-studio-sync' in a and a.endswith('.jar') for a in p.cmdline()),'Not the backend created for this task'
    # Refuse to interrupt any user task outside the isolated acceptance workspace.
    metadata,mysql,doris=t.admins()
    active=t.execute(metadata,"SELECT id FROM dw_run WHERE status IN ('RUNNING','QUEUED','RECOVERING','WAITING') AND workspace_id<>%s",(f['workspaceId'],))
    for db in (metadata,mysql,doris):db.close()
    assert not active,'Unrelated runs are active'
    command=p.cmdline();p.kill();p.wait(timeout=15)
    with (t.ROOT/'.runtime/sync-backend.out.log').open('ab') as out,(t.ROOT/'.runtime/sync-backend.err.log').open('ab') as err:
        p=subprocess.Popen(command,cwd=t.ROOT/'backend',stdout=out,stderr=err,creationflags=subprocess.CREATE_NO_WINDOW)
    pidfile.write_text(str(p.pid),encoding='utf-8')
    for _ in range(80):
        try:
            if t.SESSION.get('http://127.0.0.1:8080/actuator/health',timeout=2).status_code==200:return
        except Exception:pass
        time.sleep(.3)
    raise AssertionError('Backend did not restart')

forward=t.call('GET','/objects/'+f['objects']['forward']);reverse=t.call('GET','/objects/'+f['objects']['reverse'])
releases=t.call('GET',f"/tasks/{forward['id']}/releases");release=releases[0]
schedule=dict(taskId=forward['id'],releaseId=release['id'],cron='0 * * * * *',cycle='minute',timezone='Asia/Shanghai',startDate='2026-01-01',endDate='2027-12-31',businessDateOffset=0,retries=2,retryIntervalSeconds=60,enabled=False,dependencies=[])
plans=t.call('GET',f"/task-schedules?workspaceId={f['workspaceId']}&taskId={forward['id']}")
plan=plans[0] if plans else t.call('POST',f"/tasks/{forward['id']}/schedule",schedule,expected=(200,201))
assert len(t.call('POST','/task-schedules/preview',schedule))==5
schedule_evidence.append(t.scheduled_release(plan,schedule,f['workspaceId']))
passed('real scheduled release execution and parameter preview')

slow=t.call('POST','/objects',dict(workspaceId=f['workspaceId'],parentId=None,kind='NODE',nodeType='离线同步',name='取消与恢复验收 '+uuid.uuid4().hex[:6],content='',config=json.loads(json.dumps(forward['config']))),201)
slow=t.save(slow,where='id <= 10000 AND SLEEP(0.003)=0',batchRows=100,writeMode='overwrite')
first=start(slow);until(first,lambda r:r.get('writtenRows',0)>0)
second=start(forward);time.sleep(1);pending=t.call('GET','/runs/'+second['id']);assert pending['status']=='QUEUED' and not pending.get('remoteRunId'),pending
t.call('POST','/runs/'+first['id']+'/stop',{});cancelled=t.wait(first);assert cancelled['status']=='CANCELLED',cancelled
done=t.wait(second);assert done['status']=='SUCCESS',done
passed('same-target serialization, cancellation after committed batches, queued successor runs once')

metadata,mysql,doris=t.admins();name=f['database']
if t.execute(mysql,'SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema=%s AND table_name=%s AND constraint_name=%s',(name,'returned_rows','acceptance_range')):
    t.execute(mysql,f'ALTER TABLE `{name}`.returned_rows DROP CHECK acceptance_range')
t.execute(mysql,f'TRUNCATE TABLE `{name}`.returned_rows')
constraint_failure=None
try:
    t.execute(mysql,f'ALTER TABLE `{name}`.returned_rows ADD CONSTRAINT acceptance_range CHECK (id < 20000)')
    reverse=t.save(reverse,writeMode='overwrite',where="business_date = '${day}'",batchRows=10000)
    failed=t.run_node(reverse,False);assert 0<failed['writtenRows']<31007 and failed['clearStatus']=='CLEARED' and failed['partialWrite'],failed
    assert t.execute(mysql,f'SELECT COUNT(*) FROM `{name}`.returned_rows')[0][0]==failed['writtenRows']
except BaseException as exc:
    constraint_failure=exc
    raise
finally:
    try:
        if t.execute(mysql,'SELECT 1 FROM information_schema.table_constraints WHERE constraint_schema=%s AND table_name=%s AND constraint_name=%s',(name,'returned_rows','acceptance_range')):
            t.execute(mysql,f'ALTER TABLE `{name}`.returned_rows DROP CHECK acceptance_range')
    except Exception as exc:
        print('CONSTRAINT CLEANUP FAILED '+json.dumps(dict(workspaceId=f['workspaceId'],table='returned_rows',constraint='acceptance_range',error=str(exc)),ensure_ascii=False),file=t.sys.stderr,flush=True)
        if constraint_failure is None:raise
t.run_node(reverse)
passed('overwrite failure after committed batches exposes exact partial target row count')
for db in (metadata,mysql,doris):db.close()

running=start(slow);until(running,lambda r:r.get('writtenRows',0)>0);restart();recovered=t.wait(running)
assert recovered['status'] in ('CANCELLED','FAILED') and recovered.get('recoveryReason')=='SERVICE_RESTARTED',recovered
assert t.run_node(forward)['status']=='SUCCESS'
passed('backend process loss: cancel remote by durable request ID; release target only after terminal result')

graph=dict(nodes=[dict(id='slow',objectId=slow['id'],label=slow['name'],nodeType='离线同步',x=0,y=0),dict(id='reverse',objectId=reverse['id'],label=reverse['name'],nodeType='离线同步',x=300,y=0)],edges=[dict(id='edge',source='slow',target='reverse')])
workflow=t.call('POST','/objects',dict(workspaceId=f['workspaceId'],kind='WORKFLOW',nodeType='工作流',name='工作流恢复验收 '+uuid.uuid4().hex[:6],content='',config=dict(run=dict(provider='WORKFLOW'),graph=graph)),201)
run=t.call('POST','/runs',dict(objectId=workflow['id'],expectedVersion=workflow['version'],expectedNodeVersions={slow['id']:slow['version'],reverse['id']:reverse['version']},businessDate='2026-09-29'),expected=(200,201,202))
deadline=time.monotonic()+60
while time.monotonic()<deadline:
    nodes=t.call('GET','/runs/'+run['id']+'/nodes');first=next(r for r in nodes if r['graphNodeId']=='slow')
    if first.get('writtenRows',0)>0:break
    time.sleep(.3)
else:raise AssertionError(nodes)
restart();result=t.wait(run);assert result['status']=='FAILED' and result['errorCode']=='SERVICE_RESTARTED',result
nodes=t.call('GET','/runs/'+run['id']+'/nodes');assert next(r for r in nodes if r['graphNodeId']=='reverse')['status']=='SKIPPED',nodes
passed('workflow restart stops remote writer and never dispatches downstream')

print(json.dumps(dict(checks=len(checks)),ensure_ascii=False),flush=True)
