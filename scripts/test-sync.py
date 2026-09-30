"""Real MySQL/Doris/API acceptance. Only creates isolated sync_accept_* databases.
Run scripts/test-sync.py with the project's dedicated Python environment.
Fixtures remain for browser acceptance; use --cleanup to remove this script's fixtures.
"""
import argparse
import datetime as dt
from decimal import Decimal
import json
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
import uuid
import pymysql
import requests

ROOT=Path(__file__).resolve().parents[1]
STATE=ROOT/'.runtime/sync-live-fixture.json'
REPORT=ROOT/'.runtime/sync-acceptance.json'
API='http://127.0.0.1:8080/api/v1'
SESSION=requests.Session()

def call(method,path,body=None,expected=200):
    response=SESSION.request(method,API+path,json=body,timeout=90)
    assert response.status_code in ([expected] if isinstance(expected,int) else expected), f'{method} {path}: {response.status_code} {response.text[:1500]}'
    return response.json() if response.content else None

def admins():
    cfg=dict(line.split('=',1) for line in (ROOT/'backend/application-local.properties').read_text(encoding='utf-8-sig').splitlines() if '=' in line and not line.startswith('#'))
    container=json.loads(subprocess.check_output(['docker','inspect','dataworks-demo-mysql'],encoding='utf-8'))[0]
    env=dict(v.split('=',1) for v in container['Config']['Env'] if '=' in v)
    options=dict(host='127.0.0.1',user='root',charset='utf8mb4',autocommit=True)
    return (pymysql.connect(**options,port=3306,password=cfg['spring.datasource.password'],database='fake_dataworks_260927'),pymysql.connect(**options,port=3307,password=env['MYSQL_ROOT_PASSWORD']),pymysql.connect(**options,port=9030,password=''))

def execute(connection,sql,args=None):
    with connection.cursor() as cur:cur.execute(sql,args);return cur.fetchall()

def cleanup():
    if not STATE.exists():return
    f=json.loads(STATE.read_text(encoding='utf-8'));name=f['database'];user=f['user'];wid=f['workspaceId']
    assert re.fullmatch(r'sync_accept_[a-f0-9]{8}',name) and user=='sync_'+name[-8:] and wid=='sync-accept-'+name[-8:]
    metadata,mysql,doris=admins()
    if execute(metadata,"SELECT id FROM dw_run WHERE workspace_id=%s AND status IN ('RUNNING','QUEUED','RECOVERING','WAITING')",(wid,)):
        raise RuntimeError('Fixture still has active runs; resolve or stop them first')
    for db in (mysql,doris):execute(db,f'DROP DATABASE IF EXISTS `{name}`')
    execute(mysql,f"DROP USER IF EXISTS '{user}'@'%'");execute(doris,f"DROP USER IF EXISTS '{user}'")
    ids=[row[0] for row in execute(metadata,'SELECT id FROM dw_run WHERE workspace_id=%s',(wid,))]
    for runid in ids:
        execute(metadata,'DELETE FROM dw_sync_target_lock WHERE run_id=%s',(runid,));execute(metadata,'DELETE FROM dw_sync_execution WHERE run_id=%s',(runid,))
    for trigger,schedule in [('dw_task_trigger','dw_task_schedule'),('dw_schedule_trigger','dw_workflow_schedule')]:
        execute(metadata,f'DELETE FROM {trigger} WHERE schedule_id IN (SELECT id FROM {schedule} WHERE workspace_id=%s)',(wid,))
    for table in ('dw_task_schedule','dw_workflow_schedule','dw_task_release','dw_workflow_release','dw_run','dw_record'):
        execute(metadata,f'DELETE FROM {table} WHERE workspace_id=%s',(wid,))
    for table in ('dw_file','dw_version'):
        execute(metadata,f'DELETE FROM {table} WHERE object_id IN (SELECT id FROM dw_object WHERE workspace_id=%s)',(wid,))
    for table in ('dw_object','dw_datasource'):
        execute(metadata,f'DELETE FROM {table} WHERE workspace_id=%s',(wid,))
    execute(metadata,'DELETE FROM dw_workspace WHERE id=%s',(wid,))
    for db in (metadata,mysql,doris):db.close()
    STATE.unlink();print('Isolated sync fixtures removed',flush=True)

def wait(run,timeout=180):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        value=call('GET','/runs/'+run['id'])
        if value['status'] not in ('WAITING','QUEUED','RUNNING','RECOVERING'):return value
        if value['status']=='RECOVERING' and value.get('commitUnknown'):raise AssertionError(json.dumps(value,ensure_ascii=False))
        time.sleep(.4)
    raise AssertionError('Timed out: '+json.dumps(value,ensure_ascii=False))

def run_node(node,success=True):
    result=wait(call('POST','/runs',dict(objectId=node['id'],expectedVersion=node['version'],businessDate='2026-09-29'),expected=(200,201,202)))
    assert result['status']==('SUCCESS' if success else 'FAILED'), json.dumps(result,ensure_ascii=False)
    return result

def save(node,**sync):
    node['config']['sync'].update(sync)
    return call('PUT','/objects/'+node['id'],node)

def scheduled_release(plan,schedule,workspace_id,timeout=90):
    """Require a new real trigger and always pause this fixture's exact schedule."""
    schedule_id=plan['id'];task_id=schedule['taskId']
    def current_plan():
        plans=call('GET',f'/task-schedules?workspaceId={workspace_id}&taskId={task_id}')
        matches=[value for value in plans if value['id']==schedule_id]
        assert len(matches)==1,'Acceptance schedule missing or ambiguous'
        value=matches[0]
        assert value['workspaceId']==workspace_id and value['taskId']==task_id,'Acceptance schedule ownership changed'
        return value
    def triggers():
        result=[];page=1
        while True:
            response=call('GET',f'/task-schedules/{schedule_id}/triggers?page={page}&pageSize=100')
            result.extend(response['items'])
            if len(result)>=response['total']:return result
            assert response['items'],'Trigger pagination did not advance'
            page+=1
    current=current_plan();assert not current['enabled'],'Acceptance schedule must begin paused'
    existing_ids={item['id'] for item in triggers()}
    evidence=dict(scheduleId=schedule_id,workspaceId=workspace_id,existingTriggerIds=sorted(existing_ids),startedAt=dt.datetime.now(dt.timezone.utc).isoformat())
    failure=None
    try:
        current=call('PUT',f'/task-schedules/{schedule_id}',dict(schedule,enabled=True,expectedVersion=current['version']))
        evidence.update(enabledAt=current['updatedAt'],nextFireAt=current['nextFireAt'])
        deadline=time.monotonic()+timeout;trigger=None;new=[]
        while time.monotonic()<deadline:
            new=sorted((item for item in triggers() if item['id'] not in existing_ids),key=lambda item:(item['scheduledAt'],item['id']))
            if new and new[0]['status'] in ('SUCCESS','FAILED','SKIPPED','BLOCKED'):
                trigger=new[0];break
            time.sleep(1)
        assert trigger is not None,dict(reason='No new terminal schedule trigger',scheduleId=schedule_id,newTriggers=new)
        assert trigger['status']=='SUCCESS',trigger
        assert trigger['scheduleId']==schedule_id and trigger['workspaceId']==workspace_id and trigger['taskId']==task_id,trigger
        run=call('GET','/runs/'+trigger['runId'])
        assert run['status']=='SUCCESS' and run['triggerId']==trigger['id'] and run['scheduleId']==schedule_id and run['triggerType']=='SCHEDULED',run
        assert run['releaseId']==schedule['releaseId'],run
        evidence.update(triggerId=trigger['id'],runId=run['id'],scheduledAt=trigger['scheduledAt'],triggerCreatedAt=trigger['createdAt'],triggerFinishedAt=trigger['finishedAt'],runStatus=run['status'],observedAt=dt.datetime.now(dt.timezone.utc).isoformat())
    except BaseException as exc:
        failure=exc
        raise
    finally:
        try:
            # Reload the version; scheduler scans can update this record while polling.
            current=current_plan()
            if current['enabled']:
                current=call('PUT',f'/task-schedules/{schedule_id}',dict(current,enabled=False,expectedVersion=current['version']))
            assert not current['enabled'],'Acceptance schedule was not paused'
            evidence.update(disabledAt=current['updatedAt'],disabledVersion=current['version'])
        except Exception as exc:
            print('SCHEDULE CLEANUP FAILED '+json.dumps(dict(scheduleId=schedule_id,workspaceId=workspace_id,error=str(exc)),ensure_ascii=False),file=sys.stderr,flush=True)
            if failure is None:raise
    print('SCHEDULE EVIDENCE '+json.dumps(evidence,ensure_ascii=False),flush=True)
    return evidence

def main():
    if STATE.exists():raise RuntimeError('Previous fixture exists; run --cleanup before a new acceptance run')
    key=uuid.uuid4().hex[:8];name='sync_accept_'+key;user='sync_'+key;password=secrets.token_urlsafe(24);wid='sync-accept-'+key
    f=dict(database=name,user=user,workspaceId=wid,objects={},runs={});STATE.write_text(json.dumps(f),encoding='utf-8')
    metadata,mysql,doris=admins();checks=[];schedule_evidence=[]
    def passed(name):checks.append(name);print('PASS '+name,flush=True);REPORT.write_text(json.dumps(dict(checks=checks,fixture=f,scheduleEvidence=schedule_evidence),ensure_ascii=False,indent=2),encoding='utf-8')
    for db in (mysql,doris):execute(db,f'CREATE DATABASE `{name}`')
    execute(mysql,f"CREATE USER '{user}'@'%%' IDENTIFIED BY %s",(password,));execute(mysql,f"GRANT ALL ON `{name}`.* TO '{user}'@'%'")
    execute(doris,f"CREATE USER '{user}' IDENTIFIED BY %s",(password,));execute(doris,f"GRANT SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV ON {name}.* TO '{user}'")
    execute(metadata,"INSERT INTO dw_workspace(id,name,code,region,workspace_type) VALUES(%s,%s,%s,%s,'TEST')",(wid,'同步验收 '+key,key,'本地'))
    columns='id BIGINT NOT NULL, amount DECIMAL(20,6) NULL, big_value DECIMAL(20,0) NULL, note VARCHAR(128) NULL, business_date DATE NULL, event_at DATETIME(6) NULL'
    for table in ('source_rows','returned_rows'):execute(mysql,f'CREATE TABLE `{name}`.`{table}` ({columns}, PRIMARY KEY(id)) ENGINE=InnoDB')
    for table,model in [('target_rows','DUPLICATE'),('unique_rows','UNIQUE')]:
        props='"replication_num"="1"'+(',"enable_unique_key_merge_on_write"="true"' if model=='UNIQUE' else '')
        execute(doris,f'CREATE TABLE `{name}`.`{table}` ({columns}) {model} KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES({props})')
    count=31007
    rows=[(i,Decimal('12345678901234.123456') if i%3 else None,Decimal('18446744073709551615'),None if i%7==0 else f'中文😀-{i}',dt.date(2026,9,29),dt.datetime(2026,9,29,12,34,56,123456)) for i in range(1,count+1)]
    with mysql.cursor() as cur:cur.executemany(f'INSERT INTO `{name}`.source_rows VALUES(%s,%s,%s,%s,%s,%s)',rows)
    common=dict(workspaceId=wid,host='127.0.0.1',database=name,username=user,password=password)
    ms=call('POST','/datasources',dict(common,type='MYSQL',name='验收 MySQL',port=3307),201);ds=call('POST','/datasources',dict(common,type='DORIS',name='验收 Doris',port=9030),201)
    f['mysqlId']=ms['id'];f['dorisId']=ds['id'];STATE.write_text(json.dumps(f),encoding='utf-8')
    for source in (ms,ds):assert call('POST','/datasources/'+source['id']+'/test',{})['success']
    assert call('GET',f"/datasources/{ds['id']}/tables/unique_rows/sync-metadata")['model']=='UNIQUE_MOW'
    passed('typed datasource CRUD, SQL metadata and connection tests')
    def node(label,src,tgt,sourceTable,targetTable,mode='overwrite'):
        return call('POST','/objects',dict(workspaceId=wid,parentId=None,kind='NODE',nodeType='离线同步',name=label,content='',config=dict(run=dict(provider='SYNC'),sync=dict(sourceDataSourceId=src['id'],targetDataSourceId=tgt['id'],sourceTable=sourceTable,targetTable=targetTable,writeMode=mode,columns=[],mapping=[],batchRows=10000,timeoutSeconds=3600,parallelism=1,where="business_date = '${day}'"),schedule=dict(parameters=[dict(name='day',value='2026-09-29',source='MANUAL')]))),201)
    forward=node('MySQL 到 Doris',ms,ds,'source_rows','target_rows');reverse=node('Doris 到 MySQL',ds,ms,'target_rows','returned_rows')
    f['objects'].update(forward=forward['id'],reverse=reverse['id']);STATE.write_text(json.dumps(f),encoding='utf-8')
    assert call('POST','/sync/validate',forward)['valid'];first=run_node(forward);assert first['writtenRows']==count and first['committedBatches']>=4
    returned=run_node(reverse);assert returned['writtenRows']==count
    actual=execute(mysql,f'SELECT * FROM `{name}`.returned_rows ORDER BY id');assert list(actual)==rows
    passed('31,007-row exact roundtrip: decimal, unsigned range, NULL, Chinese, microseconds; 4+ batches')
    forward=save(forward,writeMode='append',where='id <= 3');run_node(forward);assert execute(doris,f'SELECT COUNT(*) FROM `{name}`.target_rows')[0][0]==count+3
    forward=save(forward,writeMode='overwrite',where="business_date = '${day}'");run_node(forward)
    reverse=save(reverse,writeMode='upsert',keyColumns=['id']);run_node(reverse);assert execute(mysql,f'SELECT COUNT(*) FROM `{name}`.returned_rows')[0][0]==count
    unique=node('Doris 主键更新',ms,ds,'source_rows','unique_rows','upsert');run_node(unique);run_node(unique);assert execute(doris,f'SELECT COUNT(*) FROM `{name}`.unique_rows')[0][0]==count
    passed('append, MySQL upsert, Doris Unique MOW upsert and overwrite')
    reverse=save(reverse,writeMode='overwrite',where='id < 0');run_node(reverse);assert execute(mysql,f'SELECT COUNT(*) FROM `{name}`.returned_rows')[0][0]==0
    reverse=save(reverse,where="business_date = '${day}'");run_node(reverse)
    broken=save(forward,mapping=[dict(source='not_a_column',target='id')]);call('POST','/sync/validate',broken,expected=400);assert execute(doris,f'SELECT COUNT(*) FROM `{name}`.target_rows')[0][0]==count
    forward=save(broken,mapping=[])
    # All six field names differ; prove mappings preserve values in both SDK directions.
    field_names=['id','amount','big_value','note','business_date','event_at']
    mapped_names=['mapped_'+field for field in field_names]
    mapped_columns='mapped_id BIGINT NOT NULL, mapped_amount DECIMAL(20,6) NULL, mapped_big_value DECIMAL(20,0) NULL, mapped_note VARCHAR(128) NULL, mapped_business_date DATE NULL, mapped_event_at DATETIME(6) NULL'
    execute(mysql,f'CREATE TABLE `{name}`.mapping_source_rows ({mapped_columns}, PRIMARY KEY(mapped_id)) ENGINE=InnoDB')
    for row in rows[:3]:execute(mysql,f'INSERT INTO `{name}`.mapping_source_rows VALUES(%s,%s,%s,%s,%s,%s)',row)
    forward=save(forward,sourceTable='mapping_source_rows',where="mapped_business_date = '${day}'",mapping=[dict(source=mapped,target=field) for field,mapped in zip(field_names,mapped_names)])
    assert call('POST','/sync/validate',forward)['valid'];mapped_forward=run_node(forward);assert mapped_forward['writtenRows']==3
    assert list(execute(doris,f'SELECT * FROM `{name}`.target_rows ORDER BY id'))==rows[:3]
    reverse=save(reverse,targetTable='mapping_source_rows',where="business_date = '${day}'",mapping=[dict(source=field,target=mapped) for field,mapped in zip(field_names,mapped_names)])
    assert call('POST','/sync/validate',reverse)['valid'];mapped_reverse=run_node(reverse);assert mapped_reverse['writtenRows']==3
    assert list(execute(mysql,f'SELECT * FROM `{name}`.mapping_source_rows ORDER BY mapped_id'))==rows[:3]
    forward=save(forward,sourceTable='source_rows',where="business_date = '${day}'",mapping=[])
    reverse=save(reverse,targetTable='returned_rows',where="business_date = '${day}'",mapping=[])
    run_node(forward);run_node(reverse)
    assert list(execute(mysql,f'SELECT * FROM `{name}`.returned_rows ORDER BY id'))==rows
    f['runs'].update(mappingForward=mapped_forward['id'],mappingReverse=mapped_reverse['id']);STATE.write_text(json.dumps(f),encoding='utf-8')
    passed('empty-source overwrite, preflight target preservation and exact six-field renamed mapping in both directions')
    release=call('POST',f"/tasks/{forward['id']}/releases",dict(expectedVersion=forward['version'],note='sync acceptance'),expected=(200,201));forward=save(forward,where='id<0')
    rr=wait(call('POST',f"/task-releases/{release['id']}/runs",dict(businessDate='2026-09-29'),expected=(200,201,202)));assert rr['status']=='SUCCESS' and rr['writtenRows']==count
    forward=save(forward,where="business_date = '${day}'")
    passed('immutable task release uses published filter after draft changes')
    sql=call('POST','/objects',dict(workspaceId=wid,parentId=None,kind='NODE',nodeType='MySQL',name='检查返回行数',content='SELECT COUNT(*) AS row_count FROM returned_rows',config=dict(run=dict(provider='MYSQL',dataSourceId=ms['id'],timeoutSeconds=30))),201)
    workflow=call('POST','/objects',dict(workspaceId=wid,parentId=None,kind='WORKFLOW',nodeType='工作流',name='双向同步验收流程',content='',config=dict(run=dict(provider='WORKFLOW'),graph=dict(nodes=[dict(id=str(i),objectId=o['id'],label=o['name'],nodeType=o['nodeType'],x=80+i*240,y=120) for i,o in enumerate((forward,reverse,sql))],edges=[dict(id='e1',source='0',target='1'),dict(id='e2',source='1',target='2')]))),201)
    versions={o['id']:o['version'] for o in (forward,reverse,sql)}
    wr=wait(call('POST','/runs',dict(objectId=workflow['id'],expectedVersion=workflow['version'],expectedNodeVersions=versions,businessDate='2026-09-29'),expected=(200,201,202)));assert wr['status']=='SUCCESS',wr
    assert all(n['status']=='SUCCESS' for n in call('GET',f"/runs/{wr['id']}/nodes"))
    published=call('POST',f"/workflows/{workflow['id']}/releases",dict(expectedVersion=workflow['version'],expectedNodeVersions=versions,note='sync workflow'),expected=(200,201))
    wr=wait(call('POST',f"/workflow-releases/{published['id']}/runs",dict(businessDate='2026-09-29'),expected=(200,201,202)));assert wr['status']=='SUCCESS',wr
    f['objects']['workflow']=workflow['id'];f['runs']['workflow']=wr['id'];STATE.write_text(json.dumps(f),encoding='utf-8')
    passed('mixed MySQL/SYNC DAG and workflow release execute dependencies')
    schedule=dict(taskId=forward['id'],releaseId=release['id'],cron='0 * * * * *',cycle='minute',timezone='Asia/Shanghai',startDate='2026-01-01',endDate='2027-12-31',businessDateOffset=0,retries=2,retryIntervalSeconds=60,enabled=False,dependencies=[])
    plan=call('POST',f"/tasks/{forward['id']}/schedule",schedule,expected=(200,201));assert len(call('POST','/task-schedules/preview',schedule))==5
    schedule_evidence.append(scheduled_release(plan,schedule,wid))
    passed('published task scheduled execution and parameter preview')
    call('POST',f"/runs/{rr['id']}/stop",{});assert call('GET',f"/runs/{rr['id']}")['status']=='SUCCESS'
    failed=call('POST','/runs',dict(objectId=forward['id'],expectedVersion=1),expected=409)
    assert password not in json.dumps(call('GET',f"/runs/{rr['id']}"))
    assert password not in execute(metadata,'SELECT data_json FROM dw_run WHERE id=%s',(rr['id'],))[0][0]
    passed('terminal stop idempotence, version guard and secret redaction')
    for db in (metadata,mysql,doris):db.close()
    print(json.dumps(dict(checks=len(checks),workspaceId=wid,workflowId=workflow['id']),ensure_ascii=False),flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--cleanup',action='store_true');args=parser.parse_args()
    cleanup() if args.cleanup else main()
