"""Restore the isolated SDK acceptance fixture and record final proof.

Run only after both acceptance scripts have passed, using
Run scripts/test-sync-finalize.py with the project's dedicated Python environment.
Only the fixture's two draft sync nodes and their business tables are changed.
"""
import base64
import datetime as dt
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / '.runtime/sync-sdk-final-check.json'
spec = importlib.util.spec_from_file_location('sync_acceptance', ROOT / 'scripts/test-sync.py')
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)

TERMINAL_REMOTE = {'SUCCEEDED', 'FAILED', 'CANCELLED', 'INTERRUPTED'}
FORBIDDEN_SECRET_KEYS = {'password', 'passwordcipher', 'encryptedpassword', 'credentials'}


def require(condition, label):
    if not condition:
        raise AssertionError(label)


def canonical(value):
    if isinstance(value, Decimal):
        return {'decimal': str(value)}
    if isinstance(value, (dt.date, dt.datetime)):
        return {type(value).__name__: value.isoformat()}
    raise TypeError(type(value).__name__)


def rows_digest(rows):
    payload = json.dumps(rows, default=canonical, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def secret_fields_absent(value):
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = re.sub(r'[^a-z]', '', str(key).lower())
            require(normalized not in FORBIDDEN_SECRET_KEYS, 'credential field in public snapshot')
            secret_fields_absent(item)
    elif isinstance(value, list):
        for item in value:
            secret_fields_absent(item)


def scalar(connection, sql, args=None):
    return t.execute(connection, sql, args)[0][0]


def snapshot_proof(metadata, fixture, extra_public_payloads):
    wid = fixture['workspaceId']
    cipher_rows = t.execute(metadata, 'SELECT id,password_cipher FROM dw_datasource WHERE workspace_id=%s', (wid,))
    require(len(cipher_rows) == 2, 'exactly two fixture datasources')
    ciphers = []
    for _, cipher in cipher_rows:
        require(isinstance(cipher, str) and ':' in cipher, 'datasource credential encrypted format')
        nonce, encrypted = cipher.split(':', 1)
        require(len(base64.b64decode(nonce, validate=True)) == 12, 'AES-GCM nonce length')
        require(len(base64.b64decode(encrypted, validate=True)) > 16, 'AES-GCM ciphertext and tag')
        ciphers.append(cipher)
    snapshots = []
    groups = {
        'runRecords': t.execute(metadata, 'SELECT id,data_json,snapshot_json FROM dw_run WHERE workspace_id=%s', (wid,)),
        'taskReleases': t.execute(metadata, 'SELECT id,data_json FROM dw_task_release WHERE workspace_id=%s', (wid,)),
        'workflowReleases': t.execute(metadata, 'SELECT id,bundle_json FROM dw_workflow_release WHERE workspace_id=%s', (wid,)),
    }
    for records in groups.values():
        for row in records:
            for blob in row[1:]:
                require(isinstance(blob, str), 'snapshot JSON text')
                snapshots.append(blob)
    for value in extra_public_payloads:
        snapshots.append(json.dumps(value, ensure_ascii=False))
    for blob in snapshots:
        secret_fields_absent(json.loads(blob))
        require(all(cipher not in blob for cipher in ciphers), 'credential ciphertext absent from snapshots')
    return {
        'encryptedDatasourceCount': len(cipher_rows),
        'credentialFieldsAbsent': True,
        'credentialCiphertextsAbsent': True,
        'snapshotPayloadsChecked': len(snapshots),
        'recordsChecked': {label: len(records) for label, records in groups.items()},
        'method': 'AES-GCM format plus recursive credential-field and exact-ciphertext scan; no decryption',
    }


def receipt_proof(run):
    require(run['status'] == 'SUCCESS', 'final sync successful')
    require(run.get('readRows') == run.get('writtenRows') == 31007, 'final exact read/write counters')
    require(run.get('commitUnknown') is False, 'final commit known')
    require(run.get('partialWrite') is False, 'final successful write complete')
    public = t.call('GET', f"/runs/{run['id']}/sync/batches")
    batches = public['batches']
    require(len(batches) == run.get('committedBatches') and len(batches) >= 4, 'final receipt batch count')
    require(all(batch['state'] == 'CONFIRMED' for batch in batches), 'all final receipts confirmed')
    require(sum(batch['rows'] for batch in batches) == 31007, 'final receipt row total')
    return {
        'runId': run['id'],
        'remoteRunId': run['remoteRunId'],
        'status': run['status'],
        'readRows': run['readRows'],
        'writtenRows': run['writtenRows'],
        'committedBatches': run['committedBatches'],
        'confirmedReceiptRows': sum(batch['rows'] for batch in batches),
        'commitUnknown': False,
        'partialWrite': False,
    }, public


def remote_proof(metadata, fixture):
    records = t.execute(metadata,
        'SELECT e.run_id,e.service_url,e.state_store_id FROM dw_sync_execution e '
        'JOIN dw_run r ON r.id=e.run_id WHERE r.workspace_id=%s AND e.submitted=TRUE',
        (fixture['workspaceId'],))
    checked = []
    health_ids = {}
    for run_id, service_url, store_id in records:
        require(service_url in {'http://127.0.0.1:9876', 'http://localhost:9876'}, 'loopback acceptance engine')
        require(store_id, 'durable state-store identity')
        if service_url not in health_ids:
            health = t.SESSION.get(service_url + '/healthz', timeout=15)
            require(health.status_code == 200, 'engine health status')
            health_ids[service_url] = health.json()['state_store_id']
        require(health_ids[service_url] == store_id, 'unchanged durable state-store identity')
        response = t.SESSION.get(service_url + '/v1/requests/' + run_id,
            headers={'x-dunnelean-state-store-id': store_id}, timeout=15)
        require(response.status_code == 200, 'durable request still queryable')
        status = response.json()
        remote = status.get('run')
        if remote is None:
            require(status.get('cancel_requested') is True, 'run-less request durably cancelled')
            checked.append({'requestId': run_id, 'state': 'CANCELLED_BEFORE_START', 'commitUnknown': False})
        else:
            require(remote.get('state') in TERMINAL_REMOTE, 'fixture remote request terminal')
            require(remote.get('commit_unknown') is False, 'fixture remote commit known')
            checked.append({'requestId': run_id, 'state': remote['state'], 'commitUnknown': False})
    require(records, 'fixture remote requests were checked')
    return {'checkedRequests': len(checked), 'stateStoreIds': sorted(set(health_ids.values())), 'requests': checked}


def idle_proof(metadata, fixture):
    wid = fixture['workspaceId']
    counts = {
        'globalPendingRuns': scalar(metadata, "SELECT COUNT(*) FROM dw_run WHERE status IN ('WAITING','QUEUED','RUNNING','RECOVERING')"),
        'globalTargetLocks': scalar(metadata, 'SELECT COUNT(*) FROM dw_sync_target_lock'),
        'fixtureEnabledTaskSchedules': scalar(metadata, 'SELECT COUNT(*) FROM dw_task_schedule WHERE workspace_id=%s AND enabled=TRUE', (wid,)),
        'fixtureEnabledWorkflowSchedules': scalar(metadata, 'SELECT COUNT(*) FROM dw_workflow_schedule WHERE workspace_id=%s AND enabled=TRUE', (wid,)),
        'fixturePendingTaskTriggers': scalar(metadata, 'SELECT COUNT(*) FROM dw_task_trigger t JOIN dw_task_schedule s ON s.id=t.schedule_id '
            "WHERE s.workspace_id=%s AND t.status IN ('PENDING','WAITING_DEPENDENCY','WAITING_RESOURCE','RUNNING','RETRY_WAIT')", (wid,)),
        'fixturePendingWorkflowTriggers': scalar(metadata, 'SELECT COUNT(*) FROM dw_schedule_trigger t JOIN dw_workflow_schedule s ON s.id=t.schedule_id '
            "WHERE s.workspace_id=%s AND t.status IN ('PENDING','RUNNING','RETRY_WAIT')", (wid,)),
    }
    require(all(count == 0 for count in counts.values()), 'no pending runs, target locks or enabled fixture schedules/triggers')
    return counts


def main():
    stage = 'fixture_validation'
    connections = []
    proof = {'success': False, 'createdAt': dt.datetime.now(dt.timezone.utc).isoformat()}
    try:
        fixture = json.loads(t.STATE.read_text(encoding='utf-8'))
        name = fixture['database']
        require(re.fullmatch(r'sync_accept_[a-f0-9]{8}', name), 'isolated database name')
        require(fixture['workspaceId'] == 'sync-accept-' + name[-8:], 'isolated workspace name')
        require(fixture['user'] == 'sync_' + name[-8:], 'isolated business account')
        proof.update(database=name, workspaceId=fixture['workspaceId'])
        stage = 'restore_roundtrip'
        forward = t.call('GET', '/objects/' + fixture['objects']['forward'])
        reverse = t.call('GET', '/objects/' + fixture['objects']['reverse'])
        for node in (forward, reverse):
            require(node['workspaceId'] == fixture['workspaceId'], 'fixture object workspace')
        forward = t.save(forward, writeMode='overwrite', where="business_date = '${day}'", batchRows=10000, mapping=[])
        reverse = t.save(reverse, writeMode='overwrite', where="business_date = '${day}'", batchRows=10000, mapping=[])
        final_forward = t.run_node(forward)
        final_reverse = t.run_node(reverse)
        fixture.setdefault('runs', {}).update(finalForward=final_forward['id'], finalReverse=final_reverse['id'])
        t.STATE.write_text(json.dumps(fixture, ensure_ascii=False, indent=2), encoding='utf-8')
        stage = 'row_values'
        metadata, mysql, doris = t.admins()
        connections = [metadata, mysql, doris]
        expected = [
            (i, Decimal('12345678901234.123456') if i % 3 else None, Decimal('18446744073709551615'),
             None if i % 7 == 0 else f'中文😀-{i}', dt.date(2026, 9, 29), dt.datetime(2026, 9, 29, 12, 34, 56, 123456))
            for i in range(1, 31008)
        ]
        source = list(t.execute(mysql, f'SELECT * FROM `{name}`.source_rows ORDER BY id'))
        target = list(t.execute(doris, f'SELECT * FROM `{name}`.target_rows ORDER BY id'))
        returned = list(t.execute(mysql, f'SELECT * FROM `{name}`.returned_rows ORDER BY id'))
        require(source == target == returned == expected, 'source, target, returned and all 31007 generated rows exactly match')
        digests = {label: rows_digest(values) for label, values in [('source', source), ('target', target), ('returned', returned), ('expected', expected)]}
        require(len(set(digests.values())) == 1, 'canonical row digests match')
        proof['roundtrip'] = {
            'rows': len(source), 'exactMatch': True, 'sha256': digests,
            'fieldTypes': dict(zip(['id', 'amount', 'big_value', 'note', 'business_date', 'event_at'],
                                  [type(item).__name__ for item in source[0]])),
            'microsecondsPreserved': True, 'decimalPrecisionPreserved': True,
            'unsignedRangePreserved': True, 'unicodeAndNullPreserved': True,
        }
        stage = 'batch_receipts'
        forward_receipts, forward_public = receipt_proof(final_forward)
        reverse_receipts, reverse_public = receipt_proof(final_reverse)
        proof['finalRuns'] = {'forward': forward_receipts, 'reverse': reverse_receipts}
        stage = 'durable_remote_requests'
        proof['remoteRequests'] = remote_proof(metadata, fixture)
        stage = 'idle_resources'
        proof['idle'] = idle_proof(metadata, fixture)
        stage = 'credential_snapshots'
        proof['snapshots'] = snapshot_proof(metadata, fixture, [final_forward, final_reverse, forward_public, reverse_public])
        proof['success'] = True
        proof['finishedAt'] = dt.datetime.now(dt.timezone.utc).isoformat()
        REPORT.write_text(json.dumps(proof, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps({'success': True, 'workspaceId': fixture['workspaceId'], 'rows': len(source),
                          'forwardBatches': forward_receipts['committedBatches'],
                          'reverseBatches': reverse_receipts['committedBatches'],
                          'terminalRemoteRequests': proof['remoteRequests']['checkedRequests'],
                          'pendingRuns': proof['idle']['globalPendingRuns'],
                          'targetLocks': proof['idle']['globalTargetLocks']}, ensure_ascii=False), flush=True)
        return 0
    except Exception as error:
        # Avoid raw assertions/HTTP responses or connection errors in terminal/logs.
        proof.update(failureStage=stage, errorType=type(error).__name__)
        REPORT.write_text(json.dumps(proof, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps({'success': False, 'failureStage': stage, 'errorType': type(error).__name__}), flush=True)
        return 1
    finally:
        for connection in connections:
            connection.close()


if __name__ == '__main__':
    sys.exit(main())
