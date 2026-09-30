-- Keep one built-in workspace. Preserve legacy fixtures without exposing them in the user's list.
ALTER TABLE dw_workspace ADD COLUMN workspace_type VARCHAR(16) NOT NULL DEFAULT 'USER';
ALTER TABLE dw_workspace ADD UNIQUE KEY uq_workspace_code (code);
UPDATE dw_workspace SET workspace_type='DEFAULT',region='本地' WHERE id='local-workspace';
UPDATE dw_workspace SET workspace_type='ARCHIVED' WHERE id='sandbox' AND code='dataworks_sandbox';
UPDATE dw_workspace SET workspace_type='TEST' WHERE id REGEXP '^sync-accept-[a-f0-9]{8}$';

-- Retire old automatic plans; files, connections, releases and run history remain intact.
UPDATE dw_task_schedule SET enabled=FALSE,next_fire_at=NULL,version=version+1,
 data_json=JSON_SET(data_json,'$.enabled',CAST('false' AS JSON),'$.nextFireAt',NULL,'$.version',version,'$.pauseReason','WORKSPACE_RETIRED')
 WHERE workspace_id IN (SELECT id FROM dw_workspace WHERE workspace_type IN ('ARCHIVED','TEST'));
UPDATE dw_workflow_schedule SET enabled=FALSE,next_fire_at=NULL,version=version+1,
 data_json=JSON_SET(data_json,'$.enabled',CAST('false' AS JSON),'$.nextFireAt',NULL,'$.version',version,'$.pauseReason','WORKSPACE_RETIRED')
 WHERE workspace_id IN (SELECT id FROM dw_workspace WHERE workspace_type IN ('ARCHIVED','TEST'));
UPDATE dw_task_trigger SET status='CANCELLED',data_json=JSON_SET(data_json,'$.status','CANCELLED','$.reason','WORKSPACE_RETIRED')
 WHERE status IN ('PENDING','WAITING_DEPENDENCY','WAITING_RESOURCE','RETRY_WAIT')
 AND schedule_id IN (SELECT id FROM dw_task_schedule WHERE workspace_id IN (SELECT id FROM dw_workspace WHERE workspace_type IN ('ARCHIVED','TEST')));
UPDATE dw_schedule_trigger SET status='CANCELLED',data_json=JSON_SET(data_json,'$.status','CANCELLED','$.reason','WORKSPACE_RETIRED')
 WHERE status IN ('PENDING','RETRY_WAIT')
 AND schedule_id IN (SELECT id FROM dw_workflow_schedule WHERE workspace_id IN (SELECT id FROM dw_workspace WHERE workspace_type IN ('ARCHIVED','TEST')));
UPDATE dw_preference SET data_json=JSON_SET(data_json,'$.workspaceId','local-workspace','$.openTabs',JSON_ARRAY(),'$.activeId','')
 WHERE JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.workspaceId')) IN (SELECT id FROM dw_workspace WHERE workspace_type IN ('ARCHIVED','TEST'));
