-- One-time cleanup. Later user-created objects are never touched by this migration.
UPDATE dw_object SET deleted=TRUE, deletion_group=CONCAT('v6-',LEFT(SHA2(workspace_id,256),40)),
 version=version+1, updated_at=DATE_FORMAT(UTC_TIMESTAMP(),'%Y-%m-%dT%H:%i:%sZ')
 WHERE deleted=FALSE;

UPDATE dw_task_schedule SET enabled=FALSE,next_fire_at=NULL,version=version+1,
 data_json=JSON_SET(data_json,'$.enabled',CAST('false' AS JSON),'$.nextFireAt',NULL,'$.version',version,'$.pauseReason','WORKSPACE_CLEARED');
UPDATE dw_workflow_schedule SET enabled=FALSE,next_fire_at=NULL,version=version+1,
 data_json=JSON_SET(data_json,'$.enabled',CAST('false' AS JSON),'$.nextFireAt',NULL,'$.version',version,'$.pauseReason','WORKSPACE_CLEARED');
UPDATE dw_task_trigger SET status='CANCELLED',data_json=JSON_SET(data_json,'$.status','CANCELLED','$.reason','WORKSPACE_CLEARED')
 WHERE status IN ('PENDING','WAITING_DEPENDENCY','WAITING_RESOURCE','RETRY_WAIT');
UPDATE dw_schedule_trigger SET status='CANCELLED',data_json=JSON_SET(data_json,'$.status','CANCELLED','$.reason','WORKSPACE_CLEARED')
 WHERE status IN ('PENDING','RETRY_WAIT');
UPDATE dw_preference SET data_json=JSON_SET(data_json,'$.openTabs',JSON_ARRAY(),'$.activeId','');
