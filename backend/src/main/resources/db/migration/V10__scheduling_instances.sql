-- Automatic slots and each explicit backfill batch have independent instances.
ALTER TABLE dw_task_trigger
 ADD COLUMN batch_key VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN business_date VARCHAR(10) NULL,
 ADD COLUMN run_id VARCHAR(64) NULL,
 DROP INDEX uq_task_fire,
 ADD UNIQUE KEY uq_task_fire(schedule_id,scheduled_at,batch_key),
 ADD INDEX ix_task_trigger_batch(batch_key,status),
 ADD INDEX ix_task_trigger_date(business_date,schedule_id,batch_key);
UPDATE dw_task_trigger SET business_date=JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.businessDate'))
 WHERE JSON_EXTRACT(data_json,'$.businessDate') IS NOT NULL;
UPDATE dw_task_trigger SET run_id=JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.runId'))
 WHERE JSON_EXTRACT(data_json,'$.runId') IS NOT NULL AND JSON_TYPE(JSON_EXTRACT(data_json,'$.runId'))<>'NULL';
ALTER TABLE dw_schedule_trigger
 ADD COLUMN batch_key VARCHAR(64) NOT NULL DEFAULT '',
 ADD COLUMN business_date VARCHAR(10) NULL,
 DROP INDEX uq_schedule_fire,
 ADD UNIQUE KEY uq_schedule_fire(schedule_id,scheduled_at,batch_key),
 ADD INDEX ix_workflow_trigger_state(status),
 ADD INDEX ix_workflow_trigger_batch(batch_key,status),
 ADD INDEX ix_workflow_trigger_date(business_date,schedule_id,batch_key);
UPDATE dw_schedule_trigger SET business_date=JSON_UNQUOTE(JSON_EXTRACT(data_json,'$.businessDate'))
 WHERE JSON_EXTRACT(data_json,'$.businessDate') IS NOT NULL;
ALTER TABLE dw_run ADD INDEX ix_run_task_state(workspace_id,object_id,status);
