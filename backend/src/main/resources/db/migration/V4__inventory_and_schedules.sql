ALTER TABLE dw_datasource
 ADD COLUMN materialization_enabled BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN materialization_targets JSON NULL;

CREATE TABLE dw_workflow_schedule (
 id VARCHAR(64) PRIMARY KEY,
 workspace_id VARCHAR(64) NOT NULL,
 workflow_id VARCHAR(64) NOT NULL,
 release_id VARCHAR(64) NOT NULL,
 enabled BOOLEAN NOT NULL DEFAULT FALSE,
 version INT NOT NULL,
 next_fire_at VARCHAR(40) NULL,
 data_json JSON NOT NULL,
 UNIQUE KEY uq_schedule_workflow (workflow_id),
 INDEX ix_schedule_due (enabled,next_fire_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE dw_schedule_trigger (
 id VARCHAR(64) PRIMARY KEY,
 schedule_id VARCHAR(64) NOT NULL,
 scheduled_at VARCHAR(40) NOT NULL,
 status VARCHAR(40) NOT NULL,
 run_id VARCHAR(64) NULL,
 data_json JSON NOT NULL,
 UNIQUE KEY uq_schedule_fire (schedule_id,scheduled_at),
 INDEX ix_trigger_schedule (schedule_id,scheduled_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
