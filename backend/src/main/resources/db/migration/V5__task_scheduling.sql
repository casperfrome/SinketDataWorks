CREATE TABLE dw_task_release (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, task_id VARCHAR(64) NOT NULL,
 release_no INT NOT NULL, data_json JSON NOT NULL,
 UNIQUE KEY uq_task_release(task_id,release_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_task_schedule (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, task_id VARCHAR(64) NOT NULL,
 enabled BOOLEAN NOT NULL DEFAULT FALSE, version INT NOT NULL, next_fire_at VARCHAR(40), data_json JSON NOT NULL,
 UNIQUE KEY uq_task_schedule(task_id), INDEX ix_task_schedule_due(enabled,next_fire_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_task_trigger (
 id VARCHAR(64) PRIMARY KEY, schedule_id VARCHAR(64) NOT NULL, scheduled_at VARCHAR(40) NOT NULL,
 status VARCHAR(40) NOT NULL, data_json JSON NOT NULL,
 UNIQUE KEY uq_task_fire(schedule_id,scheduled_at), INDEX ix_task_trigger_state(status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
