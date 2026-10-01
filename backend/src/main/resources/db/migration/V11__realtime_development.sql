CREATE TABLE dw_realtime_folder (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, name VARCHAR(255) NOT NULL,
 UNIQUE KEY uq_realtime_folder_name(workspace_id,name),
 CONSTRAINT fk_realtime_folder_workspace FOREIGN KEY(workspace_id) REFERENCES dw_workspace(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_realtime_task (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, folder_id VARCHAR(64) NULL,
 name VARCHAR(255) NOT NULL, revision INT NOT NULL, deleted BOOLEAN NOT NULL DEFAULT FALSE,
 data_json JSON NOT NULL, updated_at VARCHAR(40) NOT NULL,
 folder_key VARCHAR(64) GENERATED ALWAYS AS (IFNULL(folder_id,'')) STORED,
 live_key VARCHAR(64) GENERATED ALWAYS AS (CASE WHEN deleted THEN id ELSE '' END) STORED,
 UNIQUE KEY uq_realtime_task_name(workspace_id,folder_key,name,live_key),
 INDEX ix_realtime_task_workspace(workspace_id,deleted),
 CONSTRAINT fk_realtime_task_workspace FOREIGN KEY(workspace_id) REFERENCES dw_workspace(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_realtime_draft (
 task_id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, data_json JSON NOT NULL,
 CONSTRAINT fk_realtime_draft_task FOREIGN KEY(task_id) REFERENCES dw_realtime_task(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_realtime_release (
 id VARCHAR(64) PRIMARY KEY, task_id VARCHAR(64) NOT NULL, workspace_id VARCHAR(64) NOT NULL,
 release_no INT NOT NULL, data_json JSON NOT NULL, created_at VARCHAR(40) NOT NULL,
 UNIQUE KEY uq_realtime_release_no(task_id,release_no),
 INDEX ix_realtime_release_workspace(workspace_id),
 CONSTRAINT fk_realtime_release_task FOREIGN KEY(task_id) REFERENCES dw_realtime_task(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_realtime_job (
 id VARCHAR(64) PRIMARY KEY, task_id VARCHAR(64) NOT NULL, workspace_id VARCHAR(64) NOT NULL,
 status VARCHAR(32) NOT NULL, data_json JSON NOT NULL, updated_at VARCHAR(40) NOT NULL,
 INDEX ix_realtime_job_workspace(workspace_id,updated_at), INDEX ix_realtime_job_status(status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_realtime_operation (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, task_id VARCHAR(64) NULL,
 request_id VARCHAR(128) NOT NULL, status VARCHAR(32) NOT NULL, data_json JSON NOT NULL,
 updated_at VARCHAR(40) NOT NULL, UNIQUE KEY uq_realtime_request(workspace_id,request_id),
 INDEX ix_realtime_operation_status(status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_realtime_active (
 task_id VARCHAR(64) PRIMARY KEY, job_id VARCHAR(64) NOT NULL UNIQUE,
 operation_id VARCHAR(64) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_realtime_preview (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, task_id VARCHAR(64) NULL,
 status VARCHAR(32) NOT NULL, data_json JSON NOT NULL, updated_at VARCHAR(40) NOT NULL,
 INDEX ix_realtime_preview_status(status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_realtime_import (
 workspace_id VARCHAR(64) NOT NULL, import_id VARCHAR(128) NOT NULL, digest CHAR(64) NOT NULL,
 created_at VARCHAR(40) NOT NULL, PRIMARY KEY(workspace_id,import_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
