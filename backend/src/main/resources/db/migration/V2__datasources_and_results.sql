CREATE TABLE dw_datasource (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL,
 name VARCHAR(100) NOT NULL, host VARCHAR(253) NOT NULL, port INT NOT NULL,
 database_name VARCHAR(64) NOT NULL, username VARCHAR(100) NOT NULL,
 password_cipher TEXT NOT NULL, updated_at VARCHAR(40) NOT NULL,
 UNIQUE KEY uq_datasource_name(workspace_id,name),
 CONSTRAINT fk_datasource_workspace FOREIGN KEY(workspace_id) REFERENCES dw_workspace(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE dw_run_result (
 run_id VARCHAR(64) PRIMARY KEY, data_json JSON NOT NULL,
 CONSTRAINT fk_result_run FOREIGN KEY(run_id) REFERENCES dw_run(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
