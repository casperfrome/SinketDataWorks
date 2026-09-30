ALTER TABLE dw_datasource ADD COLUMN type VARCHAR(16) NOT NULL DEFAULT 'MYSQL', ADD COLUMN options_json JSON NULL;
CREATE TABLE dw_sync_execution (
 run_id VARCHAR(64) PRIMARY KEY,
 service_url VARCHAR(512) NOT NULL,
 state_store_id VARCHAR(64) NULL,
 remote_run_id VARCHAR(64) NULL,
 target_key CHAR(64) NOT NULL,
 submitted BOOLEAN NOT NULL DEFAULT FALSE,
 cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
 recovery_reason VARCHAR(64) NULL,
 updated_at VARCHAR(40) NOT NULL
);
CREATE TABLE dw_sync_target_lock (
 target_key CHAR(64) PRIMARY KEY,
 run_id VARCHAR(64) NOT NULL UNIQUE
);
