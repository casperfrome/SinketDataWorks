CREATE TABLE IF NOT EXISTS dw_workspace (
 id VARCHAR(64) PRIMARY KEY, name VARCHAR(255) NOT NULL, code VARCHAR(100) NOT NULL, region VARCHAR(100) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS dw_object (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, parent_id VARCHAR(64),
 kind VARCHAR(32) NOT NULL, node_type VARCHAR(100) NOT NULL, name VARCHAR(255) NOT NULL,
 description TEXT NOT NULL, content LONGTEXT NOT NULL, config_json JSON NOT NULL, tags_json JSON NOT NULL,
 favorite BOOLEAN NOT NULL DEFAULT FALSE, deleted BOOLEAN NOT NULL DEFAULT FALSE, version INT NOT NULL DEFAULT 1,
 owner VARCHAR(100) NOT NULL, updated_at VARCHAR(40) NOT NULL, deletion_group VARCHAR(64),
 parent_key VARCHAR(64) GENERATED ALWAYS AS (IFNULL(parent_id,'')) STORED,
 live_key VARCHAR(64) GENERATED ALWAYS AS (CASE WHEN deleted THEN id ELSE '' END) STORED,
 UNIQUE KEY uq_object_name (workspace_id,parent_key,name,live_key), INDEX ix_object_workspace (workspace_id,deleted),
 CONSTRAINT fk_object_workspace FOREIGN KEY (workspace_id) REFERENCES dw_workspace(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS dw_version (
 id VARCHAR(64) PRIMARY KEY, object_id VARCHAR(64) NOT NULL, version INT NOT NULL, name VARCHAR(255) NOT NULL,
 content LONGTEXT NOT NULL, config_json JSON NOT NULL, created_at VARCHAR(40) NOT NULL,
 UNIQUE KEY uq_object_version (object_id,version), CONSTRAINT fk_version_object FOREIGN KEY(object_id) REFERENCES dw_object(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS dw_run (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, object_id VARCHAR(64) NOT NULL,
 status VARCHAR(20) NOT NULL, data_json JSON NOT NULL, snapshot_json JSON NOT NULL, created_at VARCHAR(40) NOT NULL,
 INDEX ix_run_workspace (workspace_id,created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS dw_record (
 id VARCHAR(64) PRIMARY KEY, workspace_id VARCHAR(64) NOT NULL, kind VARCHAR(30) NOT NULL, data_json JSON NOT NULL,
 created_at VARCHAR(40) NOT NULL, INDEX ix_record_workspace (workspace_id,kind)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS dw_preference (
 id VARCHAR(100) PRIMARY KEY, data_json JSON NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS dw_file (
 object_id VARCHAR(64) PRIMARY KEY, storage_name VARCHAR(100) NOT NULL, original_name VARCHAR(255) NOT NULL,
 content_type VARCHAR(255) NOT NULL, file_size BIGINT NOT NULL,
 CONSTRAINT fk_file_object FOREIGN KEY(object_id) REFERENCES dw_object(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
