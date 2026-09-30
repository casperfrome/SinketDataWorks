ALTER TABLE dw_run
 ADD COLUMN parent_run_id VARCHAR(64) NULL,
 ADD COLUMN graph_node_id VARCHAR(64) NULL,
 ADD INDEX ix_run_parent (parent_run_id, created_at),
 ADD UNIQUE KEY uq_run_graph_node (parent_run_id, graph_node_id);

CREATE TABLE dw_workflow_release (
 id VARCHAR(64) PRIMARY KEY,
 workspace_id VARCHAR(64) NOT NULL,
 workflow_id VARCHAR(64) NOT NULL,
 release_no INT NOT NULL,
 workflow_version INT NOT NULL,
 name VARCHAR(255) NOT NULL,
 note TEXT NOT NULL,
 created_at VARCHAR(40) NOT NULL,
 bundle_json JSON NOT NULL,
 UNIQUE KEY uq_workflow_release (workflow_id, release_no),
 INDEX ix_release_workspace (workspace_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
