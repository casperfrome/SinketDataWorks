CREATE TABLE dw_node_debug_parameters (
 object_id VARCHAR(64) PRIMARY KEY,
 parameters_json JSON NOT NULL,
 updated_at VARCHAR(40) NOT NULL,
 CONSTRAINT fk_debug_parameters_object FOREIGN KEY (object_id) REFERENCES dw_object(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
