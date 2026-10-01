CREATE TABLE dw_realtime_cdc_endpoint (
 endpoint VARCHAR(320) COLLATE utf8mb4_bin PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE dw_realtime_cdc_reservation (
 id CHAR(64) PRIMARY KEY, reservation_key CHAR(64) NOT NULL,
 owner_type VARCHAR(16) NOT NULL, owner_id VARCHAR(64) NOT NULL, attempt_id VARCHAR(64) NOT NULL,
 endpoint VARCHAR(320) COLLATE utf8mb4_bin NOT NULL, table_name VARCHAR(512) NOT NULL,
 first_id BIGINT NOT NULL, last_id BIGINT NOT NULL,
 released BOOLEAN NOT NULL DEFAULT FALSE, created_at VARCHAR(40) NOT NULL, released_at VARCHAR(40) NULL,
 released_key CHAR(64) GENERATED ALWAYS AS (CASE WHEN released THEN id ELSE '' END) STORED,
 UNIQUE KEY uq_realtime_cdc_active_identity(reservation_key,released_key),
 INDEX ix_realtime_cdc_owner(owner_type,owner_id,attempt_id,released),
 INDEX ix_realtime_cdc_overlap(endpoint,released,first_id,last_id),
 CONSTRAINT fk_realtime_cdc_endpoint FOREIGN KEY(endpoint) REFERENCES dw_realtime_cdc_endpoint(endpoint),
 CONSTRAINT ck_realtime_cdc_interval CHECK(first_id>0 AND last_id>=first_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
