CREATE DATABASE IF NOT EXISTS studio_inventory CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE studio_inventory;
CREATE TABLE IF NOT EXISTS inventory_demo_config (id INT PRIMARY KEY, first_day DATE NOT NULL);
INSERT IGNORE INTO inventory_demo_config VALUES (1,DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY));
CREATE TABLE IF NOT EXISTS dim_warehouse (warehouse_id VARCHAR(32) PRIMARY KEY, warehouse_name VARCHAR(100) NOT NULL, region VARCHAR(100) NOT NULL);
CREATE TABLE IF NOT EXISTS dim_sku (sku_id VARCHAR(32) PRIMARY KEY, sku_name VARCHAR(100) NOT NULL, unit VARCHAR(20) NOT NULL, standard_cost DECIMAL(18,4) NOT NULL, safety_stock DECIMAL(20,4) NOT NULL);
CREATE TABLE IF NOT EXISTS ods_inventory_opening (
 record_id VARCHAR(64) PRIMARY KEY, document_no VARCHAR(64) NOT NULL, line_no INT NOT NULL,
 revision INT NOT NULL, business_date DATE NOT NULL, warehouse_id VARCHAR(32), to_warehouse_id VARCHAR(32),
 sku_id VARCHAR(32) NOT NULL, quantity DECIMAL(20,4), status VARCHAR(16) NOT NULL,
 ingested_at DATETIME(6) NOT NULL, INDEX ix_source_version(document_no,line_no,revision,ingested_at)
);
CREATE TABLE IF NOT EXISTS ods_inventory_inbound LIKE ods_inventory_opening;
CREATE TABLE IF NOT EXISTS ods_inventory_outbound LIKE ods_inventory_opening;
CREATE TABLE IF NOT EXISTS ods_inventory_transfer LIKE ods_inventory_opening;
CREATE TABLE IF NOT EXISTS ods_inventory_adjustment LIKE ods_inventory_opening;

CREATE TABLE IF NOT EXISTS dwd_inventory_ledger_di (
 business_date DATE NOT NULL, row_key VARCHAR(180) NOT NULL, row_kind VARCHAR(24) NOT NULL,
 movement_type VARCHAR(24) NOT NULL, source_table VARCHAR(64) NOT NULL, source_doc VARCHAR(64) NOT NULL,
 warehouse_id VARCHAR(32) NOT NULL, sku_id VARCHAR(32) NOT NULL,
 quantity DECIMAL(20,4) NOT NULL, unit_cost DECIMAL(18,4) NOT NULL, inventory_amount DECIMAL(24,4) NOT NULL,
 PRIMARY KEY (business_date,row_key), INDEX ix_ledger_warehouse(business_date,warehouse_id,sku_id)
);
CREATE TABLE IF NOT EXISTS dws_inventory_warehouse_di (
 business_date DATE NOT NULL, warehouse_id VARCHAR(32) NOT NULL,
 opening_qty DECIMAL(20,4) NOT NULL, inbound_qty DECIMAL(20,4) NOT NULL, outbound_qty DECIMAL(20,4) NOT NULL,
 transfer_in_qty DECIMAL(20,4) NOT NULL, transfer_out_qty DECIMAL(20,4) NOT NULL, adjustment_qty DECIMAL(20,4) NOT NULL,
 closing_qty DECIMAL(20,4) NOT NULL, opening_amount DECIMAL(24,4) NOT NULL, closing_amount DECIMAL(24,4) NOT NULL,
 outbound_amount DECIMAL(24,4) NOT NULL, sku_count INT NOT NULL, negative_sku_count INT NOT NULL, low_sku_count INT NOT NULL,
 PRIMARY KEY(business_date,warehouse_id)
);
CREATE TABLE IF NOT EXISTS ads_inventory_analysis_di (
 business_date DATE NOT NULL, warehouse_id VARCHAR(32) NOT NULL,
 warehouse_name VARCHAR(100) NOT NULL, region VARCHAR(100) NOT NULL,
 opening_qty DECIMAL(20,4) NOT NULL, inbound_qty DECIMAL(20,4) NOT NULL, outbound_qty DECIMAL(20,4) NOT NULL,
 transfer_in_qty DECIMAL(20,4) NOT NULL, transfer_out_qty DECIMAL(20,4) NOT NULL, adjustment_qty DECIMAL(20,4) NOT NULL,
 closing_qty DECIMAL(20,4) NOT NULL, opening_amount DECIMAL(24,4) NOT NULL, closing_amount DECIMAL(24,4) NOT NULL,
 outbound_amount DECIMAL(24,4) NOT NULL, sku_count INT NOT NULL, negative_sku_count INT NOT NULL, low_sku_count INT NOT NULL,
 net_change_qty DECIMAL(20,4) NOT NULL, low_stock_ratio DECIMAL(10,6) NOT NULL, anomaly VARCHAR(32) NOT NULL,
 PRIMARY KEY(business_date,warehouse_id)
);
CREATE TABLE IF NOT EXISTS etl_stage_dwd_inventory_ledger_di (
 build_id VARCHAR(64) NOT NULL,
 business_date DATE NOT NULL, row_key VARCHAR(180) NOT NULL, row_kind VARCHAR(24) NOT NULL,
 movement_type VARCHAR(24) NOT NULL, source_table VARCHAR(64) NOT NULL, source_doc VARCHAR(64) NOT NULL,
 warehouse_id VARCHAR(32) NOT NULL, sku_id VARCHAR(32) NOT NULL,
 quantity DECIMAL(20,4) NOT NULL, unit_cost DECIMAL(18,4) NOT NULL, inventory_amount DECIMAL(24,4) NOT NULL,
 PRIMARY KEY(build_id,business_date,row_key)
);
CREATE TABLE IF NOT EXISTS etl_stage_dws_inventory_warehouse_di (
 build_id VARCHAR(64) NOT NULL,
 business_date DATE NOT NULL, warehouse_id VARCHAR(32) NOT NULL,
 opening_qty DECIMAL(20,4) NOT NULL, inbound_qty DECIMAL(20,4) NOT NULL, outbound_qty DECIMAL(20,4) NOT NULL,
 transfer_in_qty DECIMAL(20,4) NOT NULL, transfer_out_qty DECIMAL(20,4) NOT NULL, adjustment_qty DECIMAL(20,4) NOT NULL,
 closing_qty DECIMAL(20,4) NOT NULL, opening_amount DECIMAL(24,4) NOT NULL, closing_amount DECIMAL(24,4) NOT NULL,
 outbound_amount DECIMAL(24,4) NOT NULL, sku_count INT NOT NULL, negative_sku_count INT NOT NULL, low_sku_count INT NOT NULL,
 PRIMARY KEY(build_id,business_date,warehouse_id)
);
CREATE TABLE IF NOT EXISTS etl_stage_ads_inventory_analysis_di (
 build_id VARCHAR(64) NOT NULL,
 business_date DATE NOT NULL, warehouse_id VARCHAR(32) NOT NULL,
 warehouse_name VARCHAR(100) NOT NULL, region VARCHAR(100) NOT NULL,
 opening_qty DECIMAL(20,4) NOT NULL, inbound_qty DECIMAL(20,4) NOT NULL, outbound_qty DECIMAL(20,4) NOT NULL,
 transfer_in_qty DECIMAL(20,4) NOT NULL, transfer_out_qty DECIMAL(20,4) NOT NULL, adjustment_qty DECIMAL(20,4) NOT NULL,
 closing_qty DECIMAL(20,4) NOT NULL, opening_amount DECIMAL(24,4) NOT NULL, closing_amount DECIMAL(24,4) NOT NULL,
 outbound_amount DECIMAL(24,4) NOT NULL, sku_count INT NOT NULL, negative_sku_count INT NOT NULL, low_sku_count INT NOT NULL,
 net_change_qty DECIMAL(20,4) NOT NULL, low_stock_ratio DECIMAL(10,6) NOT NULL, anomaly VARCHAR(32) NOT NULL,
 PRIMARY KEY(build_id,business_date,warehouse_id)
);
CREATE TABLE IF NOT EXISTS etl_publish_receipt (build_id VARCHAR(64) PRIMARY KEY, business_date DATE NOT NULL, committed_at DATETIME(6) NOT NULL);
CREATE TABLE IF NOT EXISTS etl_partition_publication (
 target_table VARCHAR(64) NOT NULL, business_date DATE NOT NULL, build_id VARCHAR(64) NOT NULL,
 lineage_json JSON NOT NULL, committed_at DATETIME(6) NOT NULL,
 PRIMARY KEY(target_table,business_date)
);
