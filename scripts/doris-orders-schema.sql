CREATE DATABASE IF NOT EXISTS `test_ods`;

CREATE TABLE IF NOT EXISTS `test_ods`.`ods_orders_di` (
    `ds` DATE NOT NULL,
    `id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `total_amount` DECIMAL(18, 2) NOT NULL,
    `status` VARCHAR(20) NOT NULL,
    `ordered_at` DATETIME NOT NULL
)
AUTO PARTITION BY RANGE (date_trunc(`ds`, 'day')) ()
PROPERTIES (
    "partition.retention_count" = "400",
    "replication_num" = "1"
);
