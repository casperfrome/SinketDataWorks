USE studio_inventory;
SET @day1=(SELECT first_day FROM inventory_demo_config WHERE id=1);
SET @day2=DATE_ADD(@day1,INTERVAL 1 DAY);
SET @loaded=DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 1 HOUR);
INSERT IGNORE INTO dim_warehouse VALUES ('W1','华东中心仓','华东'),('W2','华北周转仓','华北'),('W3','华南储备仓','华南');
INSERT IGNORE INTO dim_sku VALUES ('S1','标准螺栓','件',10,20),('S2','精密轴承','件',20,10),('S3','密封组件','件',7.5,5);
INSERT IGNORE INTO ods_inventory_opening VALUES
 ('o1','OPEN-1',1,1,@day1,'W1',NULL,'S1',100,'VALID',@loaded),
 ('o2','OPEN-1',2,1,@day1,'W1',NULL,'S2',40,'VALID',@loaded),
 ('o3','OPEN-2',1,1,@day1,'W2',NULL,'S1',10,'VALID',@loaded),
 ('o4','OPEN-2',2,1,@day1,'W2',NULL,'S2',20,'VALID',@loaded),
 ('o5','OPEN-3',1,1,@day1,'W3',NULL,'S3',4,'VALID',@loaded);
INSERT IGNORE INTO ods_inventory_inbound VALUES
 ('i1','IN-1',1,1,@day1,'W1',NULL,'S1',45,'VALID',@loaded),
 ('i2','IN-1',1,2,@day1,'W1',NULL,'S1',50,'VALID',@loaded),
 ('i3','IN-1',1,2,@day1,'W1',NULL,'S1',50,'VALID',@loaded),
 ('i4','IN-CANCEL',1,1,@day1,'W1',NULL,'S2',999,'VALID',@loaded),
 ('i5','IN-CANCEL',1,2,@day1,'W1',NULL,'S2',999,'CANCELLED',@loaded),
 ('i6','IN-2',1,1,@day2,'W1',NULL,'S1',10,'VALID',@loaded);
INSERT IGNORE INTO ods_inventory_outbound VALUES
 ('x1','OUT-1',1,1,@day1,'W1',NULL,'S1',20,'VALID',@loaded),
 ('x2','OUT-2',1,1,@day1,'W1',NULL,'S2',5,'VALID',@loaded),
 ('x3','OUT-3',1,1,@day1,'W2',NULL,'S2',5,'VALID',@loaded),
 ('x4','OUT-4',1,1,@day2,'W1',NULL,'S2',3,'VALID',@loaded),
 ('x5','OUT-5',1,1,@day2,'W2',NULL,'S1',7,'VALID',@loaded),
 ('x6','OUT-NEGATIVE',1,1,@day2,'W3',NULL,'S3',6,'VALID',@loaded);
INSERT IGNORE INTO ods_inventory_transfer VALUES
 ('t1','TRANSFER-1',1,1,@day1,'W1','W2','S1',10,'VALID',@loaded),
 ('t2','TRANSFER-2',1,1,@day2,'W2','W1','S2',2,'VALID',@loaded);
INSERT IGNORE INTO ods_inventory_adjustment VALUES
 ('a1','COUNT-1',1,1,@day1,'W2',NULL,'S1',-2,'VALID',@loaded),
 ('a2','COUNT-2',1,1,@day2,'W2',NULL,'S2',2,'VALID',@loaded),
 ('a3','COUNT-3',1,1,@day2,'W1',NULL,'S1',-2,'VALID',@loaded);
