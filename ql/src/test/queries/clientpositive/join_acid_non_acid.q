set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

CREATE TABLE orc_update_table (k1 INT, f1 STRING, op_code STRING)
CLUSTERED BY (k1) INTO 2 BUCKETS
STORED AS ORC TBLPROPERTIES("transactional"="true");

INSERT INTO TABLE orc_update_table VALUES (1, 'a', 'I');

CREATE TABLE orc_table (k1 INT, f1 STRING)
CLUSTERED BY (k1) SORTED BY (k1) INTO 2 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE orc_table VALUES (1, 'x');

set hive.cbo.enable=true;
SET hive.execution.engine=mr;
SET hive.auto.convert.join=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET hive.conf.validation=false;
SET hive.doing.acid=false;

SELECT t1.*, t2.* FROM orc_table t1
JOIN orc_update_table t2 ON t1.k1=t2.k1 ORDER BY t1.k1;
