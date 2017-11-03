set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.vectorized.execution.enabled=true;
set hive.enforce.bucketing=true;

create table complex2 (c1 int, c2 string, c3 array<struct<c3_1: int, c3_2: string>>, c4 string, c5 array<struct<c5_1: int, c5_2: string>>) clustered by (c1) into 2 buckets stored as orc TBLPROPERTIES ("transactional"="true");
insert into complex2 select 111, 'abc', array(named_struct('c3_1', 1, 'c3_2', 'a'), named_struct('c3_1', 2, 'c3_2', 'b')), 'c4', array(named_struct('c5_1', 1, 'c5_2', 'a'), named_struct('c5_1', 2, 'c5_2', 'b')) from src limit 1;
select count(*) from complex2;
