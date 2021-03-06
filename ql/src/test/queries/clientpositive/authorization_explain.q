set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.security.authorization.enabled=true;

-- JAVA_VERSION_SPECIFIC_OUTPUT

explain authorization select * from src join srcpart;
explain formatted authorization select * from src join srcpart;

explain authorization use default;
explain formatted authorization use default;
