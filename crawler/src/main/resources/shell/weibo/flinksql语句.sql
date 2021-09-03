-- flinksql语句
-- 关键用户表
drop table if exists kafka_ods_tm_key_users;
CREATE TABLE kafka_ods_tm_key_users (
  user_id STRING COMMENT '用户id' ,
  user_name STRING COMMENT '用户名称',
  province_code STRING COMMENT '省份编码',
  is_key_user INT COMMENT '是否关键用户 0-否 1-是',
  remark STRING COMMENT '备注'
 
) WITH (
 'connector' = 'kafka',
 'topic' = 'ods_tm_key_users',
 'properties.bootstrap.servers' = 'ELK01:9092',
 'properties.group.id' = 'ods_group',
 'format' = 'maxwell-json'
);

-- 省份表

drop table if exists kafka_ods_tm_province;
CREATE TABLE kafka_ods_tm_province (
  id INT COMMENT '主键id' ,
  code STRING COMMENT '省份编码',
  name STRING COMMENT '省份名称',
  code_level INT COMMENT '级别'
) WITH (
 'connector' = 'kafka',
 'topic' = 'ods_tm_province',
 'properties.bootstrap.servers' = 'ELK01:9092',
 'properties.group.id' = 'ods_group',
 'format' = 'maxwell-json'
);


--hbase 重点用户表
drop table if exists hbase_tm_key_users;
CREATE TABLE hbase_tm_key_users (
 rowkey STRING,
 info ROW<user_name STRING,province_code STRING,is_key_user INT,remark STRING>,
 PRIMARY KEY (rowkey) NOT ENFORCED
 ) WITH (
 'connector' = 'hbase-1.4',
 'table-name' = 'wm:hbase_tm_key_users',
 'zookeeper.quorum' = 'ELK01:2181,ELK02:2181,ELK03:2181',
 'zookeeper.znode.parent' = '/hbase'
);

create 'wm:hbase_tm_key_users', 'info';
scan 'wm:hbase_tm_key_users' 

INSERT INTO hbase_tm_key_users
SELECT user_id, ROW(user_name, province_code,is_key_user,remark) as info from kafka_ods_tm_key_users;

SELECT * FROM hbase_tm_key_users;

--hbase 重点用户表
drop table if exists hbase_tm_province;
CREATE TABLE hbase_tm_province (
 rowkey STRING,
 info ROW<name STRING,code_level INT>,
 PRIMARY KEY (rowkey) NOT ENFORCED
 ) WITH (
 'connector' = 'hbase-1.4',
 'table-name' = 'wm:hbase_tm_province',
 'zookeeper.quorum' = 'ELK01:2181,ELK02:2181,ELK03:2181',
 'zookeeper.znode.parent' = '/hbase'
);


create 'wm:hbase_tm_province', 'info';
scan 'wm:hbase_tm_province' 

INSERT INTO hbase_tm_province
SELECT code, ROW(name,code_level) as info from kafka_ods_tm_province;

SELECT * FROM hbase_tm_province;

disable 'wm:hbase_tm_province'
drop 'wm:hbase_tm_province'


-- 舆情信息表
drop table if exists kafka_ods_tm_key_users;
CREATE TABLE kafka_ods_tm_key_users (
  id STRING,
  user_id STRING,
  user_name STRING,
  title STRING,
  title_type INT,
  content STRING,
  create_time STRING,
  urls STRING,
  comment_num BIGINT,
  forward_num BIGINT,
  like_num BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_tm_key_users',
  'properties.bootstrap.servers' = 'ELK01:2181',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
   'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true'
);

-- dwd舆情信息表
drop table if exists kafka_dwd_tm_key_users;
CREATE TABLE kafka_dwd_tm_key_users (
  id STRING,
  user_id STRING,
  user_name STRING,
  title STRING,
  title_type INT,
  content STRING,
  create_time STRING,
  urls STRING,
  comment_num BIGINT,
  forward_num BIGINT,
  like_num BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'properties.bootstrap.servers' = '192.168.25.128:2181',
  'topic' = 'dwd_tm_key_users',
  'properties.group.id' = 'testGroup',
  'key.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
  'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer'
);

-- 账户信息
drop table if exists kafka_ods_tm_account;
CREATE TABLE kafka_ods_tm_account (
  user_id STRING,
  user_name STRING,
  head_url STRING,
  province_code STRING,
  fans_num BIGINT,
  follow_num BIGINT,
  update_time STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'properties.bootstrap.servers' = '192.168.25.128:2181',
  'topic' = 'ods_tm_account',
  'properties.group.id' = 'testGroup',
  'key.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'value.fields-include' = 'EXCEPT_KEY'
);



