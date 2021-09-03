
-- flink sql 建表语句


{"createTime":"2021-04-11 16:10:57","mtWmPoiId":"123456","platform":"3","sessionId":"3444444","shopName":"2","source":"shoplist","userId":"268193426"}

{"createTime":"2021-04-11 16:10:57","mtWmPoiId":"123456789","platform":"3","sessionId":"3444444","shopName":"1","source":"shoplist","userId":"268193426"}

sh bin/kafka-console-consumer.sh --zookeeper ELK01:2181 --from-beginning --topic user_behavior

CREATE TABLE ods_top10_shop (
  id STRING,
  mt_wm_poi_id STRING,
  shop_name STRING,
  source STRING,
  platform STRING,
  time_key STRING,
  action_num BIGINT,
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.25.1:3306/wm?useSSL=false&characterEncoding=UTF-8',
   'table-name' = 'ods_top10_shop',
   'username' = 'root',
   'password' = '123456'
);


insert into ods_top10_shop
select
REPLACE(UUID(),'-','') id,c.mt_wm_poi_id,c.shop_name,c.source,c.platform,c.time_key,c.action_num,'1' as status
from (
select
b.mt_wm_poi_id,b.shop_name,b.source,b.platform,b.action_num,b.time_key,ROW_NUMBER() OVER (PARTITION BY b.platform ORDER BY b.action_num DESC) as row_num
from (
select mt_wm_poi_id,shop_name,action_num,platform,source,dt||' '||hr||':'||mm time_key from hot_shop 
) b 
) c where c.row_num <=10;




select
c.mt_wm_poi_id||c.shop_name name
from ods_top10_shop c
;
insert into ods_top10_shop
select
REPLACE(UUID(),'-','') id,
c.mt_wm_poi_id,
c.shop_name,
c.source,
c.platform,
c.time_key,
c.num action_num,
'1' as status
from (
select
b.mt_wm_poi_id,
b.shop_name,
b.source,
b.platform,
b.num,
b.time_key,
ROW_NUMBER() OVER (PARTITION BY platform ORDER BY shop_name DESC) as row_num
from (
select mt_wm_poi_id,shop_name,10 as num,platform,source,'0000-00-00 00:00' as time_key from ods_hot_shop 
) b 
) c where c.row_num <=10;





