
BATCH_DATE="1989"
create_shop_sql="use wm;
drop table if exists dwd_shop;
create table dwd_shop(
wm_poi_id string,
dp_shop_id bigint,
shop_name string,
shop_status string,
shop_pic string,
delivery_fee string,
delivery_type string,
delivery_time string,
min_fee double,
online_pay int,
bulletin string,
shipping_time string,
create_time string
)
partitioned by (batch_date string)
row format delimited fields terminated by '\t';
"
hive -e "${create_shop_sql}"
echo ${create_shop_sql}
#---dwd店铺插入语句----
dwd_insert_sql="use wm;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table dwd_shop partition(batch_date) 
select wm_poi_id,dp_shop_id,shop_name,shop_status,shop_pic,delivery_fee,
delivery_type,delivery_time,min_fee,online_pay,bulletin,shipping_time,create_time,'1989' from ods_shop where batch_date='1989' group by wm_poi_id,dp_shop_id;"
hive -e "${dwd_insert_sql}"
echo ${dwd_insert_sql}



set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table dwd_shop partition(batch_date) 
select wm_poi_id,dp_shop_id,collect_set(shop_name)[0] shop_name,collect_set(shop_status)[0] shop_status ,collect_set(shop_pic)[0] shop_pic,collect_set(delivery_fee)[0] delivery_fee,
collect_set(delivery_type)[0] delivery_type,collect_set(delivery_time)[0] delivery_time,collect_set(min_fee)[0] min_fee,collect_set(online_pay)[0] online_pay,collect_set(bulletin)[0] bulletin,collect_set(shipping_time)[0] shipping_time,collect_set(create_time)[0] create_time,'2020-04-03' from ods_shop where batch_date='1989' group by wm_poi_id,dp_shop_id;


select count(1) from dwd_shop;






