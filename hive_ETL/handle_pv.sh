#!/bin/sh



for((i=1;i<=3;i++))
do
now_date=`date -d "$i days ago" +%Y-%m-%d`

hive -e " create database if not exists statistics_data;

use statistics_data;

create table if not exists pv_old(request_json string) PARTITIONED BY (site string, month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;


create table if not exists pv(status string,remote_addr string,http_x_forwarded_for string,time_stamp string,
request_uri_ca_kw string,request_uri_ca_s string,request_uri_ca_source string,request_uri_ca_id string,request_uri_ca_from string,request_uri_ca_n string,request_uri_city string,request_uri_sid string,request_uri_ca_i string,request_uri_ca_name string,request_uri_reqid string,request_uri_ifid string,request_uri_uuid string,request_uri_from string,request_uri_third_auth string,request_uri_dmuser string,request_uri_ua string,request_uri_to string,
request_uri_dmch_post_id string,request_uri_dmch_cooperation_type string,request_uri_dmch_post_title string,request_uri_dmch_city string,request_uri_dmch_reward_flag string,request_uri_dmch_page string,request_uri_dmch_pn string,
request_uri_refer_ca_i string,request_uri_refer_ca_n string,request_uri_refer_ca_name string,request_uri_refer_ca_s string,request_uri_refer_domain string,request_uri_refer_ca_kw string,request_uri_refer_ca_source string,request_uri_refer_os string,request_uri_refer_ca_from string,request_uri_refer_page string,request_uri_refer_from string) PARTITIONED BY (site string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

load data inpath '/data/service_data/hadoop/weblogs/wapp/${now_date:2:8}/*' into table pv_old partition (site = 'wapp_m',month=${now_date:5:2}, day =${now_date:8:2});


add file /home/rd/fuxinxin/python/handle_pv.py;

INSERT OVERWRITE TABLE pv partition (site = 'wapp_m',month=${now_date:5:2}, day =${now_date:8:2})
SELECT
  TRANSFORM (request_json)
 USING 'python handle_pv.py'
AS (status,remote_addr,http_x_forwarded_for,time_stamp,request_uri_ca_kw,request_uri_ca_s,request_uri_ca_source,request_uri_ca_id,request_uri_ca_from,request_uri_ca_n,request_uri_city,request_uri_sid,request_uri_ca_i,request_uri_ca_name,request_uri_reqid,request_uri_ifid,request_uri_uuid,request_uri_from,request_uri_third_auth,request_uri_dmuser,request_uri_ua,request_uri_to,request_uri_dmch_post_id,request_uri_dmch_cooperation_type,request_uri_dmch_post_title,request_uri_dmch_city,request_uri_dmch_reward_flag,request_uri_dmch_page,request_uri_dmch_pn,request_uri_refer_ca_i,request_uri_refer_ca_n,request_uri_refer_ca_name,request_uri_refer_ca_s,request_uri_refer_domain,request_uri_refer_ca_kw,request_uri_refer_ca_source,request_uri_refer_os,request_uri_refer_ca_from,request_uri_refer_page,request_uri_refer_from) FROM pv_old where site = 'wapp_m'  and month=${now_date:5:2} and day =${now_date:8:2};"


done


for((i=1;i<=3;i++))
do
now_date=`date -d "$i days ago" +%Y-%m-%d`

hive -e "create database if not exists statistics_data;

use statistics_data;

create table if not exists pv_old(request_json string) PARTITIONED BY (site string, month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;


create table if not exists pv(status string,remote_addr string,http_x_forwarded_for string,time_stamp string,
request_uri_ca_kw string,request_uri_ca_s string,request_uri_ca_source string,request_uri_ca_id string,request_uri_ca_from string,request_uri_ca_n string,request_uri_city string,request_uri_sid string,request_uri_ca_i string,request_uri_ca_name string,request_uri_reqid string,request_uri_ifid string,request_uri_uuid string,request_uri_from string,request_uri_third_auth string,request_uri_dmuser string,request_uri_ua string,request_uri_to string,
request_uri_dmch_post_id string,request_uri_dmch_cooperation_type string,request_uri_dmch_post_title string,request_uri_dmch_city string,request_uri_dmch_reward_flag string,request_uri_dmch_page string,request_uri_dmch_pn string,
request_uri_refer_ca_i string,request_uri_refer_ca_n string,request_uri_refer_ca_name string,request_uri_refer_ca_s string,request_uri_refer_domain string,request_uri_refer_ca_kw string,request_uri_refer_ca_source string,request_uri_refer_os string,request_uri_refer_ca_from string,request_uri_refer_page string,request_uri_refer_from string) PARTITIONED BY (site string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

load data inpath '/data/service_data/hadoop/weblogs/anap/${now_date:2:8}/*' into table pv_old partition (site = 'anap_pc',month=${now_date:5:2}, day =${now_date:8:2});


add file /home/rd/fuxinxin/python/handle_pv.py;

INSERT OVERWRITE TABLE pv partition (site = 'anap_pc',month=${now_date:5:2}, day =${now_date:8:2})
SELECT
  TRANSFORM (request_json)
 USING 'python handle_pv.py'
AS (status,remote_addr,http_x_forwarded_for,time_stamp,request_uri_ca_kw,request_uri_ca_s,request_uri_ca_source,request_uri_ca_id,request_uri_ca_from,request_uri_ca_n,request_uri_city,request_uri_sid,request_uri_ca_i,request_uri_ca_name,request_uri_reqid,request_uri_ifid,request_uri_uuid,request_uri_from,request_uri_third_auth,request_uri_dmuser,request_uri_ua,request_uri_to,request_uri_dmch_post_id,request_uri_dmch_cooperation_type,request_uri_dmch_post_title,request_uri_dmch_city,request_uri_dmch_reward_flag,request_uri_dmch_page,request_uri_dmch_pn,request_uri_refer_ca_i,request_uri_refer_ca_n,request_uri_refer_ca_name,request_uri_refer_ca_s,request_uri_refer_domain,request_uri_refer_ca_kw,request_uri_refer_ca_source,request_uri_refer_os,request_uri_refer_ca_from,request_uri_refer_page,request_uri_refer_from) FROM pv_old where site = 'anap_pc'  and month=${now_date:5:2} and day =${now_date:8:2};"

done



for((i=1;i<=3;i++))
do
now_date=`date -d "$i days ago" +%Y-%m-%d`

hive -e "create database if not exists statistics_data;

use statistics_data;

create table if not exists pv_old(request_json string) PARTITIONED BY (site string, month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;


create table if not exists pv(status string,remote_addr string,http_x_forwarded_for string,time_stamp string,
request_uri_ca_kw string,request_uri_ca_s string,request_uri_ca_source string,request_uri_ca_id string,request_uri_ca_from string,request_uri_ca_n string,request_uri_city string,request_uri_sid string,request_uri_ca_i string,request_uri_ca_name string,request_uri_reqid string,request_uri_ifid string,request_uri_uuid string,request_uri_from string,request_uri_third_auth string,request_uri_dmuser string,request_uri_ua string,request_uri_to string,
request_uri_dmch_post_id string,request_uri_dmch_cooperation_type string,request_uri_dmch_post_title string,request_uri_dmch_city string,request_uri_dmch_reward_flag string,request_uri_dmch_page string,request_uri_dmch_pn string,
request_uri_refer_ca_i string,request_uri_refer_ca_n string,request_uri_refer_ca_name string,request_uri_refer_ca_s string,request_uri_refer_domain string,request_uri_refer_ca_kw string,request_uri_refer_ca_source string,request_uri_refer_os string,request_uri_refer_ca_from string,request_uri_refer_page string,request_uri_refer_from string) PARTITIONED BY (site string,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

load data inpath '/data/service_data/hadoop/weblogs/wapc/${now_date:2:8}/*' into table pv_old partition (site = 'wapc_php_m',month=${now_date:5:2}, day =${now_date:8:2});


add file /home/rd/fuxinxin/python/handle_pv.py;

INSERT OVERWRITE TABLE pv partition (site = 'wapc_php_m',month=${now_date:5:2}, day =${now_date:8:2})
SELECT
  TRANSFORM (request_json)
 USING 'python handle_pv.py'
AS (status,remote_addr,http_x_forwarded_for,time_stamp,request_uri_ca_kw,request_uri_ca_s,request_uri_ca_source,request_uri_ca_id,request_uri_ca_from,request_uri_ca_n,request_uri_city,request_uri_sid,request_uri_ca_i,request_uri_ca_name,request_uri_reqid,request_uri_ifid,request_uri_uuid,request_uri_from,request_uri_third_auth,request_uri_dmuser,request_uri_ua,request_uri_to,request_uri_dmch_post_id,request_uri_dmch_cooperation_type,request_uri_dmch_post_title,request_uri_dmch_city,request_uri_dmch_reward_flag,request_uri_dmch_page,request_uri_dmch_pn,request_uri_refer_ca_i,request_uri_refer_ca_n,request_uri_refer_ca_name,request_uri_refer_ca_s,request_uri_refer_domain,request_uri_refer_ca_kw,request_uri_refer_ca_source,request_uri_refer_os,request_uri_refer_ca_from,request_uri_refer_page,request_uri_refer_from) FROM pv_old where site = 'wapc_php_m'  and month=${now_date:5:2} and day =${now_date:8:2};"

done

