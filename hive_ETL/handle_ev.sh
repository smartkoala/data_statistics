#!/bin/sh



for((i=6;i<=8;i++))
do
now_date=`date -d "$i days ago" +%Y-%m-%d`

hive -e "create database if not exists statistics;

use statistics;

create table if not exists ev_old(request_json string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;


create table if not exists ev(site string ,status string,remote_addr string,http_x_forwarded_for string,time_stamp string,
request_uri_ca_kw string,request_uri_ca_s string,request_uri_ca_source string,request_uri_ca_id string,request_uri_ca_from string,request_uri_ca_n string,request_uri_city string,request_uri_sid string,request_uri_ca_i string,request_uri_ca_name string,request_uri_reqid string,request_uri_ifid string,request_uri_uuid string,request_uri_from string,request_uri_third_auth string,request_uri_dmuser string,request_uri_ua string,request_uri_to string,
request_uri_dmch_post_id string,request_uri_dmch_cooperation_type string,request_uri_dmch_post_title string,request_uri_dmch_city string,request_uri_dmch_reward_flag string,request_uri_dmch_page string,request_uri_dmch_pn string,
request_uri_dmalog_label string,request_uri_dmalog_city string,request_uri_dmalog_ca_from string,request_uri_dmalog_ca_source string,request_uri_dmalog_business string,request_uri_dmalog_page string,request_uri_dmalog_ca_name string,request_uri_dmalog_has_profile string,request_uri_dmalog_atype string,request_uri_dmalog_dialog_button string,request_uri_dmalog_ca_kw string,
request_uri_refer_ca_i string,request_uri_refer_ca_n string,request_uri_refer_ca_name string,request_uri_refer_ca_s string,request_uri_refer_domain string,request_uri_refer_ca_kw string,request_uri_refer_ca_source string,request_uri_refer_os string,request_uri_refer_ca_from string,request_uri_refer_page string,request_uri_refer_from string) PARTITIONED BY (site string ,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

load data inpath '/data/service_data/hadoop/weblogs/wape/${yesterday:2:8}/*' into table pv_old partition (site = 'wape_m',month=${now_date:5:2}, day =${now_date:8:2});


add file /home/hadoop/fuxinxin/python/handle_ev.py;

INSERT OVERWRITE TABLE ev partition (site = 'wape_m',month=${now_date:5:2}, day =${now_date:8:2})
SELECT
  TRANSFORM (request_json)
  USING 'python handle_ev.py'
  AS (status,remote_addr,http_x_forwarded_for,time_stamp,
  request_uri_ca_kw,request_uri_ca_s,request_uri_ca_source,request_uri_ca_id,request_uri_ca_from,request_uri_ca_n,request_uri_city,request_uri_sid,request_uri_ca_i,request_uri_ca_name,request_uri_reqid,request_uri_ifid,request_uri_uuid,request_uri_from,request_uri_third_auth,request_uri_dmuser,request_uri_ua,request_uri_to,
  request_uri_dmch_post_id,request_uri_dmch_cooperation_type,request_uri_dmch_post_title,request_uri_dmch_city,request_uri_dmch_reward_flag,request_uri_dmch_page,request_uri_dmch_pn,
  request_uri_dmalog_label,request_uri_dmalog_city,request_uri_dmalog_ca_from,request_uri_dmalog_ca_source,request_uri_dmalog_business,request_uri_dmalog_page,request_uri_dmalog_ca_name,request_uri_dmalog_has_profile,request_uri_dmalog_atype,request_uri_dmalog_dialog_button,request_uri_dmalog_ca_kw,
  request_uri_refer_ca_i,request_uri_refer_ca_n,request_uri_refer_ca_name,request_uri_refer_ca_s,request_uri_refer_domain,request_uri_refer_ca_kw,request_uri_refer_ca_source,request_uri_refer_os,request_uri_refer_ca_from,request_uri_refer_page,request_uri_refer_from)
FROM ev_old where site = 'wape_m'  and month=${now_date:5:2} and day =${now_date:8:2};"


done


for((i=6;i<=8;i++))
do
now_date=`date -d "$i days ago" +%Y-%m-%d`

hive -e "create database if not exists statistics;

use statistics;

create table if not exists ev_old(request_json string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;


create table if not exists ev(site string ,status string,remote_addr string,http_x_forwarded_for string,time_stamp string,
request_uri_ca_kw string,request_uri_ca_s string,request_uri_ca_source string,request_uri_ca_id string,request_uri_ca_from string,request_uri_ca_n string,request_uri_city string,request_uri_sid string,request_uri_ca_i string,request_uri_ca_name string,request_uri_reqid string,request_uri_ifid string,request_uri_uuid string,request_uri_from string,request_uri_third_auth string,request_uri_dmuser string,request_uri_ua string,request_uri_to string,
request_uri_dmch_post_id string,request_uri_dmch_cooperation_type string,request_uri_dmch_post_title string,request_uri_dmch_city string,request_uri_dmch_reward_flag string,request_uri_dmch_page string,request_uri_dmch_pn string,
request_uri_dmalog_label string,request_uri_dmalog_city string,request_uri_dmalog_ca_from string,request_uri_dmalog_ca_source string,request_uri_dmalog_business string,request_uri_dmalog_page string,request_uri_dmalog_ca_name string,request_uri_dmalog_has_profile string,request_uri_dmalog_atype string,request_uri_dmalog_dialog_button string,request_uri_dmalog_ca_kw string,
request_uri_refer_ca_i string,request_uri_refer_ca_n string,request_uri_refer_ca_name string,request_uri_refer_ca_s string,request_uri_refer_domain string,request_uri_refer_ca_kw string,request_uri_refer_ca_source string,request_uri_refer_os string,request_uri_refer_ca_from string,request_uri_refer_page string,request_uri_refer_from string) PARTITIONED BY (site string ,month string,day string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;

load data inpath '/data/service_data/hadoop/weblogs/wape/${yesterday:2:8}/*' into table pv_old partition (site = 'anae_pc',month=${now_date:5:2}, day =${now_date:8:2});


add file /home/hadoop/fuxinxin/python/handle_ev.py;

INSERT OVERWRITE TABLE ev partition (site = 'anae_pc',month=${now_date:5:2}, day =${now_date:8:2})
SELECT
  TRANSFORM (request_json)
  USING 'python handle_ev.py'
  AS (status,remote_addr,http_x_forwarded_for,time_stamp,
  request_uri_ca_kw,request_uri_ca_s,request_uri_ca_source,request_uri_ca_id,request_uri_ca_from,request_uri_ca_n,request_uri_city,request_uri_sid,request_uri_ca_i,request_uri_ca_name,request_uri_reqid,request_uri_ifid,request_uri_uuid,request_uri_from,request_uri_third_auth,request_uri_dmuser,request_uri_ua,request_uri_to,
  request_uri_dmch_post_id,request_uri_dmch_cooperation_type,request_uri_dmch_post_title,request_uri_dmch_city,request_uri_dmch_reward_flag,request_uri_dmch_page,request_uri_dmch_pn,
  request_uri_dmalog_label,request_uri_dmalog_city,request_uri_dmalog_ca_from,request_uri_dmalog_ca_source,request_uri_dmalog_business,request_uri_dmalog_page,request_uri_dmalog_ca_name,request_uri_dmalog_has_profile,request_uri_dmalog_atype,request_uri_dmalog_dialog_button,request_uri_dmalog_ca_kw,
  request_uri_refer_ca_i,request_uri_refer_ca_n,request_uri_refer_ca_name,request_uri_refer_ca_s,request_uri_refer_domain,request_uri_refer_ca_kw,request_uri_refer_ca_source,request_uri_refer_os,request_uri_refer_ca_from,request_uri_refer_page,request_uri_refer_from)
FROM ev_old where site = 'anae_pc'  and month=${now_date:5:2} and day =${now_date:8:2};"

done

