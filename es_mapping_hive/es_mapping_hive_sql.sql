CREATE EXTERNAL TABLE  nginx_access_log(
dmver string,
dt timestamp,
host string,
uri string,
type string,
client_ip string,
domain string,
server_addr string,
verb string,
geoip_country_code string,
country_code2 string,
referrer string,
agent string,
geoip_ip string,
geoip_continent_code string,
geoip_region_name string,
geoip_city_name string,
geoip_real_region_name string,
useragent_name string,
useragent_os string,
useragent_os_name string,
useragent_device string,
useragent_major string,
useragent_minor string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
'es.nodes' = '10.1.188.1:8200,10.1.188.1:8200',
'es.index.auto.create' = 'false',
'es.resource' = 'nginx-access-log-2016.05.17/doumi_nginx_access',
'es.read.metadata' = 'true',
'es.mapping.names' = 'dmver:@version,dt:timestamp,host:host,uri:request,type:type,client_ip:client_ip,domain:domain,server_addr:server_addr,verb:verb,geoip_country_code:geoip.country_code2,referrer:referrer,agent:agent,geoip_ip:geoip.ip,geoip_continent_code:geoip.continent_code,geoip_region_name:geoip.region_name,geoip_city_name:geoip.city_name,geoip_real_region_name:geoip.real_region_name,useragent_name:name,useragent_os:os,useragent_os_name:os_name,useragent_device:device,useragent_major:major,useragent_minor:minor');
