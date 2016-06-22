#! /usr/bin/python
# -*- coding: utf-8 -*-
import sys
import json
import re
import urllib
import sys
reload(sys)

sys.setdefaultencoding("utf-8")

# JSON_KEYS = ['status', 'domain', 'request_time', 'remote_addr', 'http_x_forwarded_for', 'timestamp', 'request_uri']
# REQUEST_URL_KEYS = ['city','uuid','user_id','ua','sid','from','ca_name','ca_source','ca_from','ca_kw','ca_n','ca_s','ca_i','ca_id','ifid','label','dmuser','reqid','third_auth']
# DMCH_KEYS = ['city','pn','post_id','post_title','reward_flag','cooperation_type','to']
# REFER_KEYS = ['ca_name','ca_source','ca_from','ca_kw','os','domain','ca_s','ca_n','ca_i']
# DMALOG_FILEDS= ['atype','business','ca_name','ca_source','ca_from','ca_kw','dialog_button','label','is_login','has_profile']

#主方法
def main(argv):
        line = '{"timestamp": "2016-05-11T02:10:05+08:00","http_x_forwarded_for": "-","remote_addr": "112.109.150.222","remote_user": "-","domain": "analytics.doumi.com","server_addr": "10.1.188.218","http_referer": "http://m.doumi.com/gz/jz_1089124.htm?from=58_58app_post&ca_name=58&ca_source=58app&ca_from=post&ca_kw=1","request_method": "GET","request_uri": "/wape.gif?dmch=/jianzhi/17/detail/index@city=gz@post_id=1089124@post_title=%E6%8B%9B%E9%85%B7%E7%8B%97%E7%B9%81%E6%98%9F%E4%B8%BB%E6%92%AD@cooperation_type=2@reward_flag=1&dmalog=/jianzhi/17/detail/index@atype=click@ca_name=doumi@ca_source=m@ca_from=detail@ca_kw=1&city=-&from=58_58app_post&uuid=e55d5864-16d9-11e6-a68a-200bc793f08d&dmuser=2189710&sid=7598308967&ca_name=58&ca_source=58app&ca_from=post&ca_kw=1&ca_id=-&ca_n=-&ca_s=-&ca_i=-&refer=http%3A%2F%2Fm.doumi.com%2Fgz%2Fjz_1332536.htm%3Ffrom%3D58_58app_lbpost%26ca_name%3D58%26ca_source%3D58app%26ca_from%3Dlbpost%26ca_kw%3D1%26os%3Dandroid&ua=device:HUAWEI%20RIO-AL00%20Build/HuaweiRIO-AL00|os:Android%205.1;|webkit:537.36|browser:Version/4.0%20Chrome/37.0.0.0%20Mobile%20MQQBrowser/6.2%20TBS/036215|lang:zh-CN&ifid=-&rnd=0.0039235069416463375","http_version": "HTTP/1.1","request_time": 0.020,"upstream_response_time": "-","status": 200,"body_bytes_sent": 35,"http_user_agent": "Mozilla/5.0 (Linux; Android 5.1; HUAWEI RIO-AL00 Build/HuaweiRIO-AL00) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/37.0.0.0 Mobile MQQBrowser/6.2 TBS/036215 Safari/537.36"}'
   #for line in sys.stdin:
        try:
            sql_map = parse_json_key(line)
            sql_map = parse_request_uri(sql_map)
            sql_map = parse_dmch(sql_map)
            sql_map = parse_refer(sql_map)
            sql_map = handle_field(sql_map)
            sql_field = sql_map.keys()
            for key in sql_field:
                if sql_map[key] == '-':
                    sql_map[key] = ''
            for key in sql_field:
               sql_map[key] = str(sql_map[key])
            # for key in sql_field:
            #     print key + ":" +  sql_map[key]
            print '\t'.join([sql_map['status'],sql_map['remote_addr'],sql_map['http_x_forwarded_for'],sql_map['time_stamp'],sql_map['request_uri_ca_kw'],sql_map['request_uri_ca_s'],sql_map['request_uri_ca_source'],sql_map['request_uri_ca_id'],sql_map['request_uri_ca_from'],sql_map['request_uri_ca_n'],sql_map['request_uri_city'],sql_map['request_uri_sid'],sql_map['request_uri_ca_i'],sql_map['request_uri_ca_name'],sql_map['request_uri_reqid'],sql_map['request_uri_ifid'],sql_map['request_uri_uuid'],sql_map['request_uri_from'],sql_map['request_uri_third_auth'],sql_map['request_uri_dmuser'],sql_map['request_uri_ua'],sql_map['request_uri_to'],sql_map['request_uri_dmch_post_id'],sql_map['request_uri_dmch_cooperation_type'],sql_map['request_uri_dmch_post_title'],sql_map['request_uri_dmch_city'],sql_map['request_uri_dmch_reward_flag'],sql_map['request_uri_dmch_page'],sql_map['request_uri_dmch_pn'],sql_map['request_uri_refer_ca_i'],sql_map['request_uri_refer_ca_n'],sql_map['request_uri_refer_ca_name'],sql_map['request_uri_refer_ca_s'],sql_map['request_uri_refer_domain'],sql_map['request_uri_refer_ca_kw'],sql_map['request_uri_refer_ca_source'],sql_map['request_uri_refer_os'],sql_map['request_uri_refer_ca_from'],sql_map['request_uri_refer_page'],sql_map['request_uri_refer_from']])
        except Exception as  ex:
            pass
def handle_field(sql_map):
    if 'request_uri' in sql_map.keys():
        del sql_map['request_uri']

    if 'request_uri_dmalog' in sql_map.keys():
        del sql_map['request_uri_dmalog']

    if 'request_uri_dmch' in sql_map.keys():
        del sql_map['request_uri_dmch']

    if 'request_uri_refer' in sql_map.keys():
        del sql_map['request_uri_refer']

    return sql_map

#解析json
def parse_json_key(line):
    JSON_KEYS = ['status', 'remote_addr', 'http_x_forwarded_for', 'timestamp', 'request_uri']
    sql_map = {}
    new_line = line.strip()
    new_line =re.sub('\\\[0-9A-Za-z]{3}','',new_line)
    json_data = json.loads(new_line)
    for json_key in JSON_KEYS:
        sql_map[json_key] = json_data[json_key]
    sql_map['time_stamp'] = sql_map['timestamp']
    del sql_map['timestamp']
    return sql_map

#解析request_uri
def parse_request_uri(sql_map):
    REQUEST_URL_KEYS = ['city','uuid','ua','sid','from','ca_name','ca_source','ca_from','ca_kw','ca_n','ca_s','ca_i','ca_id','ifid','dmuser','reqid','third_auth','to']
    request_uri = str(sql_map['request_uri'])
    if 'e.gif' in request_uri or 'p.gif' in request_uri:
        sql_map['site'] = 'pc_site'
    elif 'wape.gif' in request_uri or 'wapp.gif' in request_uri or 'wwapc.gif' in request_uri:
        sql_map['site'] = 'm_site'

    request_uri = re.sub('^/([\s\S])*?gif\?','',request_uri)
    handle_array = REQUEST_URL_KEYS
    field_array  = re.split("&",request_uri)
    for  field in field_array:
        if 'dmch=' in field:
            sql_map['request_uri_dmch'] = field
        elif 'dmalog=' in field:
            sql_map['request_uri_dmalog'] = field
        elif 'refer=' in field:
            sql_map['request_uri_refer'] = field
        else:
            result_array = re.split("=",field)
            if len(result_array) ==2  :
                key = result_array[0]
                value = result_array[1]
                if key  in REQUEST_URL_KEYS:
                    handle_array.remove(key)
                    sql_map['request_uri_'+key] = value
    for handle_field in handle_array:
        sql_map['request_uri_'+handle_field] = ''
    return sql_map



#解析request_much
def parse_dmch(sql_map):
    DMCH_KEYS = ['city','pn','post_id','post_title','reward_flag','cooperation_type']
    dmch = str(sql_map['request_uri_dmch'])
    handle_array  = DMCH_KEYS
    field_array  = re.split("@",dmch)
    for field in field_array:
        result_array  = re.split("=",field)
        if len(result_array) == 2:
            key =  result_array[0]
            value = result_array[1]
            if key == 'dmch':
                sql_map['request_uri_dmch_page'] = value
            else:
                if key in DMCH_KEYS:
                    handle_array.remove(key)
                    sql_map['request_uri_dmch_'+key] = value
    for handle_field in handle_array:
        sql_map['request_uri_dmch_'+handle_field] = ''
    return sql_map


#解析refer
def parse_refer(sql_map):
    REFER_KEYS = ['ca_name','ca_source','ca_from','ca_kw','os','domain','ca_s','ca_n','ca_i','from']
    if 'request_uri_refer' in sql_map.keys():
        refer = urllib.unquote(sql_map['request_uri_refer'])
        handle_array = REFER_KEYS
        if refer != 'refer=-':
            refer_page = re.findall('refer=([\s\S]*?)\?',refer)
            if refer_page:
                sql_map['request_uri_refer_page'] = refer_page[0]
                replace_refer = refer.replace('refer='+refer_page[0]+'?','')
                field_array  = re.split("&",replace_refer)
                for field in field_array:
                    result_array  = re.split("=",field)
                    if len(result_array) == 2:
                        key =  result_array[0]
                        value = result_array[1]
                        if key in REFER_KEYS:
                            handle_array.remove(key)
                            sql_map['request_uri_refer_'+key] = value
            else:
                sql_map['request_uri_refer_page'] = refer.split("=")[1]
            for handle_field in handle_array:
                sql_map['request_uri_refer_'+handle_field] = ''
        else:
            for key in REFER_KEYS:
                sql_map['request_uri_refer_'+key] = ''
                sql_map['request_uri_refer_page']  = ''

    else:
        for key in REFER_KEYS:
            sql_map['request_uri_refer_'+key] = ''
            sql_map['request_uri_refer_page']  = ''
    return  sql_map

if __name__ == "__main__":
    main(sys.argv)
