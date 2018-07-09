#!/usr/bin/python2
# _*_encoding:utf-8_*_

import logging
import copy
from elasticsearch2 import Elasticsearch
from elasticsearch2 import helpers

logging.basicConfig(filename="es-insert-perf-eva.log", format='%(asctime)s%(levelname)-8s%(message)s',
                    datefmt="%Y-%m-%d %I:%M:%S%p ",
                    level="INFO")

config = {
    "dst_es": [
        "172.16.106.67:9200"
    ]
}

data = {
    "_index": "event_20180518",
    "_type": "event",
    "_id": "1",
    "_source": {
        "dst_address": "221.181.72.79",
        "sa_da": "172.16.100.151_221.181.72.79",
        "index_type": "event",
        "event_type": "/14YRL6KY0003/2K3DUQOZ0011",
        "src_address": "172.16.100.151",
        "dst_city": u"上海",
        "app_protocol": "http",
        "id": 45143659782145,
        "dst_province": u"上海",
        "original_log": u"<190>May 5 02:59:34 2018 H3C %%10FILTER/6/FILTER_ZONE_IPV4_EXECUTION: SrcZoneName(1025)=Trust;DstZoneName(1035)=Untrust;Type(1067)=ACL;ObjectPolicy(1072)=Trust-Untrust;RuleID(1078)=0;Protocol(1001)=TCP;Application(1002)=http;SrcIPAddr(1003)=172.16.100.151;SrcPort(1004)=60417;DstIPAddr(1007)=221.181.72.79;DstPort(1008)=80;MatchCount(1069)=1;Event(1048)=Permit;",
        "rule_name": u"FW_H3C_SecPathF100系列_6",
        "receive_time": 1525460402628,
        "event_name": u"网络连接",
        "dev_address": "192.168.14.2",
        "merge_count": 1,
        "src_address_str": "172.16.100.151",
        "occur_time": 1525460374000,
        "vendor": "H3C",
        "dst_country": u"中国",
        "product": u"FW_SecPathF100系列",
        "sa_sp_ap_da_dp": "172.16.100.151_60417_http_221.181.72.79_80",
        "dst_address_str": "221.181.72.79",
        "dst_geo": {
            "latitude": 31.231706,
            "longitude": 121.472644
        },
        "rule_id": "EPM7J8KR6723",
        "src_port": 60417,
        "event_content": "",
        "response": "/accept",
        "dst_port": 80,
        "event_level": 0
    }
}

if __name__ == "__main__":
    logging.info("==================== Start ====================")
    dst_es = Elasticsearch(hosts=config["dst_es"], sniff_on_start=True, sniff_on_connection_fail=True, timeout=120)
    bat = []
    _id = 1
    while True:
        item = copy.deepcopy(data)
        item["_id"] = _id
        bat.append(item)
        if (_id % 1000) == 0:
            helpers.bulk(client=dst_es, actions=bat, chunk_size=1000, max_chunk_bytes=209715200)
            bat = []

        if (_id % 30000) == 0:
            logging.info("Write 30000 items to ES.")
        _id += 1

    logging.info("==================== End ====================\n\n")
