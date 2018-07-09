#!/usr/bin/python2
# _*_encoding:utf-8_*_

import logging
import datetime
from elasticsearch import Elasticsearch

logging.basicConfig(filename="esmaintain-test.log", format='%(asctime)s%(levelname)-8s%(message)s',
                    datefmt="%Y-%m-%d %I:%M:%S%p ",
                    level="INFO")


def create_test_indices(es_host, prefix, days_before):
    indices = [(prefix + (datetime.date.today() + datetime.timedelta(days=(0 - x))).strftime("%Y%m%d")) for x in range(days_before)]
    for i in indices:
        es_host.indices.create(index=i)
        for j in range(10):
            data = {
                "@timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                "position": i,
                "count": j
            }
            es_host.index(index=i, doc_type="test", body=data)


if __name__ == '__main__':
    logging.info("==================== Start ====================")
    es = Elasticsearch("172.16.106.1:9200", sniff_on_start=True, sniff_on_connection_fail=True, timeout=120)
    create_test_indices(es, "eventing_", 20)
    create_test_indices(es, "alarming_", 20)
