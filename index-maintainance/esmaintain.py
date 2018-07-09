#!/usr/bin/python2
# _*_encoding:utf-8_*_

import logging
import datetime
from elasticsearch2 import Elasticsearch

config = {
    "es_cluster": [
        "127.0.0.1:9200",
        "localhost:9200"
    ],
    "indices_prefix": [
        "event_",
        "alarm_"
    ],
    "retention_time": 7,
    "dry-run": True
}

logging.basicConfig(filename="esmaintain.log", format='%(asctime)s%(levelname)-8s%(message)s',
                    datefmt="%Y-%m-%d %I:%M:%S%p ",
                    level="INFO")


def get_indices_to_be_closed(es_host, prefix_list, retention_days):
    indices = []
    upper_date = (datetime.date.today() + datetime.timedelta(days=(0 - retention_days))).strftime("%Y%m%d")
    close_to = [(x, x+upper_date) for x in prefix_list]
    logging.debug("The indices earlier than the listed items will be closed. The list: " + str(close_to))
    for i in es_host.cat.indices().split('\n'):
        index = i.split()
        if len(i) > 4 and index[1] == "open":
            for j in close_to:
                if (j[0] in index[2]) and index[2] <= j[1]:
                    indices.append(index[2])

    indices.sort()
    return indices


if __name__ == '__main__':
    logging.info("==================== Start ====================")
    es = Elasticsearch(hosts=config["es_cluster"], sniff_on_start=True, sniff_on_connection_fail=True, timeout=120)
    prefix = config["indices_prefix"]
    retain = config["retention_time"]
    dryrun = config["dry-run"]

    logging.info("let's do the job, carry small and live large...")
    logging.info("IMPORTANT: the indices should follow the naming pattern xxxx_yyyyMMdd")
    indices_to_close = get_indices_to_be_closed(es_host=es, prefix_list=prefix, retention_days=retain)
    if dryrun:
        logging.info("<Dry-run> The following indices will be closed: " + str(indices_to_close))
    else:
        if len(indices_to_close) > 0:
            # close indices in batches to avoid url exceed 4096 limit
            while len(indices_to_close) > 20:
                tmp = indices_to_close[0:20]
                es.indices.close(index=tmp, ignore=[400, 404])
                del indices_to_close[0:20]
                logging.info("The following indices been closed: " + str(tmp))

            es.indices.close(index=indices_to_close, ignore=[400, 404])
            logging.info("The following indices been closed: " + str(indices_to_close))

    logging.info("==================== End ====================\n")

