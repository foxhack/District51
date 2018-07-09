#!/usr/bin/python2
# _*_encoding:utf-8_*_

import logging
import datetime
from elasticsearch2 import Elasticsearch
from elasticsearch2 import helpers

config = {
    "src_es": [
        "127.0.0.1:9200",
        "localhost:9200"
    ],
    "dst_es": [
        "127.0.0.1:9200",
        "localhost:9200"
    ],
    "time_range": ("2018-03-29", "2018-03-30")
}

logging.basicConfig(filename="importer.log", format='%(asctime)s%(levelname)-8s%(message)s',
                    datefmt="%Y-%m-%d %I:%M:%S%p ",
                    level="INFO")


def get_indices(prefix, start_date=None, end_date=None):
    if start_date is None:
        start_date = datetime.date.today()
    if end_date is None:
        end_date = datetime.date.today()

    indices = prefix + start_date.strftime("%Y%m%d")
    while start_date < end_date:
        start_date = start_date + datetime.timedelta(days=1)
        indices = indices + ',' + prefix + start_date.strftime("%Y%m%d")

    logging.debug("The indices are: " + indices)
    return indices


def do_search_and_write(from_es, to_es, indices, query, step=5000, **kwargs):
    action = None
    if "action" in kwargs:
        action = kwargs["action"]
    # act_para = None
    # if "act_para" in kwargs:
    #    act_para = kwargs["act_para"]

    if (to_es is None) or (from_es is None):
        logging.log(logging.ERROR, "The destination es has not been correctly specified.")
        return
    else:
        search_res = helpers.scan(client=from_es, query=query, index=indices, scroll=u"6m", size=step)
        bat = []
        cnt = 0
        for i in search_res:
            bat.append(i)
            cnt += 1
            if (cnt % 30000) == 0:
                if action is not None:
                    action(objs=bat, **kwargs)
                helpers.bulk(client=to_es, actions=bat, chunk_size=step, max_chunk_bytes=209715200)
                del bat[:]
                logging.info("Write 30000 items to dst ES.")

        # insert the rest data into dst_es
        if action is not None:
            action(objs=bat, **kwargs)
        helpers.bulk(client=to_es, actions=bat, chunk_size=step, max_chunk_bytes=204857600)
        return


# There will be no more than 1k relevant events of the HES implementation, this implementation was based on that
def get_alarm_relevant_events(objs, **kwargs):
    from_es = kwargs["act_from_es"]
    to_es = kwargs["act_to_es"]
    events = []
    for obj in objs:
        alarm = obj["_source"]
        start_date = datetime.datetime.fromtimestamp(int(alarm["start_time"]) / 1000)
        end_date = datetime.datetime.fromtimestamp(int(alarm["end_time"]) / 1000)
        indices = [("event_" + (start_date + datetime.timedelta(x)).strftime("%Y%m%d"))
                   for x in range(0, (end_date - start_date).days + 1)]

        query_str = {"query": {"terms": {"_id": alarm["event_id"]}}}
        s_res = from_es.search(index=indices, body=query_str, allow_no_indices=True, size=2000)
        events.extend(s_res["hits"]["hits"])

        while len(events) > 3000:
            helpers.bulk(client=to_es, actions=events, chunk_size=3000)
            logging.debug("Write 3000 items into dest ES.")
            del events[:3000]

    # write the rest items into dst ES.
    helpers.bulk(client=to_es, actions=events, chunk_size=3000)
    logging.debug("Write the remaining " + str(len(events)) + " items into dest ES.")

    return


def get_and_write_all_alarms(from_es, to_es, start_date=None, end_date=None):
    alarm_merge_indices = get_indices("alarm_merge_", start_date=start_date, end_date=end_date)
    alarm_indices = get_indices("alarm_", start_date=start_date, end_date=end_date)
    query = {"query": {"match_all": {}}}
    do_search_and_write(from_es=from_es, to_es=to_es, indices=alarm_merge_indices, query=query)
    do_search_and_write(from_es=from_es, to_es=to_es, indices=alarm_indices, query=query,
                        action=get_alarm_relevant_events, act_from_es=from_es, act_to_es=to_es)


if __name__ == "__main__":
    logging.info("==================== Start ====================")

    src_es = Elasticsearch(hosts=config["src_es"], sniff_on_start=True, sniff_on_connection_fail=True, timeout=120)
    dst_es = Elasticsearch(hosts=config["dst_es"], sniff_on_start=True, sniff_on_connection_fail=True, timeout=120)
    if "time_range" in config:
        start = datetime.datetime.strptime(config["time_range"][0], "%Y-%m-%d")
        end = datetime.datetime.strptime(config["time_range"][1], "%Y-%m-%d")
    else:
        start = end = datetime.date.today()

    logging.info("GO GO GO! Rock and Roll! Let's do the job...")
    get_and_write_all_alarms(src_es, dst_es, start, end)

    logging.info("==================== End ====================\n")
