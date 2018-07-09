#!/usr/bin/python2
# _*_encoding:utf-8_*_

import sys
import argparse
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import logging
import datetime

# trick for unicode of python2.7
# reload(sys)  # Reload does the trick!
# sys.setdefaultencoding('UTF8')
# trick for unicode of python2.7 end

logging.basicConfig(filename="transporter.log", format='%(asctime)s%(levelname)-8s%(message)s',
                    datefmt="%Y-%m-%d %I:%M:%S%p ",
                    level="INFO")


# get the user and mac address mapping
# prefix - the elasticsearch index prefix
# start - the date that data will be collected start from, will from today if not specified
# end - the date that data will be collected end to, if not specified then it will be today
def get_indices(prefix, start=None, end=None):
    if start is None:
        start = datetime.date.today()
    if end is None:
        end = datetime.date.today()

    indices = prefix + start.strftime("%Y%m%d")
    while start < end:
        start = start + datetime.timedelta(days=1)
        indices = indices + ',' + prefix + start.strftime("%Y%m%d")

    logging.log(logging.DEBUG, "The indices are: " + indices)
    return indices


# do search by 'query' condition, search result max to the 'size', and if can't return in
# 1 query, then search again with the 'step' length until reach the end.
def do_search(es, indices, query, step=5000):
    search_res = helpers.scan(client=es, query=query, index=indices, scroll=u"2m", size=step)
    ret = []
    for i in search_res:
        ret.append(i)

    return len(ret), ret


# do search by 'query' condition from the 'src_es', search result max to the 'size', and if can't return in
# 1 query, then search again with the 'step' length until reach the end. And write the results
# to the dst_es
def do_search_and_write(src_es, dst_es, indices, query, step=5000):
    if dst_es is None:
        logging.log(logging.ERROR, "The destination es has not been correctly specified.")
        return
    else:
        search_res = helpers.scan(client=src_es, query=query, index=indices, scroll=u"2m", size=step)
        bat = []
        cnt = 0
        for i in search_res:
            bat.append(i)
            cnt += 1
            if (cnt % 30000) == 0:
                helpers.bulk(client=dst_es, actions=bat, chunk_size=5000, max_chunk_bytes=204857600)
                del bat[:]
                logging.info("Write 30000 items to dst ES.")

        # insert the rest data into dst_es
        helpers.bulk(client=dst_es, actions=bat, chunk_size=5000, max_chunk_bytes=204857600)
        return


# almost the same as 'get_user_login_activities' except this will write the search result to another place
def get_and_write_user_login_activities(src_es, dst_es, start=None, end=None):
    indices = get_indices('event_', start, end)
    query_str = {"query": {"terms": {"event_name": [u"账号登录", u"Radius认证"]}}}
    do_search_and_write(src_es, dst_es, indices, query_str, 6000)


# almost the same as 'get_dns_activities' except this will write the search result to another place
def get_and_write_dns_activities(src_es, dst_es, start=None, end=None):
    indices = get_indices('event_', start, end)
    query_str = {"query": {"term": {"event_name": {"value": u"DNS查询"}}}}
    do_search_and_write(src_es, dst_es, indices, query_str, 6000)


def get_and_write_malicious_activities(src_es, dst_es, start=None, end=None):
    indices = get_indices('event_', start, end)
    query_str = {"query": {"terms": {"event_name": [u"入侵检测", u"木马", u"代码执行", u"钓鱼攻击", u"间谍软件", u"漏洞利用",
                                                    u"暴力破解", u"网络扫描", u"后门连接", u"可疑UA"]}}}
    do_search_and_write(src_es, dst_es, indices, query=query_str, step=8000)


# get the user and mac address mapping
# es - the elasticsearch connection
# start - the date that data will be collected start from, will from today if not specified
# end - the date that data will be collected end to, will be today if not specified
def get_user_login_activities(es, start=None, end=None):
    indices = get_indices('event_', start, end)
    query_str = {"query": {"terms": {"event_name": [u"账号登录", u"Radius认证"]}}}
    ret = do_search(es, indices, query=query_str, step=8000)[1]

    return ret


def get_dns_activities(es, start=None, end=None):
    indices = get_indices('event_', start, end)
    query_str = {"query": {"term": {"event_name": {"value": u"DNS查询"}}}}
    ret = do_search(es, indices, query=query_str, step=8000)[1]

    return ret


def get_malicious_activities(es, start=None, end=None):
    indices = get_indices('event_', start, end)
    query_str = {"query": {"terms": {"event_name": [u"入侵检测", u"木马", u"代码执行", u"钓鱼攻击", u"间谍软件", u"漏洞利用",
                                                    u"暴力破解", u"网络扫描", u"后门连接", u"可疑UA"]}}}
    ret = do_search(es, indices, query=query_str, step=8000)[1]

    return ret


def bulk_write_user_login_activities(es, body):
    index = 'login_' + datetime.date.today().strftime("%Y%m%d")
    ret = es.bulk(index=index, body=body)

    return ret


if __name__ == '__main__':
    parser = argparse.ArgumentParser(sys.argv[0])
    parser.add_argument("-s", type=str, action="append", dest="seeds",
                        help="Source Elasticsearch unicast hosts, specify with ip:port format is recommended")
    parser.add_argument("-d", type=str, action="append", dest="dests",
                        help="Dest Elasticsearch unicast hosts, specify with ip:port format is recommended")
    args = parser.parse_args()
    seeds = args.seeds
    dests = args.dests
    if seeds is None:
        logging.log(logging.ERROR, "The elasticsearch clustor is not specified. quit!")
        exit(0)

    logging.log(logging.INFO, 'Started, connected to seeds: ' + str(seeds))
    es_src = Elasticsearch(seeds, sniff_on_start=True, sniff_on_connection_fail=True, timeout=120)
    es_dst = None
    if dests is not None:
        es_dst = Elasticsearch(dests, sniff_on_start=True, sniff_on_connection_fail=True, timeout=120)
        logging.log(logging.INFO, 'Started, connected to dests: ' + str(dests))

    # Get all login & dns activities on 2018-03-23 to my es
    get_and_write_user_login_activities(es_src, es_dst)
    get_and_write_dns_activities(es_src, es_dst)
    get_and_write_malicious_activities(es_src, es_dst)
