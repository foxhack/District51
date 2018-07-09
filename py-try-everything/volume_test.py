#!/usr/bin/python2
# _*_encoding:utf-8_*_

# import time
import threading

import time
import Queue
import collections
import json
import ujson
import re

######################################################
q = collections.deque()
t0 = time.clock()
for i in xrange(100000):
    q.append(1)
for i in xrange(100000):
    q.popleft()
print 'deque', time.clock() - t0

q = Queue.Queue(200000)
t0 = time.clock()
for i in xrange(100000):
    q.put(1)
for i in xrange(100000):
    q.get()
print 'Queue', time.clock() - t0

print 'json & ujson test'
config = '''{
  "kafka": {
    "host": "127.0.0.1",
    "port": 9092
  },
  "topic_groups": {
    "192.168.1.1": "dispatch-1",
    "192.168.1.2": "dispatch-2",
    "192.168.2.1": "dispatch-3",
    "111.202.116.81": "dispatch-4"
  },
  "default_topic": "dispatch-5",
  "socket": {
    "port": 514
  },
  "forward_queue_size": 100000,
  "receiver_threads": 2,
  "forwarder_threads":3,
  "log_path": "/opt/hansight/openresty/nginx/logs/access.log",
  "supervisord": {
      "supervisord_1": {
        "url": "http://127.0.0.1:9001/",
        "process": ["cep_slave_1", "cep_slave_2", "cep_slave_3", "cep_slave_4"]
      },
      "supervisord_2": {
        "url": "http://127.0.0.1:9001/",
        "process": ["cep_slave_5", "cep_slave_6", "cep_slave_7", "cep_slave_8", "cep_slave_9"]
      }
    }
}'''

t0 = time.clock()
for i in xrange(100000):
    obj = json.loads(config)
    #json.dumps(obj)
print 'json', time.clock() - t0

t0 = time.clock()
for i in xrange(100000):
    obj = ujson.loads(config)
    #ujson.dumps(obj)
print 'ujson', time.clock() - t0

print "Compare ujson & regular expression"
config = '''{"receive_time":1528350048259,"event_level":1,"occur_time":1515564325000,"sa_sp_ap_da_dp":"10.23.134.6____","src_geo":{"longitude":0.0,"latitude":0.0,"lat":0.0,"lon":0.0},"event_name":"检测到病毒","host_name":"WIN-TSEVOC3F301","dev_name":"virus wall","src_longitude_latitude_name":"0.0_0.0_内网","rule_id":"fbf03076-534d-403a-aae2-5ef525fb28b8","dev_city":"内网","original_log":"<148>Jan 11 07:01:49 TMCM:SLF_INCIDENT_EVT_VIRUS_FOUND_CLEAN_SUCCESS Security product=\"virus wall\" Security product node =\"WIN-TSEVOC3F301\" Security product IP=\"10.23.134.6\" Event time=\"2018/1/10 14:05:25\" Virus=\"Ransom_WCRY.SM2\" Action taken=\"clean\" Result=\"failed\" Infection destination=\"BJ-WIN7X32-256\" Infection destination IP=\"0.0.0.0\" Infection source=\"N/A\" Infection source IP=\"0.0.0.0\" Destination IP=\"0.0.0.0\" Source IP=\"0.0.0.0\"","response":"/clear","id":51061654034752,"src_city":"内网","virus_name":"Ransom_WCRY.SM2","event_type":"/M04RQKDW0006/XIH6IOIE0024","dev_geo":{"longitude":0.0,"latitude":0.0,"lat":0.0,"lon":0.0},"src_address":"10.23.134.6","sa_da":"10.23.134.6_","rule_name":"防病毒_趋势_防毒墙网络版_9","dev_longitude_latitude_name":"0.0_0.0_内网","dev_address":"172.16.101.82","vendor":"趋势","action":"failed","product":"防毒墙网络版"}'''
type_pattern = re.compile('"event_type":"(.*?)"')
print '''"src_address":"(\\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\b)"'''
ip_pattern = re.compile('''"src_address":"(\\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\b)"''')
print type_pattern.search(config).group()
print ip_pattern.search(config).group()
t0 = time.clock()
for i in xrange(100000):
    type_pattern.search(config)
    ip_pattern.search(config)
print 'regex', time.clock() - t0
#######################################################

class VolumeTest:
    def __init__(self):
        self._msg_cnt = 0

    def send_message(self):
        while True:
            self._msg_cnt += 1
            #time.sleep(0.01)

    @classmethod
    def counter(cls, inst):
        while True:
            print "%d messages been sent in past 5 seconds." % inst._msg_cnt
            time.sleep(2)

    def run(self):
        threads = []
        t1 = threading.Thread(target=self.send_message, name="Sender")
        threads.append(t1)
        t2 = threading.Thread(target=VolumeTest.counter, name="counter", kwargs={"inst": self})
        threads.append(t2)
        for t in threads:
            t.start()


if __name__ == "__main__":
    print "======================== Start ==========================="
    #vt = VolumeTest()
    #vt.run()

