#!/usr/bin/python2
# _*_encoding:utf-8_*_

import logging
import string
import random
import time
import os
import subprocess
import datetime
from elasticsearch2 import Elasticsearch
from elasticsearch2 import helpers

config = {
    "es_host": [
        "127.0.0.1:9200",
        "localhost:9200"
    ],
    "time_range": ("2018-03-29", "2018-03-30")
}

logging.basicConfig(filename="es-perf-eva.log", format='%(asctime)s%(levelname)-8s%(message)s',
                    datefmt="%Y-%m-%d %I:%M:%S%p ",
                    level="INFO")

if __name__ == "__main__":
    logging.info("==================== Start ====================")

    # random create a 4K string for write
    txt = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4096))
    file_1 = "/tmp/test1.dat"
    file_2 = "/diskb/test2.dat"
    fd1 = open(file_1, "w+")
    fd2 = open(file_2, "w+")

    logging.info("++++++++++++++++ Start testing the raw disk write performance ++++++++++++++++")

    logging.info("---------------- Sequential write START: Write the 4K data 50w times ----------------")
    start = time.time()
    for i in range(500000):
        fd1.write(txt)
    # flush with fsync to ensure the file been written to disk
    fd1.flush()
    os.fsync(fd1)
    logging.info("Sequential write spend " + str(time.time() - start) + "seconds.")
    logging.info("---------------- Sequential write END: Write the 4K data 50w times ----------------\n")

    logging.info("---------------- Random write START: Write the 4K data 50w times in 2 files ----------------")
    start = time.time()
    for i in range(500000):
        if i % 2 == 0:
            fd1.write(txt)
        else:
            fd2.write(txt)
    # flush with fsync to ensure the file been written to disk
    fd1.flush()
    os.fsync(fd1)
    fd2.flush()
    os.fsync(fd2)
    logging.info("Random write spend " + str(time.time() - start) + "seconds.")
    logging.info("---------------- Random write END: Write the 4K data 50w times in 2 files ----------------\n")

    logging.info(
        '---------------- Test the file cache impact START: Write performance with different dirty ratio settings '
        '----------------')
    p = subprocess.Popen(["cat", "/proc/sys/vm/dirty_ratio"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    orig_dirty_ratio = out.strip('\n')
    p = subprocess.Popen(["cat", "/proc/sys/vm/dirty_background_ratio"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    orig_dirty_background_ratio = out.strip('\n')
    # An the following test with set the "dirty_bytes & dirty_background" as they will be easier to control
    # In all tests of this section, close the background flush by set 'dirty_bytes & dirty_background_bytes' to same
    # scenario 1. decrease the cache will lower the performance, first set the dirty_bytes to 200MB,
    subprocess.call("echo 209715200 > /proc/sys/vm/dirty_bytes", shell=True)
    subprocess.call("echo 209715200 > /proc/sys/vm/dirty_background_bytes", shell=True)
    # check if successfully set
    ratio, err = subprocess.Popen(["cat", "/proc/sys/vm/dirty_ratio"], stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE).communicate()
    dirty_bytes, err = subprocess.Popen(["cat", "/proc/sys/vm/dirty_bytes"], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE).communicate()
    bkg_ratio, err = subprocess.Popen(["cat", "/proc/sys/vm/dirty_background_ratio"], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE).communicate()
    bkg_bytes, err = subprocess.Popen(["cat", "/proc/sys/vm/dirty_background_bytes"], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE).communicate()
    logging.info("SETTINGS: dirty_ratio=" + ratio.strip('\n') + ', dirty_bytes=' + dirty_bytes.strip('\n') +
                 ', bkg_ratio=' + bkg_ratio.strip('\n') + ', bkg_bytes=' + bkg_bytes.strip('\n'))
    start = time.time()
    check_point = start
    time_write_10w = []
    for i in range(3000000):
        fd2.write(txt)
        if (i % 100000) == 0:
            now = time.time()
            time_write_10w.append(now - check_point)
            check_point = now
    fd2.flush()
    os.fsync(fd2)
    logging.info("Write spend " + str(time.time() - start) + " seconds with dirty_bytes=200MB.")
    logging.info("Write spends " + str(time_write_10w) +
                 " seconds for every 10w write actions with dirty_bytes=200MB.\n")

    # then set the dirty_bytes to 4GB
    subprocess.call("echo 4294967296 > /proc/sys/vm/dirty_bytes", shell=True)
    subprocess.call("echo 4294967296 > /proc/sys/vm/dirty_background_bytes", shell=True)
    # check if successfully set
    ratio, err = subprocess.Popen(["cat", "/proc/sys/vm/dirty_ratio"], stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE).communicate()
    dirty_bytes, err = subprocess.Popen(["cat", "/proc/sys/vm/dirty_bytes"], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE).communicate()
    bkg_ratio, err = subprocess.Popen(["cat", "/proc/sys/vm/dirty_background_ratio"], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE).communicate()
    bkg_bytes, err = subprocess.Popen(["cat", "/proc/sys/vm/dirty_background_bytes"], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE).communicate()
    logging.info("SETTINGS: dirty_ratio=" + ratio.strip('\n') + ', dirty_bytes=' + dirty_bytes.strip('\n') +
                 ', bkg_ratio=' + bkg_ratio.strip('\n') + ', bkg_bytes=' + bkg_bytes.strip('\n'))
    start = time.time()
    check_point = start
    time_write_10w = []
    # write 12G data
    for i in range(3000000):
        fd2.write(txt)
        if (i % 100000) == 0:
            now = time.time()
            time_write_10w.append(now - check_point)
            check_point = now
    fd2.flush()
    os.fsync(fd2)
    logging.info("Write spend " + str(time.time() - start) + "seconds with dirty_bytes=4GB.")
    logging.info("Write spends " + str(time_write_10w) + " seconds for every 10w write actions with dirty_bytes=4GB.")

    # Test complete, recover the original system settings
    p = subprocess.call("echo " + orig_dirty_ratio + " > /proc/sys/vm/dirty_ratio", shell=True)
    p = subprocess.call("echo " + orig_dirty_background_ratio + " > /proc/sys/vm/dirty_background_ratio", shell=True)
    logging.info(
        '---------------- Test the file cache impact END: Write performance with different dirty ratio settings '
        '----------------\n')

    logging.info("++++++++++++++++ Complete testing the raw disk write performance ++++++++++++++++\n")
    fd1.close()
    fd2.close()
    os.remove(file_1)
    os.remove(file_2)

    # to add more here...

    logging.info("==================== End ====================\n\n")
