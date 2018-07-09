#!/usr/bin/python2
# _*_encoding:utf-8_*_

import datetime
from ib_importer import get_indices


def print_keyword_args(**kwargs):
    print kwargs
    for key, value in kwargs.iteritems():
        print "%s = %s" % (key, value)


if __name__ == "__main__":
    print "==================== Start ===================="
    print datetime.datetime.strptime("2018123", "%Y%m%d").strftime("%Y%m%d")
    print datetime.datetime.fromtimestamp(1522161437403 / 1000).strftime("%Y%m%d")
    print get_indices("event_", datetime.date.today() + datetime.timedelta(-15), datetime.date.today())
    print '\n'

    # Test the date & time
    start_date = datetime.datetime.fromtimestamp(1519718649048 / 1000)
    end_date = datetime.datetime.fromtimestamp(1522161437403 / 1000)
    print "Start from " + start_date.strftime("%Y%m%d") + ", end to " + end_date.strftime("%Y%m%d")
    print datetime.date.today()
    print datetime.datetime.today()
    print '\n'

    # Test indices generation
    indices = [("event_" + (start_date + datetime.timedelta(x)).strftime("%Y%m%d"))
               for x in range(0, (end_date - start_date).days + 1)]
    print indices

    print_keyword_args(u1="red", u2="green", u3="yellow")

    # 嵌套的生成式
    dic = {str(x): [10*x+y for y in range(10)] for x in range(10)}
    print dic
    print "Test if can del the list items across the boarder:"
    print "Before delete:"
    print dic["0"]
    del dic["0"][:len(dic["0"])]
    print "After delete:"
    print dic["0"]


    print "==================== End ====================\n"
