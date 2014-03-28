#!/usr/bin/env python

from datetime import date, timedelta
import os


def doMain():
    from optparse import OptionParser

    parser = OptionParser(usage = 'usage: %prog [options]')
    parser.add_option('-w','--web',dest='query', action='store_const', const='web',help='web log')
    parser.add_option('-s','--sql',dest='query', action='store_const', const='sql',help='SQL log')
    parser.add_option('-b','--begin',dest='begin', help='when to begin as yyyy-mm-dd')
    parser.add_option('-e','--end',dest='end', help='when to stop as yyyy-mm-dd')

    options, args = parser.parse_args()

    if options.query != 'web' and options.query != 'sql':
        parser.error('query type is not given')

    dateparts = options.begin.split('-')
    y = int(dateparts[0])
    mon = int(dateparts[1])
    d = int(dateparts[2])

    delta = timedelta(days=1)
    i = date(y, mon, d)

    dateparts = options.end.split('-')
    y = int(dateparts[0])
    mon = int(dateparts[1])
    d = int(dateparts[2])
    end = date(y, mon, d)

    queryType = '-w'
    if options.query == 'sql':
        queryType = '-s'

    while i < end:
        ret = os.system("python download.py %s -y %d -m %d -d %d" % (queryType, i.year, i.month, i.day))
        print "%s: %04x" % (i, ret)
        i += delta

if __name__ == '__main__':
    doMain()
