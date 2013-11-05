#!/usr/bin/env python

from datetime import date, timedelta
import glob
import re
import sys
import os

def doMain():
    from optparse import OptionParser

    parser = OptionParser(usage = 'usage: %prog [options]')
    parser.add_option('-w','--web',dest='query', action='store_const', const='web',help='web log')
    parser.add_option('-s','--sql',dest='query', action='store_const', const='sql',help='SQL log')

    (options, args) = parser.parse_args()

    if options.query != 'web' and options.query != 'sql':
        parser.error('query type is not given')

    # get the last one
    files = glob.glob('sdsslog_%s_*.log' % options.query)
    files.sort()

    m = re.search('sdsslog_%s_(?P<year>\d{4})\.(?P<month>\d{2})\.(?P<day>\d{2})\.log' % options.query,files[-1])
    if m is None:
        print >> sys.stderr, "No existing uch file?"
        sys.exit(-1)

    y = int(m.group('year'))
    mon = int(m.group('month'))
    d = int(m.group('day'))

    delta = timedelta(days=1)
    i = date(y,mon,d) + delta
    end = date(2010,1,1)

    queryType = '-w'
    if options.query == 'sql':
        queryType = '-s'

    while i < end:
        ret = os.system("python download.py %s -y %d -m %d -d %d" % ( queryType, i.year, i.month, i.day ) )
        print "%s: %04x" % ( i, ret )
        i += delta

if __name__ == '__main__':
    doMain()
