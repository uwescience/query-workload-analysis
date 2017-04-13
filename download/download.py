#!/usr/bin/env python

import os
import os.path
import urllib


# formulate query string
WGET='/usr/local/bin/wget'
URL='http://skyserver.sdss.org/log/en/traffic/x_sql.asp'
QUERY_TEMPLATE={ 'sql': 'SELECT * FROM SqlLog WHERE yy=%d', 'web': 'SELECT * FROM WebLog WHERE yy=%d' }
POSTDATA = { 'format': 'csv' }

def doMain():
    from optparse import OptionParser

    parser = OptionParser(usage = 'usage: %prog [options]')
    parser.add_option('-y','--year',dest='year',type='int',default=None,help='year to retrieve')
    parser.add_option('-m','--month',dest='month',type='int',default=None,help='month to retrieve')
    parser.add_option('-d','--day',dest='day',type='int',default=None,help='day to retrieve')
    parser.add_option('-o','--output',dest='outdir',default='.',help='output directory [default: %default]')
    parser.add_option('-w','--web',dest='query', action='store_const', const='web',help='web log')
    parser.add_option('-s','--sql',dest='query', action='store_const', const='sql',help='SQL log')

    (options, args) = parser.parse_args()

    if options.year is None:
        parser.error('all year, month, and day must be specified')

    if options.outdir is not None and not os.path.exists(options.outdir):
        os.makedirs(options.outdir)

    if options.query not in QUERY_TEMPLATE:
        parser.error('query type is not given')

    outfn = "%s/sdsslog_%s_%d.%02s.%02s.log" % ( options.outdir, options.query, options.year, 'XX', 'XX' )

    # build post data
    # POSTDATA['cmd'] = QUERY_TEMPLATE[options.query] % ( options.year, options.month, options.day )
    POSTDATA['cmd'] = QUERY_TEMPLATE[options.query] % ( options.year )

    query = urllib.urlencode(POSTDATA)

    # build execvp for wget

    ARGS = [ WGET,
            '--post-data', query,
            '-O', outfn,
            URL ]

    os.execv(WGET,ARGS)


if __name__ == '__main__':
    doMain()
