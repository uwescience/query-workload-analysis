"""Query workload analysis.

Usage:
  qwla (sdss|sqlshare) consume INPUT... [-d DATABASE]
  qwla (sdss|sqlshare) summarize [-d DATABASE]
  qwla (sdss|sqlshare) explain CONFIG -q [-d DATABASE]
  qwla (sdss|sqlshare) analyze [--plots] [-d DATABASE]
  qwla (-h | --help)
  qwla --version

Options:
  INPUT...       Log files to be read into database
  -d [DATABASE]  The database to read from or write into
  CONFIG         How to connect to SQLServer
  --plots        Show plots
  -q             Don't print results to stdout
  -h --help      Show this screen.
  --version      Show version.

"For a moment, nothing happened. Then, after a second or so,
nothing continued to happen."
"""
from docopt import docopt
import consume_logs
import explain_queries
import query_analysis
import summary


def main():
    arguments = docopt(__doc__, version='SDSS Tools 0.0.1')

    db = (('DATABASE' in arguments and arguments['DATABASE'])
          or 'sqlite:///test.sqlite')

    if arguments['consume']:
        consume_logs.consume(db, arguments['INPUT'], arguments['sdss'])

    if arguments['summarize']:
        summary.summarize(db, arguments['sdss'])

    if arguments['explain']:
        config = {}
        with open(arguments['CONFIG']) as f:
            for line in f:
                key, val = line.split('=')
                config[key.strip()] = val.strip()
        explain_queries.explain(config, db, arguments['-q'], arguments['sdss'])

    if arguments['analyze']:
        query_analysis.analyze(db, arguments['--plots'], arguments['sdss'])


if __name__ == '__main__':
    main()
