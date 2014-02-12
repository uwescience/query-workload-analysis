"""Query workload analysis.

Usage:
  qwla (sdss|sqlshare) consume [DATABASE] -i INPUT ...
  qwla (sdss|sqlshare) explain CONFIG [DATABASE]
  qwla (sdss|sqlshare) analyze [DATABASE] [--plots]
  qwla (-h | --help)
  qwla --version

Options:
  -i INPUT     Logs to be read into database
  --plots      Show plots
  -h --help    Show this screen.
  --version    Show version.

"For a moment, nothing happened. Then, after a second or so,
nothing continued to happen."
"""
from docopt import docopt
import consume_logs
import explain_queries
import query_analysis


def main():
    arguments = docopt(__doc__, version='SDSS Tools 0.0.1')

    db = arguments['DATABASE'] or 'sqlite:///test.sqlite'

    if arguments['consume']:
        consume_logs.consume(db, arguments['-i'], arguments['sdss'])

    if arguments['explain']:
        config = {}
        with open(arguments['CONFIG']) as f:
            for line in f:
                key, val = line.split('=')
                config[key.strip()] = val.strip()
        explain_queries.explain(config, db, arguments['sdss'])

    if arguments['analyze']:
        query_analysis.analyze(db, arguments['--plots'], arguments['sdss'])


if __name__ == '__main__':
    main()
