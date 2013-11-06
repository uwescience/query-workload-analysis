"""Query explainer.

Usage:
  sdss_tools consume [DATABASE] -i INPUT ...
  sdss_tools explain CONFIG
  sdss_tools (-h | --help)
  sdss_tools --version

Options:
  -i INPUT     Logs to be read into database
  -h --help    Show this screen.
  --version    Show version.

"For a moment, nothing happened. Then, after a second or so,
nothing continued to happen."
"""
from docopt import docopt
import consume_logs
import explain_queries


def main():
    arguments = docopt(__doc__, version='SDSS Tools 0.0.1')

    if arguments['consume']:
        db = arguments['DATABASE'] or 'sqlite:///:memory:'
        consume_logs.consume(db, arguments['-i'])

    if arguments['explain']:
        config = {}
        with open(arguments['CONFIG']) as f:
            for line in f:
                key, val = line.split('=')
                config[key.strip()] = val.strip()
        explain_queries.explain(config)


if __name__ == '__main__':
    main()
