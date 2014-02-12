"""Query explainer.

Usage:
  sdss_tools consume [DATABASE] -i INPUT ...
  sdss_tools explain CONFIG [DATABASE]
  sdss_tools analyze [DATABASE] [--plots]
  sdss_tools (-h | --help)
  sdss_tools --version

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

    if arguments['consume']:
        db = arguments['DATABASE'] or 'sqlite:///test.sqlite'
        consume_logs.consume(db, arguments['-i'])

    if arguments['explain']:
        config = {}
        with open(arguments['CONFIG']) as f:
            for line in f:
                key, val = line.split('=')
                config[key.strip()] = val.strip()
        db = arguments['DATABASE'] or 'sqlite:///test.sqlite'
        explain_queries.explain(config, db)

    if arguments['analyze']:
        db = arguments['DATABASE'] or 'sqlite:///test.sqlite'
        query_analysis.analyze(db, arguments['--plots'])


if __name__ == '__main__':
    main()
