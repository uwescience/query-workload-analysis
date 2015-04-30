import dataset
from tabulate import tabulate
from utils import format_tabulate as ft


def summarize_sdss(db):
    datareleases = db.query(
        'SELECT db, count(*) c, count(distinct query) dc, count(distinct query)*1.0/count(*) FROM logs GROUP BY db ORDER BY c DESC')
    print "Number of queries per data release:"
    print tabulate(ft(datareleases),
                   headers=['release', 'count', 'distinct', '% distinct'])


def summarize(database, sdss):
    db = dataset.connect(database)
    if sdss:
        summarize_sdss(db)
