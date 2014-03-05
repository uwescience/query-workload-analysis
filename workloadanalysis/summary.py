import dataset
from tabulate import tabulate
from utils import format_tabulate as ft


def summarize_sdss(db):
    datareleases = db.query('SELECT db, count(*) c, count(distinct query) dc FROM logs GROUP BY db ORDER BY c DESC')
    print "Number of queries per data release:"
    print tabulate(ft(datareleases), headers=['release', 'count', 'distinct'])


def summarize(database, sdss):
    db = dataset.connect(database)
    if sdss:
        summarize_sdss(db)
