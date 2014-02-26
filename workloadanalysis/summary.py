import dataset
from tabulate import tabulate


def summarize_sdss(db):
    datareleases = db.query('SELECT db, count(*), count(distinct query) c FROM logs GROUP BY db ORDER BY c DESC')
    print "Number of queries per data release:"
    print tabulate(datareleases, headers=['release', 'count', 'distinct'])


def summarize(database, sdss):
    db = dataset.connect(database)
    if sdss:
        summarize_sdss(db)
