import json

import dataset


def get_tables(query_plan):
    tables = query_plan.get('tables', [])
    for child in query_plan['children']:
        tables += get_tables(child)
    return tables


def analyze(database):
    db = dataset.connect(database)
    queries = db.query('SELECT *, COUNT(*) c FROM logs WHERE db = "BestDR5" AND plan != "" GROUP BY query ORDER BY id ASC')

    seen = set()

    count_new = 0
    count_recurring = 0

    for query in queries:
        plan = json.loads(query['plan'])
        tables = set(get_tables(plan[0]))
        new = tables - seen
        count_new += len(new)
        count_recurring += len(seen) - len(new)
        seen |= new

    print "New", count_new
    print "Recurring", count_recurring
