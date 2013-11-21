import json

import dataset


def get_tables(query_plan):
    tables = query_plan.get('tables', [])
    for child in query_plan['children']:
        tables += get_tables(child)
    return tables


def print_stats(db):
    num_interesting_queries = list(db.query('SELECT COUNT(*) c FROM (SELECT DISTINCT query FROM logs WHERE db = "BestDR5")'))[0]['c']
    print "Distinct queries in BestDR5:", num_interesting_queries

    print
    print "Top 10 queries:"
    result = db.query('SELECT query, COUNT(*) c FROM logs WHERE db = "BestDR5" GROUP BY query ORDER BY c DESC LIMIT 10')
    for line in result:
        print '{:3}'.format(line['c']), line['query']

    print
    print "Error messages:"
    result = db.query('SELECT COUNT(*) c, error_msg FROM logs where error GROUP BY error_msg ORDER BY c DESC')
    for line in result:
        print '{:3}'.format(line['c']), line['error_msg']


def find_recurring(db):
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


def get_aggregated_cost(db, cost, query):
    result = db.query('SELECT SUM(cost) cost FROM (SELECT {query}, COUNT(*) count, AVG({cost}) cost FROM logs WHERE db = "BestDR5" AND plan != "" GROUP BY {query})'.format(cost=cost, query=query))
    return list(result)[0]['cost']


def get_cost(db, cost):
    result = db.query('SELECT SUM({cost}) cost FROM logs WHERE db = "BestDR5" AND plan != ""'.format(cost=cost))
    return list(result)[0]['cost']


def analyze(database):
    db = dataset.connect(database)

    print "Limited to DR5"
    print

    print_stats(db)

    # see whether we can analyze plans
    if 'plan' not in db['logs'].columns:
        "Run explain to see more!"
        return

    print

    num_interesting_queries = list(db.query('SELECT COUNT(*) c FROM (SELECT query FROM logs WHERE db = "BestDR5" AND plan != "")'))[0]['c']
    print "Distinct queries with query plan:", num_interesting_queries

    print "Overall cost assuming 1 (aka number of queries):", get_cost(db, '1')
    print "Overall real cost:", get_cost(db, 'elapsed')

    print

    print "Cost of 1, aggregate on query:", get_aggregated_cost(db, '1', 'query')
    print "Real cost, aggregate on query:", get_aggregated_cost(db, 'elapsed', 'query')
    print "Cost of 1, aggregate on plan:", get_aggregated_cost(db, '1', 'plan')
    print "Real cost, aggregate on plan:", get_aggregated_cost(db, 'elapsed', 'plan')
    print "(Average cost assumed per query)"
