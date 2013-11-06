import dataset


def print_stats(database):
    db = dataset.connect(database)

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
