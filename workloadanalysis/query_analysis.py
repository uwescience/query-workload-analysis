import json
import hashlib
from collections import Counter
from tabulate import tabulate
import dataset
import bz2
import sqltokens

from utils import format_tabulate as ft

# visitors
tables = lambda x: x.get('columns', {}).keys()
logical_ops = lambda x: [x['operator']]


def visit_operators(query_plan, visitor):
    l = visitor(query_plan)
    for child in query_plan['children']:
        l += visit_operators(child, visitor)
    return l


def hash_dict(d):
    keys = sorted(d.keys())
    flattened_values = [x for l in d.values() for x in l]
    return hash(frozenset(keys + flattened_values))


def get_hash(tree):
    c = hash_dict(tree['columns'])
    f = c + hash(frozenset(['filters']))
    h = hash(f + hash(tree['operator']))
    for child in tree['children']:
        h = hash(get_hash(child) + h)
    return h


def find_recurring(queries, estimated_cost):
    seen = {}

    # has to be list (mutable) so that we can modify it in sub-function
    cost_saved = [0]
    rows_cached = [0]

    def check_tree(tree):
        """Checks the plan for recurring subexpressions"""
        h = get_hash(tree)
        if h in seen:
            cost_saved[0] += tree['total']
        else:
            seen[h] = tree
            rows_cached[0] += tree['numRows']
            for child in tree['children']:
                check_tree(child)

    for query in queries:
        plan = json.loads(query['plan'])
        check_tree(plan)

    print "Saved cost", cost_saved, str(cost_saved[0] / estimated_cost * 100) + "%"
    print "Remaining cost", estimated_cost - cost_saved[0]
    print "Cached rows:", rows_cached[0]


def get_counts(queries, visitor, name):
    c = Counter()

    def count(plan):
        for o in visit_operators(plan, visitor):
            c[o] += 1

    for query in queries:
        plan = json.loads(query['plan'])
        count(plan)

    print tabulate(sorted(
        c.iteritems(),
        key=lambda t: t[1], reverse=True),
        headers=[name, "count"])


def explicit_implicit_joins(queries):
    explicit_join = 0
    implicit_join = 0
    for query in queries:
        plan = json.loads(query['plan'])
        log_ops = visit_operators(plan, logical_ops)
        has_join = len([x for x in log_ops if 'join' in x.lower()])
        if has_join and 'join' not in query['query'].lower():
            implicit_join += 1
        if 'join' in query['query'].lower():
            explicit_join += 1

    print 'Implicit join:', implicit_join
    print 'Explicit join:', explicit_join


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
    print tabulate(ft(result), headers=['count', 'error msg'])


def get_aggregated_cost(db, cost, query):
    result = db.query('SELECT SUM(cost) cost FROM (SELECT {query}, COUNT(*) count, AVG({cost}) cost FROM logs_dr5_explained GROUP BY {query})'.format(cost=cost, query=query))
    return list(result)[0]['cost']


def get_cost(db, cost):
    result = db.query('SELECT SUM({cost}) cost FROM logs_dr5_explained'.format(cost=cost))
    return list(result)[0]['cost']


def analyze_sdss(db, show_plots):
    print "Limited to DR5"
    print

    print_stats(db)

    # see whether we can analyze plans
    if 'plan' not in db['logs'].columns:
        "Run explain to see more!"
        return

    #db.query('DROP VIEW IF EXISTS logs_dr5_explained')
    exists = list(db.query('SELECT count(*) c FROM sqlite_master WHERE name="logs_dr5_explained"'))[0]['c'] > 0
    if not exists:
        db.query('CREATE VIEW logs_dr5_explained AS SELECT * FROM logs WHERE db = "BestDR5" AND plan != ""')

    print

    num_interesting_queries = list(db.query('SELECT COUNT(*) c FROM logs_dr5_explained'))[0]['c']
    print "Distinct queries with query plan:", num_interesting_queries

    print "Overall cost assuming 1 (aka number of queries):", get_cost(db, '1')
    print "Overall actual cost:", get_cost(db, 'elapsed')
    print "Overall estimated cost:", get_cost(db, 'estimated_cost')

    print

    print "Cost of 1, aggregate on query:", get_aggregated_cost(db, '1', 'query')
    print "Actual cost, aggregate on query:", get_aggregated_cost(db, 'elapsed', 'query')
    print "Estimated cost, aggregate on query:", get_aggregated_cost(db, 'estimated_cost', 'query')
    print "Cost of 1, aggregate on plan:", get_aggregated_cost(db, '1', 'simple_plan')
    print "Actual cost, aggregate on plan:", get_aggregated_cost(db, 'elapsed', 'simple_plan')
    print "Estimated cost, aggregate on plan:", get_aggregated_cost(db, 'estimated_cost', 'simple_plan')
    print "(Average cost assumed per query)"

    print
    print "Find recurring subtrees in distinct queries:"
    queries = db.query('SELECT *, COUNT(*) c FROM logs_dr5_explained GROUP BY query ORDER BY id ASC')
    estimated_cost = get_cost(db, 'estimated_cost')
    find_recurring(queries, estimated_cost)

    print
    queries = db.query('SELECT *, COUNT(*) c FROM logs_dr5_explained GROUP BY query ORDER BY id ASC')
    get_counts(queries, tables, 'table')

    print
    queries = db.query('SELECT *, COUNT(*) c FROM logs_dr5_explained GROUP BY query ORDER BY id ASC')
    get_counts(queries, logical_ops, 'logical op')

    print
    queries = db.query('SELECT *, COUNT(*) c FROM logs_dr5_explained GROUP BY query ORDER BY id ASC')
    explicit_implicit_joins(queries)

    if show_plots:
        import numpy as np
        #import scipy.stats
        import matplotlib.pyplot as plt
        import prettyplotlib as ppl
        print

        print "Correlation between estimated and actual cost"
        costs = list(db.query('SELECT elapsed actual, estimated_cost estimated, query query FROM logs WHERE db = "BestDR5" AND plan != ""'))
        actual = np.array([x['actual'] for x in costs], dtype=np.float)
        estimated = np.array([x['estimated'] for x in costs], dtype=np.float)
        queries = np.array([int(hashlib.md5(x['query']).hexdigest()[:10], 16) for x in costs], dtype=np.float)

        cost_hist = list(db.query('SELECT elapsed, COUNT(*) c FROM logs_dr5_explained GROUP BY elapsed ORDER BY elapsed'))

        colors = queries / np.max(queries)

        fig, axes = plt.subplots(2)

        # Correlation

        ax = axes[0]

        ppl.scatter(ax, estimated, actual, c=colors, s=60, alpha=0.6)

        ax.set_title("Correlation estimated and actual cost")
        ax.set_xlim(xmin=0)
        ax.set_ylim(ymin=0)
        ax.set_xlabel('Estimated')
        ax.set_ylabel('Actual')

        # Cost histogram

        ax = axes[1]

        val, weight = zip(*[(x['elapsed'], x['c']) for x in cost_hist])
        ppl.hist(ax, np.array(val), bins=10, weights=weight, grid='y')

        ax.set_title("Correlation estimated and actual cost")
        ax.set_xlabel('Elapsed time')
        ax.set_ylabel('Count')

        plt.show()


def analyze_sqlshare(db):
    queries = list(db.query('SELECT * from sqlshare_logs where has_plan = 1'))
    explicit_implicit_joins(queries)
    print 'Total queries with plan: ', len(queries)
    lengths = [] #
    compressed_lengths = [] #
    ops = [] #
    distinct_ops = [] #
    expanded_lengths = [] #
    expanded_ops = []  #
    expanded_distinct_ops = [] #
    compressed_expanded_lengths = [] #
    str_ops = [] #
    distinct_str_ops = [] #
    expanded_str_ops = [] #
    expanded_distinct_str_ops = [] #

    for q in queries:
        length = len(q['query'])
        lengths.append(length)
        compressed_lengths.append(len(bz2.compress(q['query'])))
        referenced_views = [x for x in q['ref_views'].split(',') if x != '']

        q_ex_ops = q['expanded_plan_ops'].split(',')
        expanded_ops.append(len(q_ex_ops))
        expanded_distinct_ops.append(len(set(q_ex_ops)))

        q_ops = q_ex_ops
        expanded_query = q['query']
        for ref_view in referenced_views:
            view = list(db.query('SELECT * from sqlshare_logs where id = {}'.format(ref_view)))
            expanded_query = expanded_query.replace(view[0]['view'], '(' + view[0]['query'] + ')')
            if view[0]['expanded_plan_ops']:
                for op in view[0]['expanded_plan_ops'].split(','):
                    if op in q_ops:
                        q_ops.remove(op)
            else:
                print 'No ops in view: ', view[0]['view']

        expanded_lengths.append(len(expanded_query))
        compressed_expanded_lengths.append(len(bz2.compress(expanded_query)))
        ops.append(len(q_ops))
        distinct_ops.append(len(set(q_ops)))

        if '--' in q['query']:
            str_ops.append(-1)
            distinct_str_ops.append(-1)
            expanded_str_ops.append(-1)
            expanded_distinct_str_ops.append(-1)
        else:
            tokens = sqltokens.get_tokens(q['query'])
            str_ops.append(len(tokens))
            distinct_str_ops.append(len(set(tokens)))
            ex_tokens = sqltokens.get_tokens(expanded_query)
            expanded_str_ops.append(len(ex_tokens))
            expanded_distinct_str_ops.append(len(set(ex_tokens)))

    print 'lengths = ', lengths
    print 'compressed_lengths = ', compressed_lengths
    print 'expanded_lengths = ', expanded_lengths
    print 'compressed_expanded_lengths = ', compressed_expanded_lengths

    print 'ops = ', ops
    print 'distinct_ops = ', distinct_ops
    print 'expanded_ops = ', expanded_ops
    print 'expanded_distinct_ops = ', expanded_distinct_ops

    print 'str_ops = ', str_ops
    print 'distinct_str_ops = ', distinct_str_ops
    print 'expanded_str_ops = ', expanded_str_ops
    print 'expanded_distinct_str_ops = ', expanded_distinct_str_ops

    ####SDSS
    exit()

    sdss_queries = db.query('SELECT query, COUNT(*) c FROM logs WHERE has_plan = 1 GROUP BY query ORDER BY c DESC')
    explicit_implicit_joins(sdss_queries)
    print 'Total queries with plan: ', len(sdss_queries)
    lengths = []  #
    compressed_lengths = []  #
    ops = []  #
    distinct_ops = []  #
    str_ops = []  #
    distinct_str_ops = []  #

    for q in queries:
        plan = json.loads(q['plan'])
        log_ops = visit_operators(plan, logical_ops)
        ops.append(len(log_ops))
        distinct_ops.append(len(set(log_ops)))

        tokens = sqltokens.get_tokens(q['query'])
        str_ops.append(len(tokens))
        distinct_str_ops.append(len(set(tokens)))

        lengths.append(len(q['query']))
        compressed_lengths.append(len(q['query']))

    print 'length = ', lengths
    print 'compressed_lengths = ', compressed_lengths

    print 'ops = ', ops
    print 'distinct_ops = ', distinct_ops

    print 'str_ops = ', str_ops
    print 'distinct_str_ops = ', distinct_str_ops


def analyze(database, show_plots, sdss):
    """Analyze the query log from the database
    """
    db = dataset.connect(database)

    if sdss:
        analyze_sdss(db, show_plots)
    else:
        analyze_sqlshare(db)
