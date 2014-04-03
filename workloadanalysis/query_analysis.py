import json
from collections import Counter
from tabulate import tabulate
import dataset
import bz2
import sqltokens
import csv

from utils import format_tabulate as ft

# table with all queries from DR5
ALL = 'logs'

# table that contains distinct queries from ALL
DISTINCT = 'distinctlogs'

# the queries that have been explained
EXPLAINED = 'explained'


# visitors
visitor_tables = lambda x: x.get('columns', {}).keys()
visitor_logical_ops = lambda x: [x['operator']]
visitor_physical_ops = lambda x: [x['physicalOp']]


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


def print_table(data, headers):
    print tabulate(data, headers, tablefmt='latex')

    with open('results/' + '_'.join(headers).replace(' ', '_') + '.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)


def get_counts(queries, visitors, names):
    '''Count something which requires visiting all operators'''

    assert len(visitors) == len(names)

    c = [Counter() for _ in visitors]

    def count(plan):
        for i, visitor in enumerate(visitors):
            c[i].update(visit_operators(plan, visitor))

    for query in queries:
        plan = json.loads(query['plan'])
        count(plan)

    for r, name in zip(c, names):
        print
        print_table(sorted(
            r.iteritems(),
            key=lambda t: t[1], reverse=True),
            headers=[name, "count"])


def explicit_implicit_joins(queries):
    explicit_join = 0
    implicit_join = 0
    for query in queries:
        plan = json.loads(query['plan'])
        log_ops = visit_operators(plan, visitor_logical_ops)
        has_join = len([x for x in log_ops if 'join' in x.lower()])
        if has_join and 'join' not in query['query'].lower():
            implicit_join += 1
        if 'join' in query['query'].lower():
            explicit_join += 1

    print '#Implicit join:', implicit_join
    print '#Explicit join:', explicit_join


def print_stats(db):
    num_interesting_queries = list(db.query('SELECT COUNT(*) c FROM {}'.format(DISTINCT)))[0]['c']
    print "Distinct queries in BestDR5:", num_interesting_queries

    print
    print "Top 10 queries:"
    result = db.query('SELECT query, COUNT(*) AS c FROM {} GROUP BY query ORDER BY c DESC LIMIT 10'.format(ALL))
    for line in result:
        print '{:3}'.format(line['c']), line['query']

    print
    print "Error messages:"
    result = db.query('SELECT COUNT(*) AS c, error_msg FROM {} where error GROUP BY error_msg ORDER BY c DESC'.format(ALL))
    print_table(ft(result), headers=['count', 'error msg'])


def get_aggregated_cost(db, cost, query):
    result = db.query('SELECT SUM(cost) AS cost FROM (SELECT {query}, COUNT(*) count, AVG({cost}) AS cost FROM {table} GROUP BY {query}) AS tbl'.format(cost=cost, query=query, table=EXPLAINED))
    return list(result)[0]['cost']


def get_cost(db, cost):
    result = db.query('SELECT SUM({cost}) AS cost FROM {table}'.format(cost=cost, table=EXPLAINED))
    return list(result)[0]['cost']


def analyze_sdss(db):
    print "Limited to DR5"
    print

    print_stats(db)

    print

    num_interesting_queries = list(db.query('SELECT COUNT(*) c FROM {}'.format(EXPLAINED)))[0]['c']
    print "Distinct queries with query plan:", num_interesting_queries

    # TODO: This does not seem to be interesting
    """
    print "Overall cost assuming 1 (aka number of queries):", get_cost(db, '1')
    print "Overall actual cost:", get_cost(db, 'elapsed')
    print "Overall estimated cost:", get_cost(db, 'estimated_cost')

    print

    print "Cost of 1, aggregate on query:", get_aggregated_cost(db, '1', 'query')
    print "Actual cost, aggregate on query:", get_aggregated_cost(db, 'elapsed', 'query')
    print "Estimated cost, aggregate on query:", get_aggregated_cost(db, 'estimated_cost', 'query')
    print "Cost of 1, aggregate on plan:", get_aggregated_cost(db, '1', 'plan')
    print "Actual cost, aggregate on plan:", get_aggregated_cost(db, 'elapsed', 'plan')
    print "Estimated cost, aggregate on plan:", get_aggregated_cost(db, 'estimated_cost', 'plan')
    print "(Average cost assumed per query)"
    """

    expl_queries = '''
        SELECT query, plan, time_start, elapsed
        FROM {}
        ORDER BY time_start ASC'''.format(EXPLAINED)

    print
    print "Find recurring subtrees in distinct queries:"
    queries = db.query(expl_queries)
    estimated_cost = get_cost(db, 'estimated_cost')
    find_recurring(queries, estimated_cost)

    print
    queries = db.query(expl_queries)
    get_counts(queries,
               [visitor_tables, visitor_logical_ops, visitor_physical_ops],
               ['table', 'logical op', 'physical op'])

    print
    queries = db.query(expl_queries)
    explicit_implicit_joins(queries)

    queries = db.query(expl_queries)

    # counters for how often we have a certain count in a query
    lengths = Counter()
    compressed_lengths = Counter()
    ops = Counter()
    distinct_ops = Counter()
    str_ops = Counter()
    distinct_str_ops = Counter()
    touch = Counter()

    which_str_ops = Counter()

    for q in queries:
        plan = json.loads(q['plan'])
        log_ops = visit_operators(plan, visitor_logical_ops)
        tables = visit_operators(plan, visitor_tables)
        ops[len(log_ops)] += 1
        touch[len(tables)] += 1
        distinct_ops[len(set(log_ops))] += 1

        query = q['query']

        lengths[len(query)] += 1
        compressed_lengths[len(bz2.compress(query))] += 1

        # tokenization is horribly slow and does not work for sdss
        continue

        tokens = sqltokens.get_tokens(query)
        str_ops[len(tokens)] += 1
        distinct_str_ops[len(set(tokens))] += 1

        which_str_ops.update(tokens)

    print
    print_table(sorted(
        which_str_ops.iteritems(),
        key=lambda t: t[1], reverse=True),
        headers=["string op", "count"])

    for name, values in zip(
        ['lengths', 'compressed lengths', 'ops', 'distinct ops', 'string ops', 'distinct string ops', 'touch'],
        [lengths, compressed_lengths, ops, distinct_ops, str_ops, distinct_str_ops, touch]):
        print
        print_table(sorted(
            values.iteritems(),
            key=lambda t: t[0]),
            headers=[name, "counts"])


def analyze_sqlshare(db, write_to_file = False):
    if write_to_file:
        f = open('ProcessedQueries_Sqlshare.csv', 'w')
        f.write('Source|owner|Query|starttime|duration|length|compressed_length|expanded_length|compressed_expanded_lengths|ops|distinct_ops|expanded_ops|expanded_distinct_ops|keywords|distinct_keywords|expanded_keywords|expanded_distinct_keywords|Touch\n')
    queries = list(db.query('SELECT * from sqlshare_logs where has_plan = 1'))
    views = list(db.query('SELECT * FROM sqlshare_logs WHERE isView = 1'))
    explicit_implicit_joins(queries)
    print '#Total queries with plan: ', len(queries)
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
    touch = [] #

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
        while(True):
            previousLength = len(expanded_query)
            for view in views:
                if view['view'] in q['query']:
                    expanded_query = expanded_query.replace(view['view'], '(' + view['query'] + ')')
            if (len(expanded_query) == previousLength):
                break

        for ref_view in referenced_views:
            view = list(db.query('SELECT * from sqlshare_logs where id = {}'.format(ref_view)))
            if view[0]['expanded_plan_ops']:
                for op in view[0]['expanded_plan_ops'].split(','):
                    if op in q_ops:
                        q_ops.remove(op)
            #else:
                #print 'No ops in view: ', view[0]['view']

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

        plan = json.loads(q['plan'])
        tables = visit_operators(plan, visitor_tables)
        touch.append(len(tables))

        #'Source|owner|Query|starttime|duration|length|compressed_length|expanded_length|compressed_expanded_lengths|
        #ops|distinct_ops|expanded_ops|expanded_distinct_ops|keywords|distinct_keywords|expanded_keywords|expanded_distinct_keywords|Touch\n'
        if write_to_file:
            f.write('%s|%s|%s|%s|%s|%f|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d\n'%
                ('sqlshare',q['owner'], q['query'], q['time_start'],q['runtime'],lengths[-1], compressed_lengths[-1], expanded_lengths[-1], compressed_expanded_lengths[-1],
                    ops[-1],distinct_ops[-1],expanded_ops[-1],expanded_distinct_ops[-1],str_ops[-1],distinct_str_ops[-1],expanded_str_ops[-1],expanded_distinct_str_ops[-1],touch[-1]))


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
    print 'touch = ', touch
    if write_to_file:
        f.close()


def analyze(database, sdss):
    """Analyze the query log from the database
    """
    db = dataset.connect(database)

    if sdss:
        analyze_sdss(db)
    else:
        analyze_sqlshare(db, write_to_file=False)
