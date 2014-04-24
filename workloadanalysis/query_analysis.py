import json
from collections import Counter, defaultdict
from tabulate import tabulate
import dataset
import bz2
import sqltokens
import csv
import hashlib

from utils import format_tabulate as ft

from pprint import pprint

# table with all queries from DR5
ALL = 'logs'

# table that contains distinct queries from ALL
DISTINCT = 'distinctlogs'

# the queries that have been explained
EXPLAINED = 'explained'

# the queries that don't have the same query plan
UNIQUE = 'uniqueplans'


# visitors
visitor_tables = lambda x: x.get('columns', {}).keys()
visitor_logical_ops = lambda x: [x['operator']]
visitor_physical_ops = lambda x: [x['physicalOp']]


def visit_operators(query_plan, visitor):
    l = visitor(query_plan)
    for child in query_plan['children']:
        l += visit_operators(child, visitor)
    return l


def hashable(d):
    if isinstance(d, set):
        d = list(d)
    if isinstance(d, frozenset):
        d = list(d)
    return json.dumps(d, sort_keys=True)


def get_hash(tree):
    f = hashlib.md5()
    f.update(hashable(tree['columns']))
    f.update(hashable(tree['filters']))
    f.update(tree['operator'])
    [f.update(get_hash(child)) for child in sorted(tree['children'], key=lambda x: x['operator'])]
    return f.hexdigest()


def print_op_tree(tree, indent=0):
    print ' ' * indent, tree['operator']
    for child in sorted(tree['children'], key=lambda x: x['operator']):
        print_op_tree(child, indent + 1)


def find_recurring(queries):
    seen = {}

    # has to be list (mutable) so that we can modify it in sub-function
    cost_saved = [0]
    rows_cached = [0]
    cost = 0
    saved = [0]  # for each query
    savings = Counter()

    def check_tree(tree):
        """Checks the plan for recurring subexpressions"""
        h = get_hash(tree)
        if h in seen:
            cost_saved[0] += tree['total']
            saved[0] += tree['total']
        else:
            seen[h] = tree
            rows_cached[0] += tree['numRows']
            for child in tree['children']:
                check_tree(child)

    for query in queries:
        saved = [0]
        plan = json.loads(query['plan'])
        #print_op_tree(plan)
        cost += plan['total']
        check_tree(plan)
        savings[saved[0]/plan['total']] += 1

    print "Saved cost", cost_saved, str(cost_saved[0] / cost * 100) + "%"
    print "Remaining cost", cost - cost_saved[0]
    print "Cached rows:", rows_cached[0]

    print_table(sorted(
                savings.iteritems(),
                key=lambda t: t[0]), ["savings", "count"])


def transformed(tree):
    """ Transforms the plan to to have sets for faster comparisons """
    t = {}
    cols = set()
    for name, table in tree['columns'].iteritems():
        for column in table:
            cols.add(name + '.' + column)
    filters = frozenset(map(hashable, tree['filters']))
    t['columns'] = frozenset(cols)
    t['filters'] = filters
    t['operator'] = tree['operator']
    t['total'] = tree['total']
    children = []
    for child in tree['children']:
        children.append(transformed(child))
    t['children'] = children
    return t


comparator = lambda x: x['operator'] + str(hash(x['columns']))


def check_one_child(tree, match):
    if tree['operator'] != match['operator']:
        return False
    if len(tree['children']) != len(match['children']):
        return False
    if not match['filters'].issubset(tree['filters']):
        return False
    if not match['columns'].issuperset(tree['columns']):
        return False
    for c, mc in zip(sorted(tree['children'], key=comparator), sorted(match['children'], key=comparator)):
        if not check_one_child(c, mc):
            return False
    return True


def check_child_matches(tree, matches):
    for match in matches:
        if check_one_child(tree, match):
            return match
    return False


def find_recurring_subset(queries):
    columns = defaultdict(set)
    seen = {}
    cost = 0
    cost_saved = [0]
    usefulness = Counter()  # how often a subtree is used

    def add_to_index(tree):
        h = get_hash(tree)
        seen[h] = tree
        for col in tree['columns']:
            columns[col].add(h)

    def have_seen(tree):
        tablematches = None
        for column in tree['columns']:
            c = columns[column]
            if tablematches is None:
                tablematches = c
            else:
                tablematches = tablematches.intersection(c)
        if not tablematches:
            return False
        matches = [seen[h] for h in tablematches]
        return check_child_matches(tree, matches)

    def check_tree(tree, level=0):
        m = have_seen(tree)
        if m:
            cost_saved[0] += tree['total']
            #pprint(m)
            #pprint(tree)
            #print "have seen", level
            usefulness[get_hash(m)] += 1
        else:
            add_to_index(tree)
            for child in tree['children']:
                check_tree(child, level+1)

    for i, query in enumerate(queries):
        plan = transformed(json.loads(query['plan']))
        #pprint(plan)
        cost += plan['total']
        check_tree(plan)
        if not i % 1000:
            print "Looked for reuse in", i

    for h, c in usefulness.most_common(5):
        print c, seen[h]

    print "Saved cost", cost_saved, str(cost_saved[0] / cost * 100) + "%"
    print "Remaining cost", cost - cost_saved[0]


def print_table(data, headers, sdss=True):
    print tabulate(data, headers, tablefmt='latex')
    subfolder = 'sdss' if sdss else 'sqlshare'
    with open('results/' + subfolder + '/' + '_'.join(headers).replace(' ', '_') + '.csv', 'w') as f:
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
        SELECT query, plan, time_start, elapsed, estimated_cost
        FROM {}
        WHERE estimated_cost < 100
        ORDER BY time_start ASC'''.format(EXPLAINED)

    dist_queries = '''
        SELECT query, plan, elapsed, estimated_cost
        FROM {}
        WHERE estimated_cost < 100
        ORDER BY time_start ASC'''.format(UNIQUE)

    print
    print "Find recurring subtrees in distinct queries:"
    queries = db.query(expl_queries)
    find_recurring(queries)

    print
    print "Find recurring subtrees in distinct queries (distinct by plan):"
    queries = db.query(dist_queries)
    find_recurring_subset(queries)

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
    logops = Counter()
    physops = Counter()
    distinct_ops = Counter()
    str_ops = Counter()
    distinct_str_ops = Counter()
    touch = Counter()
    estimated = Counter()
    actual = Counter()

    which_str_ops = Counter()

    for q in queries:
        plan = json.loads(q['plan'])
        log_ops = visit_operators(plan, visitor_logical_ops)
        phys_ops = visit_operators(plan, visitor_physical_ops)
        tables = visit_operators(plan, visitor_tables)
        logops[len(log_ops)] += 1
        physops[len(phys_ops)] += 1
        touch[len(tables)] += 1
        distinct_ops[len(set(log_ops))] += 1

        query = q['query']

        lengths[len(query)] += 1
        compressed_lengths[len(bz2.compress(query))] += 1

        estimated[q['estimated_cost']] += 1
        actual[q['elapsed']] += 1

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
        ['lengths', 'compressed lengths', 'logops', 'physops', 'distinct ops', 'string ops', 'distinct string ops', 'touch', 'estimated', 'actual'],
        [lengths, compressed_lengths, logops, physops, distinct_ops, str_ops, distinct_str_ops, touch, estimated, actual]):
        print
        print_table(sorted(
            values.iteritems(),
            key=lambda t: t[0]),
            headers=[name, "counts"])

def increment_element_count(element, dict_obj):
    if element in dict_obj:
        dict_obj[element] += 1
    else:
        dict_obj[element] = 1

def write_to_csv(dict_obj, col1, col2, filename):
    f = open(filename, 'w')
    f.write("%s,%s\n"%(col1,col2))
    for key in dict_obj:
        f.write("%d,%d\n"%(key, dict_obj[key]))
    f.close()


def analyze_sqlshare(db, write_to_file = False):
    if write_to_file:
        f = open('ProcessedQueries_Sqlshare.csv', 'w')
        f.write('Source|owner|Query|starttime|duration|length|comp_length|exp_length|comp_exp_lengths|ops|distinct_ops|exp_ops|exp_distinct_ops|keywords|distinct_keywords|expanded_keywords|exp_distinct_keywords|Touch\n')
    queries = list(db.query('SELECT * from sqlshare_logs where has_plan = 1'))
    views = list(db.query('SELECT * FROM sqlshare_logs WHERE isView = 1'))
    explicit_implicit_joins(queries)
    print '#Total queries with plan: ', len(queries)
    lengths = {}
    comp_lengths = {}
    ops = {}
    distinct_ops = {}
    physical_ops = {}
    distinct_physical_ops = {}
    exp_lengths = {}
    exp_ops = {}
    exp_distinct_ops = {}
    exp_physical_ops = {}
    exp_distinct_physical_ops = {}
    comp_exp_lengths = {}
    str_ops = {}
    distinct_str_ops = {}
    exp_str_ops = {}
    exp_distinct_str_ops = {}
    touch = {}
    time_taken = {}
    dataset_touch = {}

    for q in queries:
        length = len(q['query'])
        increment_element_count(length, lengths)
        comp_length = len(bz2.compress(q['query']))
        increment_element_count(comp_length, comp_lengths)
        if q['isView'] == 0:
            increment_element_count(q['runtime'], time_taken)

        expanded_query = q['query']
        # calculating dataset touch and expanded query now.
        ref_views = {}
        while(True):
            previousLength = len(expanded_query)
            prev_ref_views = ref_views
            ref_views = {}
            for view in views:
                if view['view'] in expanded_query:
                    ref_views[view['view']] = view['query']
            for v in ref_views:
                expanded_query = expanded_query.replace(v, '(' + ref_views[v] + ')')

            if (len(expanded_query) == previousLength):
                increment_element_count(len(prev_ref_views), dataset_touch)
                break

        q_ex_ops = q['expanded_plan_ops_logical'].split(',')
        increment_element_count(len(q_ex_ops), exp_ops)
        increment_element_count(len(set(q_ex_ops)), exp_distinct_ops)

        q_ex_phy_ops = q['expanded_plan_ops'].split(',')
        increment_element_count(len(q_ex_phy_ops), exp_physical_ops)
        increment_element_count(len(set(q_ex_phy_ops)), exp_distinct_physical_ops)

        q_ops = q_ex_ops
        q_phy_ops = q_ex_phy_ops
        referenced_views = [x for x in q['ref_views'].split(',') if x != '']
        for ref_view in referenced_views:
            view = list(db.query('SELECT * from sqlshare_logs where id = {}'.format(ref_view)))
            if view[0]['expanded_plan_ops_logical']:
                for op in view[0]['expanded_plan_ops_logical'].split(','):
                    if op in q_ops:
                        q_ops.remove(op)
            if view[0]['expanded_plan_ops']:
                for op in view[0]['expanded_plan_ops'].split(','):
                    if op in q_phy_ops:
                        q_phy_ops.remove(op)

            #else:
                #print 'No ops in view: ', view[0]['view']

        increment_element_count(len(expanded_query), exp_lengths)
        increment_element_count(len(bz2.compress(expanded_query)), comp_exp_lengths)
        increment_element_count(len(q_ops), ops)
        increment_element_count(len(set(q_ops)), distinct_ops)
        increment_element_count(len(q_phy_ops), physical_ops)
        increment_element_count(len(set(q_phy_ops)), distinct_physical_ops)


        if '--' in q['query']:
            pass
        else:
            tokens = sqltokens.get_tokens(q['query'])
            increment_element_count(len(tokens), str_ops)
            increment_element_count(len(set(tokens)), distinct_str_ops)
            ex_tokens = sqltokens.get_tokens(expanded_query)
            increment_element_count(len(ex_tokens), exp_str_ops)
            increment_element_count(len(set(ex_tokens)), exp_distinct_str_ops)

        plan = json.loads(q['plan'])
        tables = visit_operators(plan, visitor_tables)
        increment_element_count(len(tables), touch)

        #'Source|owner|Query|starttime|duration|length|comp_length|exp_length|comp_exp_lengths|
        #ops|distinct_ops|exp_ops|exp_distinct_ops|keywords|distinct_keywords|expanded_keywords|exp_distinct_keywords|Touch\n'
        if write_to_file:
            f.write('%s|%s|%s|%s|%s|%f|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d\n'%
                ('sqlshare',q['owner'], q['query'], q['time_start'],q['runtime'],lengths[-1], comp_lengths[-1], exp_lengths[-1], comp_exp_lengths[-1],
                    ops[-1],distinct_ops[-1],exp_ops[-1],exp_distinct_ops[-1],str_ops[-1],distinct_str_ops[-1],exp_str_ops[-1],exp_distinct_str_ops[-1],touch[-1]))

    write_to_csv(lengths, 'length', 'count', 'Results/lengths.csv')
    write_to_csv(comp_lengths, 'comp_length', 'count', 'Results/comp_lengths.csv')
    write_to_csv(exp_lengths, 'exp_length', 'count', 'Results/exp_lengths.csv')
    write_to_csv(comp_exp_lengths, 'comp_exp_length', 'count', 'Results/comp_exp_lengths.csv')
    write_to_csv(ops, 'ops', 'count', 'Results/ops.csv')
    write_to_csv(distinct_ops, 'distinct_ops', 'count', 'Results/distinct_ops.csv')
    write_to_csv(exp_ops, 'exp_ops', 'count', 'Results/exp_ops.csv')
    write_to_csv(exp_distinct_ops, 'exp_distinct_ops', 'count', 'Results/exp_distinct_ops.csv')
    write_to_csv(str_ops, 'str_ops', 'count', 'Results/str_ops.csv')
    write_to_csv(distinct_str_ops, 'distinct_str_ops', 'count', 'Results/distinct_str_ops.csv')
    write_to_csv(exp_str_ops, 'exp_str_ops', 'count', 'Results/exp_str_ops.csv')
    write_to_csv(exp_distinct_str_ops, 'exp_distinct_str_ops', 'count', 'Results/exp_distinct_str_ops.csv')
    write_to_csv(touch, 'touch', 'count', 'Results/touch.csv')
    write_to_csv(dataset_touch, 'dataset_touch', 'count', 'Results/dataset_touch.csv')
    write_to_csv(time_taken, 'time_taken', 'count', 'Results/time_taken.csv')
    write_to_csv(physical_ops, 'physical_ops', 'count', 'Results/physical_ops.csv')
    write_to_csv(distinct_physical_ops, 'distinct_physical_ops', 'count', 'Results/distinct_physical_ops.csv')
    write_to_csv(exp_physical_ops, 'exp_physical_ops', 'count', 'Results/exp_physical_ops.csv')
    write_to_csv(exp_distinct_physical_ops, 'exp_distinct_physical_ops', 'count', 'Results/exp_distinct_physical_ops.csv')

    # print 'lengths = ', lengths
    # print 'comp_lengths = ', comp_lengths
    # print 'exp_lengths = ', exp_lengths
    # print 'comp_exp_lengths = ', comp_exp_lengths

    # print 'ops = ', ops
    # print 'distinct_ops = ', distinct_ops
    # print 'exp_ops = ', exp_ops
    # print 'exp_distinct_ops = ', exp_distinct_ops

    # print 'str_ops = ', str_ops
    # print 'distinct_str_ops = ', distinct_str_ops
    # print 'exp_str_ops = ', exp_str_ops
    # print 'exp_distinct_str_ops = ', exp_distinct_str_ops
    # print 'touch = ', touch
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


if __name__ == '__main__':
    queries = [{ 'plan': '{"physicalOp": "Compute Scalar", "io": 0.0, "rowSize": 116.0, "cpu": 1e-07, "numRows": 1.0, "filters": ["myskyserver.dbo.fiaufromeqmyskyserver.dbo.specphotoall.ra", "foobar"], "operator": "Compute Scalar", "total": 0.0525204, "children": [{"physicalOp": "Sort", "io": 0.0112613, "rowSize": 80.0, "cpu": 0.00010008, "numRows": 1.0, "filters": ["100"], "operator": "TopN Sort", "total": 0.0525202, "children": [{"physicalOp": "Clustered Index Scan", "io": 0.0386806, "rowSize": 80.0, "cpu": 0.0010436, "numRows": 1.0, "filters": ["myskyserver.dbo.specphotoall.dec", "myskyserver.dbo.specphotoall.ra", "myskyserver.dbo.specphotoall.type"], "operator": "Clustered Index Scan", "total": 0.0397242, "children": [], "columns": {"SpecPhotoAll": ["z", "modelMag_g", "modelMag_r", "modelMag_i", "field", "run", "objID", "specObjID", "rerun", "obj", "dec", "type", "camcol", "ra"]}}], "columns": {"SpecPhotoAll": ["modelMag_r", "bar"]}}], "columns": {"SpecPhotoAll": ["dec", "ra"]}}'},
               { 'plan': '{"physicalOp": "Compute Scalar", "io": 0.0, "rowSize": 116.0, "cpu": 1e-07, "numRows": 1.0, "filters": ["myskyserver.dbo.fiaufromeqmyskyserver.dbo.specphotoall.ra"], "operator": "Compute Scalar", "total": 0.0525204, "children": [{"physicalOp": "Sort", "io": 0.0112613, "rowSize": 80.0, "cpu": 0.00010008, "numRows": 1.0, "filters": ["100"], "operator": "TopN Sort", "total": 0.0525202, "children": [{"physicalOp": "Clustered Index Scan", "io": 0.0386806, "rowSize": 80.0, "cpu": 0.0010436, "numRows": 1.0, "filters": ["myskyserver.dbo.specphotoall.dec", "myskyserver.dbo.specphotoall.ra", "myskyserver.dbo.specphotoall.type"], "operator": "Clustered Index Scan", "total": 0.0397242, "children": [], "columns": {"SpecPhotoAll": ["z", "modelMag_g", "modelMag_r", "modelMag_i", "field", "run", "objID", "specObjID", "rerun", "obj", "dec", "type", "camcol", "ra"]}}], "columns": {"SpecPhotoAll": ["modelMag_r"]}}], "columns": {"SpecPhotoAll": ["dec", "ra"]}}'}]
    find_recurring_subset(queries)
