import json
from collections import Counter, defaultdict
from tabulate import tabulate
import dataset
import bz2
import sqltokens
import csv
import hashlib
import copy
import re

from utils import format_tabulate as ft

from pprint import pprint

# table with all queries from DR5
ALL = 'everything'

# table that contains distinct queries from ALL
DISTINCT = 'everything'

# the queries that have been explained
EXPLAINED = 'everything'

# the queries that don't have the same query plan
UNIQUE = 'everything'

# like ALL but joined with explained
EXPLAINED_ALL = 'everything'

SDSS_TABLES = map(str.lower,
                  # tables
                  ['Algorithm', 'BestTarget2Sector', 'Chunk', 'DataConstants', 'DBColumns',
                   'DBObjects', 'DBViewCols', 'Dependency', 'ELRedShift', 'Field', 'FieldProfile', 'First',
                   'Frame', 'Glossary', 'HalfSpace', 'History', 'HoleObj', 'Inventory', 'LoadHistory', 'Mask',
                   'MaskedObject', 'Match', 'MatchHead', 'Neighbors', 'ObjMask', 'PhotoAuxAll', 'PhotoObjAll',
                   'PhotoProfile', 'PhotoTag', 'Photoz', 'Photoz2', 'PlateX', 'ProfileDefs', 'ProperMotions',
                   'QsoBest', 'QsoBunch', 'QsoCatalogAll', 'QsoConcordanceAll', 'QsoSpec', 'QsoTarget',
                   'QuasarCatalog', 'QueryResults', 'RC3', 'RecentQueries', 'Region', 'Region2Box', 'RegionArcs',
                   'RegionConvex', 'Rmatrix', 'Rosat', 'RunQA', 'RunShift', 'SDSSConstants', 'Sector', 'Sector2Tile',
                   'Segment', 'SiteConstants', 'SiteDBs', 'SpecLineAll', 'SpecLineIndex', 'SpecObjAll', 'SpecPhotoAll',
                   'Stetson', 'StripeDefs', 'TableDesc', 'Target', 'TargetInfo', 'TargetParam', 'TargRunQA', 'TileAll',
                   'TiledTargetAll', 'TilingGeometry', 'TilingInfo', 'TilingNote', 'TilingRun', 'USNO', 'Versions',
                   'XCRedshift', 'Zone'] +
                  # views
                  ['Columns', 'CoordType', 'FieldMask', 'FieldQuality', 'FramesStatus', 'Galaxy', 'GalaxyTag',
                   'HoleType',
                   'ImageMask', 'InsideMask', 'MaskType', 'ObjType', 'PhotoAux', 'PhotoFamily', 'PhotoFlags',
                   'PhotoMode',
                   'PhotoObj', 'PhotoPrimary', 'PhotoSecondary', 'PhotoStatus', 'PhotoType', 'PrimTarget',
                   'ProgramType',
                   'PspStatus', 'QsoCatalog', 'QsoConcordance', 'Run', 'SecTarget', 'Sky', 'SpecClass', 'SpecLine',
                   'SpecLineNames', 'SpecObj', 'SpecPhoto', 'SpecZStatus', 'SpecZWarning', 'Star', 'StarTag', 'Tile',
                   'TiledTarget', 'TilingBoundary', 'TilingMask', 'TiMask', 'Unknown'])


# visitors
visitor_tables = lambda x: x.get('columns', {}).keys()
visitor_logical_ops = lambda x: [x['operator']]
visitor_physical_ops = lambda x: [x['physicalOp']]

comparator = lambda x: x['operator'] + str(hash(hashable(x['columns'])))


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
    f.update(tree['operator'])
    f.update(hashable(tree['columns']))
    f.update(hashable(tree['filters']))
    [f.update(get_hash(child)) for child in sorted(tree['children'], key=comparator)]
    return f.hexdigest()


def print_op_tree(tree, indent=0):
    print ' ' * indent, tree['operator']
    for child in sorted(tree['children'], key=lambda x: x['operator']):
        print_op_tree(child, indent + 1)


def find_recurring(queries, workload='sdss'):
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
        elif len(tree['children']):  # ignore leaves
            seen[h] = tree
            rows_cached[0] += tree['numRows']
            for child in tree['children']:
                check_tree(child)

    for query in queries:
        saved = [0]
        plan = json.loads(query['plan'])
        # print_op_tree(plan)
        cost += plan['total']
        check_tree(plan)
        savings[saved[0] / plan['total']] += 1

    print "Saved cost", cost_saved, str(cost_saved[0] / cost * 100) + "%"
    print "Remaining cost", cost - cost_saved[0]
    print "Cached rows:", rows_cached[0]

    print_table(sorted(
        savings.iteritems(),
        key=lambda t: t[0]), ["savings", "count"], workload)


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
    empty_cols = defaultdict(set)  # special index for empty columns indexed by operator
    seen = {}
    cost = 0
    cost_saved = [0]
    usefulness = Counter()  # how often a subtree is used

    def add_to_index(tree):
        h = get_hash(tree)
        assert not h in seen or seen[h] == tree
        seen[h] = tree
        if not len(tree['columns']):
            empty_cols[tree['operator']].add(h)
        for col in tree['columns']:
            columns[col].add(h)

    def have_seen(tree):
        tablematches = None
        for i, column in enumerate(tree['columns']):
            c = columns[column]
            if i == 0:
                tablematches = c
            else:
                tablematches = tablematches.intersection(c)
        if tablematches is None:
            tablematches = empty_cols[tree['operator']]
        if not tablematches:
            return False
        matches = [seen[h] for h in tablematches]
        return check_child_matches(tree, matches)

    def check_tree(tree, level=0):
        m = have_seen(tree)
        if m:
            cost_saved[0] += tree['total']
            # pprint(m)
            #pprint(tree)
            #print "have seen", level
            usefulness[get_hash(m)] += 1
        elif len(tree['children']):  # ignore leaf operators
            add_to_index(tree)
            for child in tree['children']:
                check_tree(child, level + 1)

    for i, query in enumerate(queries):
        plan = transformed(json.loads(query['plan']))
        # pprint(plan)
        cost += plan['total']
        check_tree(plan)
        if not i % 100:
            print "Looked for reuse in", i, len(seen), len(columns), len(empty_cols)

    for h, c in usefulness.most_common(5):
        print c, seen[h]

    print "Saved cost", cost_saved, str(cost_saved[0] / cost * 100) + "%"
    print "Remaining cost", cost - cost_saved[0]


def print_table(data, headers, workload='sdss'):
    # print tabulate(data, headers, tablefmt='latex')
    name = 'results/' + workload + '/' + '_'.join(headers).replace(' ', '_') + '.csv'
    with open(name, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)


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


def analyze_sdss(database, analyze_recurring):
    db = dataset.connect(database)

    print "Limited to DR5"

    num_interesting_queries = list(db.query('SELECT COUNT(*) c FROM {} where has_plan = 1'.format(EXPLAINED)))[0]['c']
    print "Distinct queries with query plan:", num_interesting_queries

    num_interesting_queries = list(db.query('SELECT COUNT(*) c FROM (SELECT distinct simple_plan from {} where has_plan = 1)'.format(UNIQUE)))[0]['c']
    print "Distinct queries with constants replaced:", num_interesting_queries

    expl_queries = '''
        SELECT query, plan, time_start, estimated_cost
        FROM {}
        WHERE estimated_cost < 100
        ORDER BY time_start ASC
        '''.format(EXPLAINED)

    dist_queries = '''
        SELECT query, plan, estimated_cost
        FROM {}
        WHERE estimated_cost < 100
        ORDER BY time_start ASC'''.format(UNIQUE)

    all_queries = '''
        SELECT *
        FROM {}
        ORDER BY time_start ASC'''.format(EXPLAINED_ALL)

    if analyze_recurring:
        print
        print "Find recurring subtrees in distinct (query) queries:"
        queries = db.query(expl_queries)
        find_recurring(queries)

        # stored csv from previous will be overwritten

        print
        print "Find recurring subtrees in distinct (template) queries:"
        queries = db.query(dist_queries)
        find_recurring(queries)

        print
        print "Find recurring subtrees in distinct (template) queries (using subset check):"
        queries = db.query(dist_queries)
        find_recurring_subset(queries)

    print
    queries = db.query(expl_queries)
    explicit_implicit_joins(queries)

    # counters for how often we have a certain count in a query
    compressed_lengths = Counter()
    lengths = Counter()
    str_ops = Counter()
    distinct_str_ops = Counter()
    estimated = Counter()
    tables_seen = set()
    which_str_ops = Counter()

    table_clusters = []

    # count how many new tables we see
    not_yet_seen_tables = []
    last = 0

    # go over all queries (joined with explained)
    print "Go over all queries"
    for i, q in enumerate(db.query(all_queries)):
        last = i
        plan = json.loads(q['plan'])
        tables = visit_operators(plan, visitor_tables)
        new_tables = set(tables) - tables_seen
        if new_tables:
            for t in new_tables:
                tables_seen.add(t)
            not_yet_seen_tables.append([i, len(new_tables)])
        if not i % 100000:
            print "Went over", i

    print
    not_yet_seen_tables.append([last, 0])
    print_table(not_yet_seen_tables, headers=['query_number', 'num_new_tables'])

    # go over distinct queries
    print "Go over distinct queries"
    for q in db.query(expl_queries):
        plan = json.loads(q['plan'])
        tables = visit_operators(plan, visitor_tables)

        # only valid sdss tables
        table_set = set([x.lower() for x in tables]) & set(SDSS_TABLES)
        if len(table_set):
            equal = []
            for i, c in enumerate(table_clusters):
                if c.intersection(table_set):
                    equal.append(i)
                    table_clusters[i] = c | table_set
            equal.append(len(table_clusters))
            table_clusters.append(table_set)

            if len(equal) > 1:
                first = equal[0]
                for i in equal[1:]:
                    table_clusters[first] = table_clusters[first] | table_clusters[i]
            table_clusters = [x for i, x in enumerate(table_clusters) if i not in equal[1:]]

        query = q['query']
        lengths[len(query)] += 1

        compressed_lengths[len(bz2.compress(query))] += 1

        estimated[q['estimated_cost']] += 1

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
        headers=["string_op", "count"])

    print_table(sorted(
        [[str(list(x))] for x in table_clusters],
        key=lambda t: len(t), reverse=True),
        headers=["table_cluster"])

    for name, values in zip(
            ['compressed lengths', 'lengths', 'string ops', 'distinct string ops', 'estimated'],
            [compressed_lengths, lengths, str_ops, distinct_str_ops, estimated]):
        print
        print_table(sorted(
            values.iteritems(),
            key=lambda t: t[0]),
            headers=[name, "counts"])


def analyze_tpch(database):
    db = dataset.connect(database)

    expression_ops = db.query("""select class, operator, count(*)
    from expr_ops_tpch group by class, operator order by class, operator;""")

    print_table([[x['class'], x['operator'], x['count']] for x in expression_ops],
                ["class", "operator", "count"], 'tpch')

    queries = list(db['tpchqueries'])

    print
    explicit_implicit_joins(queries)

    # counters for how often we have a certain count in a query
    compressed_lengths = Counter()
    str_ops = Counter()
    distinct_str_ops = Counter()
    estimated = Counter()
    which_str_ops = Counter()
    table_clusters = []

    not_yet_seen_tables = []
    tables_seen = set()
    last = 0

    print "Find recurring subtrees in queries queries:"
    find_recurring(queries, 'tpch')
    find_recurring_subset(queries)

    for idx, q in enumerate(queries):
        last = idx
        plan = json.loads(q['plan'])
        tables = visit_operators(plan, visitor_tables)

        # only valid sdss tables
        table_set = set([x.lower() for x in tables]) & set(SDSS_TABLES)
        if len(table_set):

            equal = []
            for i, c in enumerate(table_clusters):
                if c.intersection(table_set):
                    equal.append(i)
                    table_clusters[i] = c | table_set
            equal.append(len(table_clusters))
            table_clusters.append(table_set)

            if len(equal) > 1:
                first = equal[0]
                for i in equal[1:]:
                    table_clusters[first] = table_clusters[first] | table_clusters[i]
            table_clusters = [x for i, x in enumerate(table_clusters) if i not in equal[1:]]

        new_tables = set(tables) - tables_seen
        if new_tables:
            for t in new_tables:
                tables_seen.add(t)
            not_yet_seen_tables.append([idx, len(new_tables)])

        query = q['query']
        compressed_lengths[len(bz2.compress(query))] += 1

        estimated[q['estimated_cost']] += 1

        tokens = sqltokens.get_tokens(query)
        str_ops[len(tokens)] += 1
        distinct_str_ops[len(set(tokens))] += 1

        which_str_ops.update(tokens)

    print
    not_yet_seen_tables.append([last, 0])
    print_table(not_yet_seen_tables, headers=['query_number', 'num_new_tables'], workload="tpch")

    print
    print_table(sorted(
        which_str_ops.iteritems(),
        key=lambda t: t[1], reverse=True),
        headers=["string_op", "count"], workload='tpch')

    print_table(sorted(
        [[str(list(x))] for x in table_clusters],
        key=lambda t: len(t), reverse=True),
        headers=["table_cluster"], workload='tpch')

    for name, values in zip(
            ['compressed lengths', 'string ops', 'distinct string ops', 'estimated'],
            [compressed_lengths, str_ops, distinct_str_ops, estimated]):
        print_table(sorted(
            values.iteritems(),
            key=lambda t: t[0]),
            headers=[name, "counts"], workload='tpch')


def analyze_sqlshare(database, all_owners=True):
    db = dataset.connect(database)
    owners = [''];
    p = re.compile(
        ur'^(\w+\.(csv|txt))[A-F0-9]{5}$')  # regular expression to match table names of the form *.(txt|csv)_____ <- these are all 1 table
    columns_q = "SELECT query_id as id, count(\"column\") as columns from sqlshare_columns group by query_id"
    expressions_q = 'SELECT query as id, count(operator) as expressions from ops_table group by query'
    columns_count = {}
    expr_count = {}
    columns = db.query(columns_q)
    expressions = db.query(expressions_q)
    for q in columns:
        columns_count[q['id']] = q['columns']
    for q in expressions:
        expr_count[q['id']] = q['expressions']

    # print columns_count, expr_count

    views_q = 'SELECT * FROM sqlshare_logs WHERE isview = 1'

    if not all_owners:
        owners = ['']
        top_owners = db.query('select owner from sqlshare_logs group by owner order by count(*) desc limit 100')
        for result in top_owners:
            owners.append(result['owner'])
    view_depth_breadth = open('../results/sqlshare/view_depth_breadth.csv', 'w')
    view_depth_breadth_per_view = open('../results/sqlshare/view_depth_breadth_per_view.csv', 'w')
    view_depth_breadth.write("user, max_depth, max_breadth\n")
    view_depth_breadth_per_view.write("id, depth, breadth\n")
    for owner in owners:
        print owner if owner != '' else 'all'
        if owner == '':
            distinct_q = 'SELECT plan from sqlshare_logs where has_plan = 1 group by plan'
            all_queries_q = 'SELECT * from sqlshare_logs where has_plan = 1'
            queries_q = 'SELECT id, query, plan, length, runtime, expanded_plan_ops_logical, expanded_plan_ops, ref_views, time_start, isview, view from sqlshare_logs where has_plan = 1 group by id, query, plan, length, runtime, expanded_plan_ops_logical, expanded_plan_ops, ref_views, time_start, isview, view order by to_timestamp(time_start, \'MM/DD/YYYY HH12:MI:SS am\')'
            query_with_same_plan_q = 'SELECT Count(*) as count from (SELECT distinct simple_plan from sqlshare_logs where has_plan = 1) as foo'
        else:
            owner_condition = 'owner = \'' + owner + '\''
            distinct_q = 'SELECT plan from sqlshare_logs where has_plan = 1 and ' + owner_condition + ' group by plan'
            all_queries_q = 'SELECT * from sqlshare_logs where has_plan = 1 and ' + owner_condition + ' '
            queries_q = 'SELECT id, query, plan, length, runtime, expanded_plan_ops_logical, expanded_plan_ops, ref_views, time_start, isview, view from sqlshare_logs where has_plan = 1 and ' + owner_condition + '  group by id, query, plan, length, runtime, expanded_plan_ops_logical, expanded_plan_ops, ref_views, time_start, isview, view order by to_timestamp(time_start, \'MM/DD/YYYY HH12:MI:SS am\')'
            query_with_same_plan_q = 'SELECT Count(*) as count from (SELECT distinct simple_plan from sqlshare_logs where has_plan = 1 and ' + owner_condition + ' ) as foo'

        # print "Find recurring subtrees in distinct queries (using subset check):"
        q = db.query(distinct_q)
        find_recurring_subset(q)

        all_queries = list(db.query(all_queries_q))
        queries = list(db.query(queries_q))
        views = list(db.query(views_q))
        query_with_same_plan = list(db.query(query_with_same_plan_q))

        print '#Total queries with plan: ', len(all_queries)
        print '#Total string distinct queries:', len(queries)
        explicit_implicit_joins(queries)
        print '#Total queries considering all constants the same:', query_with_same_plan[0]['count']
        all_tables = list(db.query('Select distinct("table") from sqlshare_tables'))
        non_trivial_views = list(db.query("select view from sqlshare_logs where isview = 1 and lower(query) not like 'select * from %.[table_%]'"))
        list_nt_views =[]
        for ntview in non_trivial_views:
            list_nt_views.append(ntview['view'])
        del non_trivial_views
        #comp_lengths = Counter()
        ops = Counter()
        exp_lengths = Counter()
        exp_ops = Counter()
        exp_distinct_ops = Counter()
        exp_physical_ops = Counter()
        exp_distinct_physical_ops = Counter()
        # comp_exp_lengths = Counter()
        str_ops = Counter()
        distinct_str_ops = Counter()
        exp_str_ops = Counter()
        exp_distinct_str_ops = Counter()
        #time_taken = Counter()
        dataset_touch = Counter()
        keywords_count = Counter()
        table_coverage = {}
        dataset_coverage = {}

        datasets_seen_so_far = []
        tables_seen_so_far = []
        tables_in_query = Counter()
        tables = []
        view_depth = []
        view_breadth = []
        cummu_q_table_by_time = open('../results/sqlshare/' + owner + 'cummu_q_table_by_time.csv', 'w')

        for i, q in enumerate(queries):
            # comp_length = len(bz2.compress(q['query']))
            # comp_lengths[comp_length] += 1
            # if q['isView'] == 0:
            #     time_taken[q['runtime']] += 1

            expanded_query = q['query']
            # calculating dataset touch and expanded query now.
            ref_views = {}
            total_ref_views = {}
            vd = 0
            vb = 0
            while (True):
                previousLength = len(expanded_query)
                ref_views = {}
                for view in views:
                    if view['view'] in expanded_query:
                        ref_views[view['view']] = view['query']
                        total_ref_views[view['view']] = view['query']
                for t in all_tables:
                    if t['table'] in expanded_query:
                        ref_views[t['table']] = t['table'] + 'table_table'
                        total_ref_views[t['table']] = t['table']
                for v in ref_views:
                    if 'table_table' not in ref_views[v]:
                        expanded_query = expanded_query.replace(v, '(' + ref_views[v] + ')')
                if vb < len(ref_views):
                    vb = len(ref_views)
                if len(ref_views) > 0:
                    vd +=1
                if (len(expanded_query) == previousLength):
                    dataset_touch[len(total_ref_views)] += 1
                    for d in total_ref_views:
                        if d not in datasets_seen_so_far:
                            datasets_seen_so_far.append(d)
                    break
            view_breadth.append(vb)
            view_depth.append(vd)
            if q['isview'] == 1:
                view_depth_breadth_per_view.write("%s,%d,%d\n" % (q['id'], vd, vb)) 
            q_ex_ops = q['expanded_plan_ops_logical'].split(',')
            exp_ops[len(q_ex_ops)] += 1
            exp_distinct_ops[len(set(q_ex_ops))] += 1

            q_ex_phy_ops = q['expanded_plan_ops'].split(',')
            exp_physical_ops[len(q_ex_phy_ops)] += 1
            exp_distinct_physical_ops[len(set(q_ex_phy_ops))] += 1

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

            exp_lengths[len(expanded_query)] += 1
            # comp_exp_lengths[len(bz2.compress(expanded_query))] += 1
            ops[len(q_ops)] += 1

            plan = json.loads(q['plan'])
            tables = visit_operators(plan, visitor_tables)
            tables = set(tables)

            logical_tables = []
            for t in tables:
                short_name = re.findall(p, t)
                if len(short_name) == 0:
                    logical_tables.append(t)
                    if t not in tables_seen_so_far:
                        tables_seen_so_far.append(t)
                else:
                    logical_tables.append(short_name[0])
                    if short_name[0] not in tables_seen_so_far:
                        tables_seen_so_far.append(short_name[0])

            tables_in_query[len(set(logical_tables))] += 1
            table_coverage[i] = len(tables_seen_so_far)
            dataset_coverage[i] = len(datasets_seen_so_far)
            cummu_q_table_by_time.write("%d, %d, %s\n" % (i, len(tables_seen_so_far), q['time_start']))

        cummu_q_table_by_time.close()
        view_depth_breadth.write("%s,%d,%d\n" % (owner if owner != '' else 'all', 0 if len(view_depth) == 0 else max(view_depth), 0 if len(view_depth) == 0 else max(view_breadth)))

        def write_to_csv(dict_obj, col1, col2, filename, to_reverse=True):
            f = open(filename, 'w')
            f.write("%s,%s\n" % (col1, col2))
            for key in sorted(dict_obj, reverse=to_reverse):
                f.write("%s,%s\n" % (key, dict_obj[key]))
            f.close()

        # write_to_csv(comp_lengths, 'comp_length', 'count', '../results/sqlshare/'+owner+'comp_lengths.csv')
        write_to_csv(exp_lengths, 'exp_length', 'count', '../results/sqlshare/' + owner + 'exp_lengths.csv')
        # write_to_csv(comp_exp_lengths, 'comp_exp_length', 'count', '../results/sqlshare/'+owner+'comp_exp_lengths.csv')
        write_to_csv(ops, 'ops', 'count', '../results/sqlshare/' + owner + 'ops.csv')
        write_to_csv(exp_ops, 'exp_ops', 'count', '../results/sqlshare/' + owner + 'exp_ops.csv')
        write_to_csv(exp_distinct_ops, 'exp_distinct_ops', 'count',
                     '../results/sqlshare/' + owner + 'exp_distinct_ops.csv')
        # write_to_csv(str_ops, 'str_ops', 'count', '../results/sqlshare/'+owner+'str_ops.csv')
        # write_to_csv(distinct_str_ops, 'distinct_str_ops', 'count', '../results/sqlshare/'+owner+'distinct_str_ops.csv')
        # write_to_csv(exp_str_ops, 'exp_str_ops', 'count', '../results/sqlshare/'+owner+'exp_str_ops.csv')
        # write_to_csv(exp_distinct_str_ops, 'exp_distinct_str_ops', 'count', '../results/sqlshare/'+owner+'exp_distinct_str_ops.csv')
        write_to_csv(dataset_touch, 'dataset_touch', 'count', '../results/sqlshare/' + owner + 'dataset_touch.csv')
        write_to_csv(tables_in_query, 'number', 'count', '../results/sqlshare/' + owner + 'table_touch.csv')
        # write_to_csv(time_taken, 'time_taken', 'count', '../results/sqlshare/'+owner+'time_taken.csv')
        write_to_csv(exp_physical_ops, 'exp_physical_ops', 'count',
                     '../results/sqlshare/' + owner + 'exp_physical_ops.csv')
        write_to_csv(exp_distinct_physical_ops, 'exp_distinct_physical_ops', 'count',
                     '../results/sqlshare/' + owner + 'exp_distinct_physical_ops.csv')
        write_to_csv(table_coverage, 'query_id', 'tables', '../results/sqlshare/' + owner + 'table_coverage.csv',
                     to_reverse=False)
        write_to_csv(dataset_coverage, 'query_id', 'tables', '../results/sqlshare/' + owner + 'dataset_coverage.csv',
                     to_reverse=False)

        touch_by_time = {}
        q_ops_by_time = {}
        q_distinct_ops_by_time = {}
        q_logops_by_time = {}
        q_distinct_logops_by_time = {}
        q_complexity_by_time = {}
        q_length_by_time = {}

        f_highcomplexity_queries = open('../results/sqlshare/high_complexity_queries.txt', 'a')
        f_lowcomplexity_queries = open('../results/sqlshare/low_complexity_queries.txt', 'a')
        for i, q in enumerate(queries):
            q_ex_ops = q['expanded_plan_ops_logical'].split(',')
            q_logops_by_time[i] = len(q_ex_ops)
            q_distinct_logops_by_time[i] = len(set(q_ex_ops))

            q_ex_phy_ops = q['expanded_plan_ops'].split(',')
            q_ops_by_time[i] = len(q_ex_phy_ops)
            q_distinct_ops_by_time[i] = len(set(q_ex_phy_ops))

            plan = json.loads(q['plan'])
            tables = visit_operators(plan, visitor_tables)
            tables = set(tables)

            logical_tables = []
            for t in tables:
                short_name = re.findall(p, t)
                if len(short_name) == 0:
                    logical_tables.append(t)
                else:
                    logical_tables.append(short_name[0])

            touch_by_time[i] = len(set(logical_tables))
            try:
                col_c = columns_count[q['id']]
            except:
                col_c = 0
                print 'Columns count defaulting to 0 for %d' % q['id']

            try:
                expr_c = expr_count[q['id']]
            except:
                expr_c = 0
                print 'Expr count defaulting to 0 for %d' % q['id']

            q_length_by_time[i] = len(q['query'])

            q_complexity_by_time[i] = (-0.00248) * touch_by_time[i] + 0.000168 * col_c + 0.001571 * q[
                'length'] + 0.012903 * q_logops_by_time[i] + 0.000355 * expr_c + 0.000000896 * q['runtime']
            if q_complexity_by_time[i] > 7:
                f_highcomplexity_queries.write(
                    "complexity: %f \n Query: \n %s \n\n\n" % (q_complexity_by_time[i], q['query'].encode('utf-8')))
            if q_complexity_by_time[i] < 1:
                f_lowcomplexity_queries.write(
                    "complexity: %f \n Query: \n %s \n\n\n" % (q_complexity_by_time[i], q['query'].encode('utf-8')))

        f_lowcomplexity_queries.close()
        f_highcomplexity_queries.close()
        write_to_csv(q_ops_by_time, 'query_id', 'count', '../results/sqlshare/' + owner + 'exp_ops_by_time.csv')
        write_to_csv(q_distinct_ops_by_time, 'query_id', 'count',
                     '../results/sqlshare/' + owner + 'exp_distinct_ops_by_time.csv')
        write_to_csv(q_logops_by_time, 'query_id', 'count',
                     '../results/sqlshare/' + owner + 'exp_physical_ops_by_time.csv')
        write_to_csv(q_distinct_logops_by_time, 'query_id', 'count',
                     '../results/sqlshare/' + owner + 'exp_distinct_physical_ops_by_time.csv')
        write_to_csv(touch_by_time, 'query_id', 'count', '../results/sqlshare/' + owner + 'table_touch_by_time.csv')
        write_to_csv(q_complexity_by_time, 'query_id', 'complexity',
                     '../results/sqlshare/' + owner + 'complexity_by_time.csv', False)
        write_to_csv(q_length_by_time, 'query_id', 'length', '../results/sqlshare/' + owner + 'length_by_time.csv',
                     False)
    view_depth_breadth.close()
    view_depth_breadth_per_view.close()


if __name__ == '__main__':
    queries = [{
               'plan': '{"physicalOp": "Compute Scalar", "io": 0.0, "rowSize": 116.0, "cpu": 1e-07, "numRows": 1.0, "filters": ["myskyserver.dbo.fiaufromeqmyskyserver.dbo.specphotoall.ra", "foobar"], "operator": "Compute Scalar", "total": 0.0525204, "children": [{"physicalOp": "Sort", "io": 0.0112613, "rowSize": 80.0, "cpu": 0.00010008, "numRows": 1.0, "filters": ["100"], "operator": "TopN Sort", "total": 0.0525202, "children": [{"physicalOp": "Clustered Index Scan", "io": 0.0386806, "rowSize": 80.0, "cpu": 0.0010436, "numRows": 1.0, "filters": ["myskyserver.dbo.specphotoall.dec", "myskyserver.dbo.specphotoall.ra", "myskyserver.dbo.specphotoall.type"], "operator": "Clustered Index Scan", "total": 0.0397242, "children": [], "columns": {"SpecPhotoAll": ["z", "modelMag_g", "modelMag_r", "modelMag_i", "field", "run", "objID", "specObjID", "rerun", "obj", "dec", "type", "camcol", "ra"]}}], "columns": {"SpecPhotoAll": ["modelMag_r", "bar"]}}], "columns": {"SpecPhotoAll": ["dec", "ra"]}}'},
               {
               'plan': '{"physicalOp": "Compute Scalar", "io": 0.0, "rowSize": 116.0, "cpu": 1e-07, "numRows": 1.0, "filters": ["myskyserver.dbo.fiaufromeqmyskyserver.dbo.specphotoall.ra"], "operator": "Compute Scalar", "total": 0.0525204, "children": [{"physicalOp": "Sort", "io": 0.0112613, "rowSize": 80.0, "cpu": 0.00010008, "numRows": 1.0, "filters": ["100"], "operator": "TopN Sort", "total": 0.0525202, "children": [{"physicalOp": "Clustered Index Scan", "io": 0.0386806, "rowSize": 80.0, "cpu": 0.0010436, "numRows": 1.0, "filters": ["myskyserver.dbo.specphotoall.dec", "myskyserver.dbo.specphotoall.ra", "myskyserver.dbo.specphotoall.type"], "operator": "Clustered Index Scan", "total": 0.0397242, "children": [], "columns": {"SpecPhotoAll": ["z", "modelMag_g", "modelMag_r", "modelMag_i", "field", "run", "objID", "specObjID", "rerun", "obj", "dec", "type", "camcol", "ra"]}}], "columns": {"SpecPhotoAll": ["modelMag_r"]}}], "columns": {"SpecPhotoAll": ["dec", "ra"]}}'}]
    find_recurring_subset(queries)
