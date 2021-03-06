import json
import sys

import dataset
import sqlalchemy as sa
import parse_xml
import query_analysis
import utils
import tpchqueries

EXAMPLE = [{'id': 42, 'has_plan': False, 'query': '''
SELECT  top 1   p.objID, p.run,
p.rerun, p.camcol, p.field, p.obj,
   p.type, p.ra, p.dec, p.u,p.g,p.r,p.i,p.z,
   p.Err_u, p.Err_g, p.Err_r,p.Err_i,p.Err_z
   FROM fGetNearbyObjEq(195,2.5,0.5) n, PhotoPrimary p
   WHERE n.objID=p.objID
'''}]

BATCH_SIZE = 50


def explain_sqlshare(database, quiet, first_pass, dry=False):
    db = dataset.connect(database)
    errors = []
    table = db['sqlshare_logs']
    ops_table = db['ops_table']

    if not first_pass:
        queries = list(db.query('SELECT * FROM sqlshare_logs Where has_plan = 1'))
        views = list(db.query('SELECT * FROM sqlshare_logs Where isview = 1'))
        for i, query in enumerate(queries):
            # print "Explain query pass 2", i

            # Getting all referenced views now
            ref_views = []
            for q in views:
                if q['view'] in query['query']:
                    ref_views.append(str(q['id']))
            query['ref_views'] = ','.join([x for x in ref_views])

            visitor_logical_ops = lambda x: [x['operator']]
            visitor_physical_ops = lambda x: [x['physicalOp']]
            ops = query_analysis.visit_operators(json.loads(query['plan']), visitor_logical_ops)
            physical_ops = query_analysis.visit_operators(json.loads(query['plan']), visitor_physical_ops)
            if ops:
                query['expanded_plan_ops_logical'] = ','.join([x for x in ops])
            else:
                print 'no logical ops'

            if physical_ops:
                query['expanded_plan_ops'] = ','.join([x for x in physical_ops])
            else:
                print 'no physical ops'
            table.update(query, ['id'])
        return

    queries = list(db.query('SELECT * FROM sqlshare_logs where has_plan = 0'))
    # views = list(db.query('SELECT * FROM sqlshare_logs WHERE isview = 1'))

    for i, query in enumerate(queries):
        # print "Explain query", i
        op_count = [0]
        #see if the plan is valid
        if query['xml_plan'][0] != '<':
            errors.append(query['xml_plan'])
            continue

        xml_string = "".join([x for x in query['xml_plan']])

        # print operators
        #parse_xml.get_physical_op_count(tree.getroot(), op_count)

        # get the simplified query plan as dictionary
        try:
            tree = parse_xml.clean(xml_string)
            query_plans = parse_xml.get_query_plans(
                tree, cost=True, show_filters=True)
            if len(query_plans) == 0:
                errors.append("No query plan found")
                print 'no query_plan'
                continue
            if len(query_plans) > 1:
                errors.append("Found two query plans")
                print 'multiple query_plan'
                continue
        except:
            print 'get query_plan error...'
            continue

        query_plan = query_plans[0]
        # if not quiet:
        #     print utils.json_pretty(query_plan)

        query['plan'] = json.dumps(query_plan, cls=utils.SetEncoder)
        simple_query_plan = parse_xml.get_query_plans(
            tree, cost=False, show_filters=True, consts=False)[0]

        query['simple_plan'] = json.dumps(
            simple_query_plan, cls=utils.SetEncoder, sort_keys=True)

        opsInQuery = []

        for op in parse_xml.get_expression_operators(tree):
            op['query'] = query['id']
            opsInQuery.append(op)

        ops_table.insert_many(opsInQuery)
        query['estimated_cost'] = query_plan['total']
        query['has_plan'] = True
        if not dry:
            table.update(query, ['id'])

    # print "Errors", errors
    print "Error: {0} \%".format(len(errors) * 100.0 / len(queries))


def get_op_tree(tree, optree, indent=0):
    optree[0] += ' ' * indent + tree['operator'] + '\n'
    for child in sorted(tree['children'], key=lambda x: x['operator']):
        get_op_tree(child, optree, indent + 1)


def explain_sdss(database, quiet=False, segments=None, dry=False, offset=0):
    # batch of queries
    batch = []

    # batch of expr ops
    ops_batch = []

    datasetdb = None
    table = None
    ops_table = None

    query = "SELECT * from everything"

    if database:
        datasetdb = dataset.connect(database)
    else:
        queries = EXAMPLE

    if datasetdb:
        table = datasetdb['everything']
        ops_table = datasetdb['expr_ops']
        queries = datasetdb.query(query)
    else:
        dry = True

    errors = []

    for i, query in enumerate(queries):
        # print "Process query index:", i
        query = dict(query)
        # print "query id:", query['id']

        entry = None
        if not dry:
            entry = table.find_one(id=query['id'])

        if entry and 'xml' in table.columns and entry['xml'] and len(entry['xml']):
            xml_string = entry['xml']
            xml_string = xml_string.strip()[1:-1]
            xml_string = xml_string.replace('""', '"')

        tree = parse_xml.clean(xml_string)

        if not quiet:
            print "==> query:", query['query']
            print

        # indent tree and export as xml file
        if not quiet:
            parse_xml.indent(tree.getroot())
            tree.write(sys.stdout)
        # tree.write('clean_{}.xml'.format(i))

        # get the simplified query plan as dictionary
        query_plans = parse_xml.get_query_plans(
            tree, cost=True, show_filters=True)
        if len(query_plans) == 0:
            errors.append("No query plan found")
            print '==> no query_plan'
            continue
        if len(query_plans) > 1:
            errors.append("Found two query plans")
            print '==> multiple query_plan'
            continue

        query_plan = query_plans[0]

        # ignore inserts
        if query_plan['operator'] == 'Insert':
            assert len(query_plan['children']) == 1
            query_plan = query_plan['children'][0]

        # if not quiet:
        #     print utils.json_pretty(query_plan)
        query['plan'] = json.dumps(query_plan, cls=utils.SetEncoder, sort_keys=True)

        query['estimated_cost'] = query_plan['total']
        query['has_plan'] = True

        # plan for uniqueness, clustering happening here
        simple_query_plan = parse_xml.get_query_plans(
            tree, cost=False, show_filters=True, consts=False)[0]

        # ignore inserts
        if simple_query_plan['operator'] == 'Insert':
            assert len(simple_query_plan['children']) == 1
            simple_query_plan = simple_query_plan['children'][0]

        query['simple_plan'] = json.dumps(
            simple_query_plan, cls=utils.SetEncoder, sort_keys=True)

        if not quiet:
            print utils.json_pretty(simple_query_plan)

        optree = ['']
        get_op_tree(simple_query_plan, optree)
        query['optree'] = optree[0]

        batch.append(query)

        if len(batch) > BATCH_SIZE and not dry:
            datasetdb.begin()
            for query in batch:
                table.update(query, ['id'])
            datasetdb.commit()
            batch = []

        # get expressions
        for op in parse_xml.get_expression_operators(tree):
            op['query'] = query['id']
            ops_batch.append(op)

        if len(ops_batch) > BATCH_SIZE and not dry:
            ops_table.insert_many(ops_batch)
            ops_batch = []

    if not dry:
        datasetdb.begin()
        for query in batch:
            table.update(query, ['id'])
        datasetdb.commit()
        ops_table.insert_many(ops_batch)

    print "Errors", errors


def explain_tpch(config, database, quiet=False, dry=False):
    """Explain queries and store the results in database
    """
    connection_string = 'mssql+pymssql://%s:%s@%s:%s/%s?charset=UTF-8' % (
        config['user'],
        config['password'],
        config['server'],
        config['port'],
        config['db'])

    db = sa.create_engine(connection_string, echo=(not quiet))

    datasetdb = None
    table = None

    if database:
        datasetdb = dataset.connect(database)
        table = datasetdb['tpchqueries']
        ops_table = datasetdb['tpch_expr_ops']
        datasetdb.query("truncate table tpch_expr_ops")
    else:
        dry = True

    errors = []

    for i, query in enumerate(tpchqueries.get_queries()):
        with db.connect() as connection:
            # clean cache to refresh constant values
            connection.execute('DBCC FREEPROCCACHE WITH NO_INFOMSGS')
            connection.execute('set showplan_xml on')

            print "Explain query", i

            import timeit

            start_time = timeit.default_timer()
            try:
                res = connection.execute(query['query']).fetchall()[0]
            except Exception as e:
                errors.append(str(e))
                print str(e)
                print '==> execute error'
                if 'closed automatically' in str(e):
                    raise
                continue

            query['elapsed'] = timeit.default_timer() - start_time

            xml_string = "".join([x for x in res])
            tree = parse_xml.clean(xml_string)

            if not quiet:
                print "==> query:", query['query']
                print

            # indent tree and export as xml file
            if not quiet:
                parse_xml.indent(tree.getroot())
                tree.write(sys.stdout)
            # tree.write('clean_{}.xml'.format(i))

            # get the simplified query plan as dictionary
            query_plans = parse_xml.get_query_plans(
                tree, cost=True, show_filters=True)
            if len(query_plans) == 0:
                errors.append("No query plan found")
                print '==> no query_plan'
                continue
            if len(query_plans) > 1:
                errors.append("Found two query plans")
                print '==> multiple query_plan'
                continue

            query_plan = query_plans[0]

            if not quiet:
                print utils.json_pretty(query_plan)
            query['plan'] = json.dumps(query_plan, cls=utils.SetEncoder, sort_keys=True)

            query['estimated_cost'] = query_plan['total']

            # plan for uniqueness, clustering happening here
            simple_query_plan = parse_xml.get_query_plans(
                tree, cost=False, show_filters=True, consts=False)[0]

            if not quiet:
                print utils.json_pretty(simple_query_plan)

            query['simple_plan'] = json.dumps(
                simple_query_plan, cls=utils.SetEncoder, sort_keys=True)

            optree = ['']
            get_op_tree(simple_query_plan, optree)
            query['optree'] = optree[0]

            if not dry:
                table.upsert(query, ['id'])

            # get expressions
            for op in parse_xml.get_expression_operators(tree):
                op['query'] = query['id']
                op['tpch_query'] = query['tp_query']
                ops_table.insert(op)

            connection.execute('set showplan_xml off')
            connection.execute('set noexec off')

    print "Errors", errors

#This version fetches the plan for each query by running explain.
def explain_sdss_old(config, database, quiet=False, segments=None, dry=False, offset=0):
    """Explain queries and store the results in database
    """
    connection_string = 'mssql+pymssql://%s:%s@%s:%s/%s?charset=UTF-8' % (
        config['user'],
        config['password'],
        config['server'],
        config['port'],
        config['db'])

    db = sa.create_engine(connection_string, echo=(not quiet))

    if not offset:
        offset = 0

    if not segments:
        segments = [0, 1]

    # batch of queries
    batch = []

    # batch of expr ops
    ops_batch = []

    datasetdb = None
    table = None
    ops_table = None

    query = "SELECT * from distinctlogs WHERE id %% {} = {} OFFSET {}".format(
        segments[1], segments[0], offset)

    if database:
        datasetdb = dataset.connect(database)
        queries = datasetdb.query(query)
        datasetdb.query("truncate table expr_ops")
    else:
        queries = EXAMPLE

    errors = []
    if datasetdb:
        table = datasetdb['logs']
        ops_table = datasetdb['expr_ops']
    else:
        dry = True

    for i, query in enumerate(queries):
        with db.connect() as connection:
            # clean cache to refresh constant values
            connection.execute('DBCC FREEPROCCACHE WITH NO_INFOMSGS')

            connection.execute('set showplan_xml on')
            connection.execute('set noexec on')

            print "Process query index:", i,
            query = dict(query)
            print "query id:", query['id']

            entry = None
            if not dry:
                entry = table.find_one(id=query['id'])

            if entry and 'xml' in table.columns and entry['xml'] and len(entry['xml']):
                xml_string = entry['xml']
            else:
                print "get explain from sql server"
                try:
                    qu = query['query'].replace('[', '"').replace(']', '"')
                    qu = qu.replace('SET PARSEONLY ON ', '')
                    res = connection.execute(qu).fetchall()[0]
                except Exception as e:
                    errors.append(str(e))
                    print str(e)
                    print '==> execute error'
                    if 'closed automatically' in str(e):
                        raise
                    continue

                xml_string = "".join([x for x in res])

                query['xml'] = xml_string

            tree = parse_xml.clean(xml_string)

            if not quiet:
                print "==> query:", query['query']
                print

            # indent tree and export as xml file
            if not quiet:
                parse_xml.indent(tree.getroot())
                tree.write(sys.stdout)
            # tree.write('clean_{}.xml'.format(i))

            # get the simplified query plan as dictionary
            query_plans = parse_xml.get_query_plans(
                tree, cost=True, show_filters=True)
            if len(query_plans) == 0:
                errors.append("No query plan found")
                print '==> no query_plan'
                continue
            if len(query_plans) > 1:
                errors.append("Found two query plans")
                print '==> multiple query_plan'
                continue

            query_plan = query_plans[0]

            # ignore inserts
            if query_plan['operator'] == 'Insert':
                assert len(query_plan['children']) == 1
                query_plan = query_plan['children'][0]

            if not quiet:
                print utils.json_pretty(query_plan)
            query['plan'] = json.dumps(query_plan, cls=utils.SetEncoder, sort_keys=True)

            query['estimated_cost'] = query_plan['total']
            query['has_plan'] = True

            # plan for uniqueness, clustering happening here
            simple_query_plan = parse_xml.get_query_plans(
                tree, cost=False, show_filters=True, consts=False)[0]

            # ignore inserts
            if simple_query_plan['operator'] == 'Insert':
                assert len(simple_query_plan['children']) == 1
                simple_query_plan = simple_query_plan['children'][0]

            query['simple_plan'] = json.dumps(
                simple_query_plan, cls=utils.SetEncoder, sort_keys=True)

            if not quiet:
                print utils.json_pretty(simple_query_plan)

            optree = ['']
            get_op_tree(simple_query_plan, optree)
            query['optree'] = optree[0]

            batch.append(query)

            if len(batch) > BATCH_SIZE and not dry:
                datasetdb.begin()
                for query in batch:
                    table.update(query, ['id'])
                datasetdb.commit()
                batch = []

            # get expressions
            for op in parse_xml.get_expression_operators(tree):
                op['query'] = query['id']
                ops_batch.append(op)

            if len(ops_batch) > BATCH_SIZE and not dry:
                ops_table.insert_many(ops_batch)
                ops_batch = []

            connection.execute('set showplan_xml off')
            connection.execute('set noexec off')

    if not dry:
        datasetdb.begin()
        for query in batch:
            table.update(query, ['id'])
        datasetdb.commit()
        ops_table.insert_many(ops_batch)

    print "Errors", errors


if __name__ == '__main__':
    config = {}
    with open(sys.argv[1]) as f:
        for line in f:
            key, val = line.split('=')
            config[key.strip()] = val.strip()
    explain_sdss(config, None, quiet=False)
