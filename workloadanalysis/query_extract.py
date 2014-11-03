import dataset
import json
import sqlalchemy as sa


# visitors
visitor_tables = lambda x: x.get('columns', {}).keys()
visitor_logical_ops = lambda x: [x['operator']]
visitor_physical_ops = lambda x: [x['physicalOp']]


def visit_operators(query_plan, visitor):
    l = visitor(query_plan)
    for child in query_plan['children']:
        l += visit_operators(child, visitor)
    return l


def extract_tpch(db):
    return extract(db, 'tpchqueries', 'tpch_tables', 'tpch_logops', 'tpch_physops')


def extract_sdss(db):
    return extract(db, 'explained', 'sdss_tables', 'sdss_logops', 'sdss_physops')


def extract_sqlshare(db):
    return extract(db, 'sqlshare_logs', 'sqlshare_tables', 'sqlshare_logops', 'sqlshare_physops')


def extract(db, query_table, tables_name, logops_name, physops_name):
    datasetdb = dataset.connect(db)

    queries = datasetdb[query_table]

    tables = datasetdb[tables_name]
    logops = datasetdb[logops_name]
    physops = datasetdb[physops_name]

    try:
        datasetdb.query("truncate table %s" % tables_name)
    except sa.exc.ProgrammingError:
        pass

    try:
        datasetdb.query("truncate table %s" % logops_name)
    except sa.exc.ProgrammingError:
        pass

    try:
        datasetdb.query("truncate table %s" % physops_name)
    except sa.exc.ProgrammingError:
        pass

    datasetdb.begin()

    for query in queries:
        if not query['plan']:
            continue
        plan = json.loads(query['plan'])

        log_ops = visit_operators(plan, visitor_logical_ops)
        logops.insert_many([
            {'query_id': query['id'], 'log_operator': x} for x in log_ops])

        phys_ops = visit_operators(plan, visitor_physical_ops)
        physops.insert_many([
            {'query_id': query['id'], 'phys_operator': x} for x in phys_ops])

        tables_ = visit_operators(plan, visitor_tables)
        tables.insert_many([
            {'query_id': query['id'], 'table': x} for x in tables_])

    datasetdb.commit()
