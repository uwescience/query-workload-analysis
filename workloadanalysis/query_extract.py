import dataset
import json


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
    datasetdb = dataset.connect(db)

    queries = datasetdb['tpchqueries']

    tables = datasetdb['tpch_tables']
    logops = datasetdb['tpch_logops']
    physops = datasetdb['tpch_physops']

    datasetdb.begin()

    datasetdb.query("truncate table tpch_tables")
    datasetdb.query("truncate table tpch_operators")

    for query in queries:
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
