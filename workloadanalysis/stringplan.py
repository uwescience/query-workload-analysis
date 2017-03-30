import json
import dataset

visitor_logical_ops = lambda x: [x['operator']]

def visit_operators(query_plan, visitor):
    l = visitor(query_plan)
    for child in query_plan['children']:
        l += visit_operators(child, visitor)
    return l

def removeTableAndColumnNames(query_plan):
    cols = {}
    for i, k in enumerate(query_plan['columns']):
        cols[i] = 'Table' + str(i) + ''.join(["Column" + str(x) for x in range(len(query_plan['columns'][k]))])

    query_plan['columns'] = cols
    query_plan['filters'] = ['filter' + str(j) for j in range(1000, 1000 + len(query_plan['filters']))]
    for child in query_plan['children']:
        removeTableAndColumnNames(child)

def preorder_string(query_plan):
    s = ""
    s += query_plan['operator'] + "   "
    s += str(query_plan['columns']) + "   "
    s += str(query_plan['filters']) + "   "
    # s += str(query_plan['cpu']) + "   "
    # s += str(query_plan['io']) + "   "
    # s += str(query_plan['total']) + "   "
    # s += str(query_plan['numRows']) + "   "
    # s += str(query_plan['rowSize']) + "   "

    for child in query_plan['children']:
        s += preorder_string(child)
    return s

def stringify_plan(database, mode=1):
    """
    convert all plans to string of operators using pre-order traversal
    :param database: connection object for psql database containing extracted plans.
    :return: nothing
    """
    db = dataset.connect(database)

    plans = list(db.query("select query, simple_plan, plan from sqlshare_logs where has_plan=true and owner='fridayharboroceanographers@gmail.com' order by id"))
    out_file = open('/Users/shrainik/Dropbox/SQLShare/query-reco/fridayharbour/not_so_simple_plans.txt', 'w')
    out_file_q = open('/Users/shrainik/Dropbox/SQLShare/query-reco/fridayharbour/orig_simple_queries.txt', 'w')
    for p in plans:
        json_plan = json.loads(p['plan'])

        if mode == 1:
            log_ops = visit_operators(json_plan, visitor_logical_ops)
            out_file.write(' '.join([x.replace(' ','') for x in log_ops]) + '\n')
        elif mode == 2:
            removeTableAndColumnNames(json_plan)
            out_file.write(preorder_string(json_plan) + '\n')
        else:
            removeTableAndColumnNames(json_plan)
            out_file.write(json.dumps(json_plan) + '\n')
        out_file_q.write(p['query'].encode('utf-8') + ';\n')
    out_file.close()
    out_file_q.close()


if __name__ == '__main__':
    stringify_plan('postgresql+psycopg2:///sqlsharelogs', mode=2)