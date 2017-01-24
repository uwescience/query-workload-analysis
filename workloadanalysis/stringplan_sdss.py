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
        cols[i] = range(len(query_plan['columns'][k]))

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

def stringify_plan(database):
    """
    convert all plans to string of operators using pre-order traversal
    :param database: connection string
    :return: nothing
    """
    db = dataset.connect(database)

    plans = list(db.query('select id, query, simple_plan, plan from everything where has_plan=1 order by id'))
    out_file = open('sdss_plans.txt','w')
    out_file_q = open('sdss_queries.txt','w')
    out_file_l = open('sdss_labels.txt','w')
    labels = {}
    for p in plans:
        if p['simple_plan'] not in labels:
            labels[p['simple_plan']] = p['id']
        json_plan = json.loads(p['plan'])
        removeTableAndColumnNames(json_plan)
        out_file.write(json.dumps(json_plan) + '\n')
        out_file_l.write(str(labels[p['simple_plan']])+ '\n')
        out_file_q.write(p['query'].encode('utf-8') + '\n')
    out_file.close()
    out_file_q.close()
    out_file_l.close()

if __name__ == '__main__':
    stringify_plan('sqlite:///sdss.sqlite')
