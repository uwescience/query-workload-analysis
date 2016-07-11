__author__ = 'shrainik'
import dataset
import json
import copy

def calculate_cost_per_operator(plan, total):
    if len(plan['children']) == 0:
        plan['operator_cost'] = plan['total']/total * 100.0
        return plan
    plan['operator_cost'] = (plan['total'] - sum([x['total'] for x in plan['children']]))/total * 100.0

    new_children = []
    for c in plan['children']:
        new_children.append(calculate_cost_per_operator(c, total))
    plan['children'] = new_children
    return plan

def listify(plan, extract_func):
    op_list = extract_func(plan)
    for c in plan['children']:
        op_list += listify(c, extract_func)
    return op_list

def extract_name_cost(plan):
    return [(plan['physicalOp'], plan['operator_cost'])]

db = dataset.connect('postgresql+psycopg2:///sqlsharelogs')
plans = db.query('select query, plan from sqlshare_logs where has_plan = true')

queries_file = open('queries.txt', 'w')
operators_file = open('operators.csv', 'w')
count = 1
for q_plan in plans:
    json_plan = json.loads(q_plan['plan'])
    plan_with_cost = calculate_cost_per_operator(json_plan, json_plan['total'])

    op_list = listify(plan_with_cost, extract_name_cost)

    def getKey(l_item):
        return l_item[1]

    queries_file.write(str(count) + ':  ' + q_plan['query'].encode('utf-8') + '\n')
    for operator in sorted(op_list, key=getKey, reverse=True):
        operators_file.write(str(count) + ', ' + operator[0] + ', ' + str(operator[1]) + '\n')
    count += 1

queries_file.close()
operators_file.close()