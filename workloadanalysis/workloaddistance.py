"""QWLA workload distance calculations. Based on algorithm in the following paper:
Mozafari, Barzan, Eugene Zhen Ye Goh, and Dong Young Yoon. "CliffGuard: A Principled Framework for Finding Robust
Database Designs." Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data. ACM, 2015.
"""

import numpy as np
import dataset
from collections import Counter


def calculate(database):
    db = dataset.connect(database)
    print 'Total Queries: ', list(db.query('select count(distinct(query_id)) from sqlshare_columns'))[0]['count']
    queries = list(db.query('select distinct(string_agg("column", \',\')) as query from (select query_id,  "column" from sqlshare_logs a, sqlshare_columns b where a.id = b.query_id order by query_id, "column") a group by query_id;'))
    print 'Total distinct queries based on columns accessed: ', len(queries)
    queries = list(db.query(
        'select owner, query_id, string_agg("column", \'#############\') as query from (select owner, query_id,  "column" from sqlshare_logs a, sqlshare_columns b where a.id = b.query_id and (owner = \'dhalperi@washington.edu\') order by owner, query_id, "column") a group by owner, query_id'
    ))
    columns = []
    all_columns = set()
    for q in queries:
        columns.append(q['query'])
        all_columns = all_columns.union(set(q['query'].split('#############')))

    subsets = list(set(columns))
    print len(queries)
    print len(subsets)

    S = [[0] * len(subsets)] * len(subsets)

    for i in range(len(subsets)):
        for j in range(len(subsets)):
            if i == j:
                S[i][j] = 0
            else:
                set_i = set(subsets[i].split('#############'))
                set_j = set(subsets[j].split('#############'))
                S[i][j] = (len(set_i.union(set_j).difference(set_i.intersection(set_j)))*1.0)/(2.0*len(subsets))

    workload_splits = 4
    VWs = []
    for i in range(workload_splits):
        VWs.append(Counter())

    for j in range(workload_splits):
        for i in range(len(queries)):
            if j == int(i*1.0/len(queries)*workload_splits):
                VWs[j][queries[i]['query']] += 1
            else:
                VWs[j][queries[i]['query']] += 0

    vw_max = 0
    vw_min = 0
    for i in range(workload_splits):
        VWs[i] = np.matrix(VWs[i].values())
        vw_max = vw_max if vw_max > np.max(VWs[i]) else np.max(VWs[i])
        vw_min = vw_min if vw_min < np.min(VWs[i]) else np.min(VWs[i])
    for i in range(workload_splits):
        VWs[i] = (VWs[i] - vw_min)/(1.0*(vw_max - vw_min))

    for i in range(1,workload_splits):
        print (VWs[i]-VWs[0]) * np.matrix(S) * (np.transpose(VWs[i]-VWs[0]))