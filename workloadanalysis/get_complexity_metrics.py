"""QWLA join similarity tool.

Usage:
    getmetrics [-d DATABASE]
    getmetrics  (-h | --help)
    getmetrics  --version

Options:
    -d [DATABASE]  The database to read from or write into
    -h --help      Show this screen.
    --version      Show version.

"For a moment, nothing happened. Then, after a second or so,
nothing continued to happen."
"""

from docopt import docopt
import matplotlib.pyplot as plt
import math
import numpy as np
import dataset
import sqlalchemy as sa
from datetime import datetime
from collections import Counter

def getmetrics(database):
	db = dataset.connect(database);
	queries_q = '''with t as (select query_id as id, count("table") as tables from sqlshare_tables where query_id in (select * from qids) group by query_id),
				c as (select query_id as id, count("column") as columns from sqlshare_columns where query_id in (select * from qids) group by query_id),
				ex as (select query as id, count(operator) as expressions from ops_table where query in (select * from qids) group by query),
				lgop as (select query_id as id, count(log_operator) as log_ops from sqlshare_logops where query_id in (select * from qids) group by query_id)
				
				select s.query, s.id, s.length, s.runtime, t.tables, c.columns, ex.expressions, lgop.log_ops 
				from sqlshare_logs s, qids, ex, t, c, lgop where 
				s.id = qids.id and ex.id = s.id and s.id = t.id and s.id = c.id and lgop.id = s.id and runtime != -1 order by s.id;'''
	views_q = 'SELECT * FROM sqlshare_logs WHERE isview = false'
	views = list(db.query(views_q))
	queries = list(db.query(queries_q))
	f = open('../results/sqlshare/query_comp_metrics.csv', 'w')
	f2 = open('../results/sqlshare/query_comp_metrics_with_q.csv', 'w')
	f.write("id, length, expanded_length, runtime, tables, columns, expressions, log_ops, ref_views\n")
	f2.write("id, length, expanded_length, runtime, tables, columns, expressions, log_ops, ref_views, query\n")
	for i, q in enumerate(queries):
		expanded_query = q['query']
		# calculating dataset touch and expanded query now.
		ref_views = {}
		total_ref_views = {}
		while(True):
			previousLength = len(expanded_query)
			ref_views = {}
			for view in views:
				if view['view'] in expanded_query:
					ref_views[view['view']] = view['query']
					total_ref_views[view['view']] = view['query']
			for v in ref_views:
				expanded_query = expanded_query.replace(v, '(' + ref_views[v] + ')')
			
			if (len(expanded_query) == previousLength):
				break
		f2.write("%d, %d, %d, %d, %d, %d, %d, %d, %d, %s\n"%(q['id'], len(q['query']),len(expanded_query), q['runtime'], q['tables'], q['columns'], q['expressions'], q['log_ops'], len(set(total_ref_views)), q['query'].replace(',', '!')))
		f.write("%d, %d, %d, %d, %d, %d, %d, %d, %d\n"%(q['id'], len(q['query']),len(expanded_query), q['runtime'], q['tables'], q['columns'], q['expressions'], q['log_ops'], len(set(total_ref_views))))
	f.close()
	f2.close()

def main():
	arguments = docopt(__doc__, version='SDSS Tools 0.0.1')

	db = (('-d' in arguments and arguments['-d']) or 'sqlite:///test.sqlite')
	getmetrics(db)

if __name__ == '__main__':
	main()