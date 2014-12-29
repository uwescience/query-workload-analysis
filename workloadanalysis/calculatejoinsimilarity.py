"""QWLA join similarity tool.

Usage:
    common_column_names [-d DATABASE]
    common_column_names  (-h | --help)
    common_column_names  --version

Options:
    -d [DATABASE]  The database to read from or write into
    -h --help      Show this screen.
    --version      Show version.

"For a moment, nothing happened. Then, after a second or so,
nothing continued to happen."
"""

from docopt import docopt
import math
import dataset
import sqlalchemy as sa
from collections import Counter

def calculate(database):
	db = dataset.connect(database);

	tables = list(db.query(
		'SELECT username, tablename, string_agg(columnname, \',\') AS columns FROM sqlshare_columns_all GROUP BY username, tablename;'
		))
	similarity = []
	for i in range(len(tables)):
		similarityi = []
		for j in range(len(tables)):
			if i==j:
				similarityi.append(1)
				continue
			tablei = tables[i]
			tablej = tables[j]
			col_i = tablei['columns'].split(',')
			col_j = tablej['columns'].split(',')
			similarityi.append(counter_cosine_similarity(Counter(col_i), Counter(col_j)))
		
		for j, similarityi_j in enumerate(similarityi):
			if similarityi_j != 0:
				similarity.append([i,j,similarityi_j]);
	for data_point in similarity:
		print data_point[0] + " " + data_point[1] + " " + data_point[2] + "\n"

def counter_cosine_similarity(c1, c2):
	# Calculates cosine similarity of two counters. 
	# is cosine similarity the right measure?

	terms = set(c1).union(c2)
	dotprod = sum(c1.get(k, 0) * c2.get(k, 0) for k in terms)
	magA = math.sqrt(sum(c1.get(k, 0)**2 for k in terms))
	magB = math.sqrt(sum(c2.get(k, 0)**2 for k in terms))
	return dotprod / (magA * magB)

def main():
	arguments = docopt(__doc__, version='SDSS Tools 0.0.1')

	db = (('-d' in arguments and arguments['-d']) or 'sqlite:///test.sqlite')
	calculate(db)

if __name__ == '__main__':
	main()