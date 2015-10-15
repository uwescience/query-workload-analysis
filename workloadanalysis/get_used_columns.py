import matplotlib.pyplot as plt
import dataset
from collections import Counter

db = dataset.connect('postgresql+psycopg2:///sqlsharelogs')
users_q = list(db.query('select distinct(owner) from sqlshare_logs'))
columns_q = list(db.query('select distinct("column") from sqlshare_columns'))
num_users = len(users_q)

# f = open('used_columns.txt', mode='w')
# f.write("\t".join(map(str, range(len(columns_q)))))

all_columns = dict()

for c in columns_q:
    all_columns[c['column'].encode('UTF-8')] = 0

data_matrix = []

for i in range(len(users_q)):
    data_matrix.append(dict(all_columns))

for i, user in enumerate(users_q):
    ref_columns = list(db.query('select distinct("column") from sqlshare_logs a, sqlshare_columns b where a.id = b.query_id and a.owner = \'{}\''.format(user['owner'].encode('UTF-8'))))
    for c in ref_columns:
        data_matrix[i][c['column'].encode('UTF-8')] = 1
    # print data_matrix[i].values()

data = [d.values() for d in data_matrix]

dense = sorted(data, key=lambda xs: len([x for x in xs if x]), reverse=False)

#matrix = np.array(random.sample(data,100))
matrix = dense
#print matrix

imgplot = plt.imshow(matrix, aspect='auto')

imgplot.set_cmap('binary')

plt.savefig("complete.png")

cols = dict()
for i, user in enumerate(users_q):
    for j, c in enumerate(all_columns.keys()):
        if c in cols:
            cols[c] += data[i][j]
        else:
            cols[c] = data[i][j]

for c in cols:
    if cols[c] > 200:
        print c, cols[c]
        # for i,u in enumerate(data_matrix):
        #     usersss = []
        #     if c in u.keys():
        #         usersss.append(users_q[i]['owner'])
        #     print usersss
# for i in range(len(data_matrix)):
#     f.write("\t".join(map(str, data_matrix[i].values())))
#
# f.close()