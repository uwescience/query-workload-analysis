"""QWLA join similarity tool.

Usage:
    analyse2 [-d DATABASE]
    analyse2  (-h | --help)
    analyse2  --version

Options:
    -d [DATABASE]  The database to read from or write into
    -h --help      Show this screen.
    --version      Show version.

"For a moment, nothing happened. Then, after a second or so,
nothing continued to happen."
"""

from docopt import docopt
import math
import numpy as np
import dataset
import sqlalchemy as sa
from datetime import datetime
from collections import Counter
import re


def analyse2(database):
    db = dataset.connect(database);

    # The following code calculates dataset sharing.
    ref_view_q = "select id, owner, view, ref_views from sqlshare_logs where isview = 0 and ref_views != ''"

    datasets = db.query(ref_view_q)
    shared_datasets = 0
    for ds in datasets:
        referenced_views = [x for x in ds['ref_views'].split(',') if x != '']
        for view in referenced_views:
            result = list(db.query('SELECT id,owner from sqlshare_logs where id = {}'.format(view)))
            if ds['owner'] != result[0]['owner']:
                shared_datasets += 1
                break
    print shared_datasets

    # # THIS PART OF THE CODE CALCULATES DATASET LIFETIME
    query = "select distinct(t.\"table\") from sqlshare_tables t, sqlshare_logs l where l.id = t.query_id and t.\"table\" != 'None' and l.owner = '%s'"

    owners = []
    top_owners = db.query('select owner from sqlshare_logs group by owner order by count(*) desc limit 20')
    for result in top_owners:
        owners.append(result['owner'])

    for owner in owners:
        tables = list(db.query(query % owner))
        lifetime = {}
        ecount = 0;
        for i, t in enumerate(tables):
            try:
                timestamps = list(db.query(
                    'select time_start from sqlshare_logs where id in (select query_id from sqlshare_tables where "table" = "' +
                    t['table'] + '") order by strftime(time_start, \'MM/DD/YYYY HH12:MI:SS am\') desc'
                ))
                if len(timestamps) <= 1:
                    lifetime[i] = 0
                else:
                    start = datetime.strptime(timestamps[-1]['time_start'], "%m/%d/%Y %I:%M:%S %p")
                    end = datetime.strptime(timestamps[0]['time_start'], "%m/%d/%Y %I:%M:%S %p")
                    lifetime[i] = (end - start).days
            except Exception as e:
                print e
                ecount += 1
                pass
        print 'done with ' + str(ecount) + ' errors'

        def write_to_csv(dict_obj, col1, col2, filename, to_reverse=True):
            f = open(filename, 'w')
            f.write("%s,%s\n" % (col1, col2))
            for key in sorted(dict_obj, reverse=to_reverse):
                f.write("%s,%s\n" % (key, dict_obj[key]))
            f.close()

        write_to_csv(lifetime, 'table_id', 'lifetime', '../results/sqlshare/' + owner + 'query_lifetime.csv')

    # THIS PART OF THE CODE CALCULATES TABLE TOUCH and COLUMN TOUCH.

    # p = re.compile(ur'^.*[A-F0-9]{5}$')
    # tables_ref_q = "select distinct(\"table\") from sqlshare_tables where query_id = '%s'"
    # distinct_query_ids_q = 'select distinct(query_id) from sqlshare_tables'
    # distinct_query_ids = list(db.query(distinct_query_ids_q))
    # table_touch = []
    # for q in distinct_query_ids:
    #     tables_ref = list(db.query(tables_ref_q % q['query_id']))
    #     logical_tables = []
    #     for t in tables_ref:
    #         short_name = re.findall(p, t['table'])
    #         if len(short_name) == 0:
    #             if t['table'] not in logical_tables:
    #                 logical_tables.append(t['table'])
    #         else:
    #             if short_name[0][0:-5] not in logical_tables:
    #                 logical_tables.append(short_name[0][0:-5])
    #     # print(logical_tables)
    #     table_touch.append(len(logical_tables))
    #
    # print np.mean(table_touch)
    #
    # columns_ref_q = "select \"table\", \"column\" from sqlshare_columns where query_id = '%s' group by \"table\", \"column\" "
    # distinct_query_ids_q = 'select distinct(query_id) from sqlshare_tables'
    # distinct_query_ids = list(db.query(distinct_query_ids_q))
    # column_touch = []
    # for q in distinct_query_ids:
    #     columns_ref = list(db.query(columns_ref_q % q['query_id']))
    #     logical_columns = []
    #     for t in columns_ref:
    #         short_name = re.findall(p, t['table'])
    #         if len(short_name) == 0:
    #             if t['column'] not in logical_columns:
    #                 logical_columns.append(t['column'])
    #         else:
    #             short_col_name = t['column'].replace(short_name[0], short_name[0][0:-5])
    #             if short_col_name not in logical_columns:
    #                 logical_columns.append(short_col_name)
    #     # print(logical_columns)
    #     column_touch.append(len(logical_columns))
    #
    # print np.mean(column_touch)


def main():
    arguments = docopt(__doc__, version='SDSS Tools 0.0.1')

    db = (('-d' in arguments and arguments['-d']) or 'sqlite:///sqlshare-sdss.sqlite')
    analyse2(db)


if __name__ == '__main__':
    main()
