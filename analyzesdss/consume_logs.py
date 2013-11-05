import csv
import dataset

TYPES = {}


def consume(database, files):
    print database, files

    for logfile in files:
        db = dataset.connect(database)

        table = db['logs']

        rows = []
        with open(logfile, 'rb') as f:
            reader = csv.reader(f)

            # ignore header
            reader.next()

            for row in reader:
                data = {
                    'client': row[9],
                    'server': row[11],
                    'db': row[12],
                    'access': row[13],
                    'elapsed': float(row[14]),
                    'rows': int(row[16]),
                    'query': row[17],
                    'error': bool(int(row[18])),
                    'error_msg': row[19]
                }
                rows.append(data)

        table.insert_many(rows, types=TYPES)
