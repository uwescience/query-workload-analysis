import unicodecsv as csv
import dataset

TYPES = {}


def csv_fixer(infile):
    """ Makes sure we can handle rows spanning multiple lines
    as the occur in the sdss query logs
    """
    pre = None
    for line in infile:
        line = line.replace("\\", "\\\\").strip()
        line = line.replace("\r\n", "\\r ")
        line = line.replace("\r", "\\r")
        if pre is None:
            pre = line
            continue
        if len(line) < 5:
            pre = pre + " " + line
            continue
        if line[0] == '2' and line[1] == '0' and (line[2] == '0' or line[2] == '1') and line[4] == ',':
            yield pre
            pre = line
            continue
        else:
            pre = pre + " " + line


def pretty_query(query):
    return query.strip().strip("")


def consume_sdss(db, f):
    table = db['logs']
    rows = []

    reader = csv.reader(csv_fixer(f), encoding='latin-1')

    for row in reader:
        # ignore header, if present
        if row[0] == 'yy':
            continue

        # fix lines that are too long
        if len(row) > 21:
            new_row = row[:18]
            new_row[-1] += ''.join(row[18:-3])
            new_row += row[-3:]
            row = new_row
        assert len(row) == 21, len(row)
        try:
            data = {
                'client': row[9],
                'server': row[11],
                'db': row[12],
                'access': row[13],
                'elapsed': float(row[14]),
                'rows': int(row[16]),
                'query': pretty_query(row[17]),
                'error': bool(int(row[18])),
                'error_msg': row[19]
            }
        except Exception as e:
            print row, e
            return
        rows.append(data)

    table.insert_many(rows, types=TYPES)

def consume_sqlshare(db,f):
    table = db['logs']
    rows = []

    reader = csv.reader(f, delimiter='\t',dialect=csv.excel)
    reader.next()

    for row in reader:
        try:
            data = {
                'id': int(row[0]),
                'owner': row[1],
                'time_start': row[2],
                'time_end': row[3],
                'status': row[4],
                'query': pretty_query(row[5]),
                'length': int(row[6]),
                'runtime': int(row[7]) 
            }
        except Exception as e:
            print row, e
            return
        rows.append(data)

    table.insert_many(rows, types=TYPES)

def consume(database, files, sdss):
    """Import logs into database
    """
    db = dataset.connect(database)
    for i, logfile in enumerate(files):
        with open(logfile, 'rb') as f:
            if sdss:
                consume_sdss(db, f)
            else:
                consume_sqlshare(db,f)
        print "Imported", i + 1, "of", len(files)
