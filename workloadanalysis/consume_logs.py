from datetime import datetime

import unicodecsv as csv
import dataset


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
        if len(line) < 10:
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


# Deprecated method, this expects file to be in an older format.
def consume_sdss_old(db, f):
    table = db['everything']
    rows = []
    reader = csv.reader(csv_fixer(f), encoding='latin-1', quotechar='"')

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
        dateparts = [row[0], row[1], row[2], row[3], row[4], row[5]]
        dateparts = map(int, dateparts)
        try:
            data = {
                'time_start': datetime(*dateparts),
                'seq': row[6],
                'db': row[12],
                'access': row[13],
                'elapsed': float(row[14]),
                'rows': int(row[16]),
                'query': pretty_query(row[17]),
                'error': bool(int(row[18])),
                'error_msg': row[19]
            }
        except Exception as e:
            print "==> Skipping line with error"
            print e
            print row
        else:
            rows.append(data)

    table.insert_many(rows)

def consume_sdss(db, f):
    table = db['everything']
    rows = []
    f.readline()

    for line in f:
        row = line.split('||')

        try:
            data = {
                'time_start': row[0],
                'seq': row[1],
                'db': row[2],
                'access': row[3],
                'rows': int(row[4]),
                'query': pretty_query(row[5]).decode('utf8'),
                'xml': row[6].decode('utf8'),
                'has_plan': False
            }
        except Exception as e:
            print "==> Skipping line with error"
            print e
        else:
            rows.append(data)

    table.insert_many(rows)


def consume_sqlshare(db, f, isview):
    table = db['sqlshare_logs']
    count = 1
    f.readline()  # remove header
    id = 250000
    fail_count = 0
    for line in f:
        # if not isview:
        #     print 'Processing Query: ', count
        # else:
        #     print 'Processing View: ', count
        row = line.split('|')
        count += 1
        try:
            if isview:
                data = {
                    'id': id,
                    'time_start': row[2],
                    'time_end': 'NA',
                    'status': 'NA',
                    'view': row[0],
                    'query': pretty_query(row[1]).decode('utf8'),
                    'owner': row[0].split(']')[0][1:],
                    'length': len(row[4]),
                    'isview': True,
                    'xml_plan': row[5].decode('utf8'),
                    'runtime': -1,
                    'has_plan': False
                }
                id += 1
            else:
                data = {
                    'id': int(row[0]),
                    'owner': row[1],
                    'time_start': row[2],
                    'time_end': row[3],
                    'status': row[4],
                    'query': pretty_query(row[5]).decode('utf8'),
                    'length': int(row[6]),
                    'runtime': int(row[7]),
                    'xml_plan': row[8].decode('utf8'),
                    'isview': False,
                    'view': 'NA',
                    'has_plan': False
                }
            table.insert(data)
        except Exception as e:
            print e
            fail_count += 1
    print fail_count

    if isview:
        vf = open('ViewDepths.csv')
        table = db['sqlshare_view_depths']
        vf.readline() # Eat header
        c = 1
        for l in vf:
            rows = l.split('|')
            data = {
                'id': c,
                'owner': rows[0].decode('utf8'),
                'view': rows[1].decode('utf8'),
                'depth': int(rows[2].strip())
            }
            c +=1
            table.insert(data)


def consume(database, files, sdss, isview):
    """Import logs into database
    """
    db = dataset.connect(database)
    db.text_factory = str
    for i, logfile in enumerate(files):
        with open(logfile, 'rU') as f:
            if sdss:
                consume_sdss(db, f)
            else:
                consume_sqlshare(db, f, isview)
        print "Imported", i + 1, "of", len(files), "({})".format(f.name)
