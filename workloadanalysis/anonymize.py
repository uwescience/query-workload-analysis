from datetime import datetime
import hashlib
import dataset

def pretty_query(query):
    return query.strip().strip("")

def anonymize(files):
    owners = dict()
    for fname,isview in files:
        f = open(fname)
        f.readline()  # remove header
        print 'Getting owners for:', fname
        for line in f:
            row = line.split('|')
            if isview:
                owners[row[0].split(']')[0][1:]] = hashlib.sha256(row[0].split(']')[0][1:]).hexdigest()
            else:
                owners[row[1]] = hashlib.sha256(row[1]).hexdigest()
        f.close()
    # checking for hash distinct
    assert len(owners.keys()) == len(set(owners.values())) 
    hash_file = open('user_hashes.csv', 'w')
    sorted_keys = owners.keys()
    sorted_keys.sort(reverse = True)
    non_email_keys = [x for x in sorted_keys if '@' not in x]
    sorted_keys = [x for x in sorted_keys if '@' in x]
    print non_email_keys

    for key in sorted_keys:
        hash_file.write("{}, {}\n".format(key, owners[key]))
    for key in non_email_keys:
        hash_file.write("{}, {}\n".format(key, owners[key]))
    hash_file.close()
    for fname,isview in files:
        count = 1
        w = open('Anonymized_%s.csv'%str(isview),'w')
        f = open(fname)
        f.readline()
        fail_count = 0
        for line in f:
            # print 'Processing Query: ', count
            count += 1
            try:
                for key in sorted_keys:
                    line = line.replace(key, owners[key])
                for key in non_email_keys:
                    line = line.replace(key, owners[key])
                w.write(line + '\n')
                if count%100 == 0:
                    w.flush()
            except Exception:
                print 'Exception...'
                fail_count += 1
        f.close()
        w.close()
        print fail_count

if __name__ == '__main__':
    anonymize([('QueriesWithPlan.csv', False),('ViewsWithPlan.csv', True)])
