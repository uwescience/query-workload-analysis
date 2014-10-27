import os
import re


def get_queries():
    # get the files
    files = []

    directory = os.path.dirname(os.path.realpath(__file__)) + "/../tpch_queries"

    for (dirpath, dirnames, filenames) in os.walk(directory):
        files.extend(filenames)
        break

    queries = []
    id = 0
    for filename in files:
        with open(directory + "/" + filename) as f:
            qs = f.read().split('\n\n')
            qs = [re.sub('\s+', ' ', q).strip().strip("go").strip().strip(";").strip() for q in qs]
            qs = [q for q in qs if len(q) and not q.strip().startswith("--")]
            queries.extend(qs)
            for tpc_query, query in enumerate(qs):
                queries.append({
                    "tp_query": tpc_query,  # note: number 15 is not used, hence the numbers are off
                    "id": id
                })
                id += 1

    return queries


if __name__ == '__main__':
    print get_queries()
