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
    for filename in files:
        with open(directory + "/" + filename) as f:
            qs = f.read().split('\n\n')
            qs = [re.sub('\s+', ' ', q).strip() for q in qs if len(re.sub('\s+', ' ', q).strip()) and not q.strip().startswith("--")]
            queries.extend(qs)

    return queries


if __name__ == '__main__':
    print len(get_queries())
