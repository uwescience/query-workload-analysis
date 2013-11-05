import dataset


def consume(database, files):
    print database, files
    db = dataset.connect(database)

    table = db['sometable']
    table.insert(dict(name='John Doe', age=37))
    table.insert(dict(name='Jane Doe', age=34, gender='female'))

    john = table.find_one(name='John Doe')
    print john
