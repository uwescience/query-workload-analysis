import dataset


def main():
    db = dataset.connect('sqlite:///:memory:')

    table = db['sometable']
    table.insert(dict(name='John Doe', age=37))
    table.insert(dict(name='Jane Doe', age=34, gender='female'))

    john = table.find_one(name='John Doe')
    print john

if __name__ == '__main__':
    main()
