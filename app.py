import collections.abc


class Repository(object):
    """
    this is like a fake DB
    """

    def __init__(self, data):
        self.data = {**data}

    def find(self):
        return self.data.items()

    def findOne(self, x):
        print(f'I am value of {x}')
        return self.data[x]

    def deleteOne(self, x):
        del self.data[x]
        return f'Deleted {x}'

    def updateOne(self, k, v):
        self.data.update({k: v})
        return f'{k} updated to {self.data[k]}'


class MongoDict(collections.abc.MutableMapping):
    def __len__(self):
        print('I am length')
        return len(self.data)

    def __iter__(self):
        print('I am iter')
        return iter(map(lambda x: x[0], self.data))

    def __getitem__(self, key):
        print(f'Fetching key {key}')
        return self.repository.findOne(key)

    def __delitem__(self, key):
        print(f'Deleting key {key}')
        return self.repository.deleteOne(key)

    def __setitem__(self, key, value):
        print(f'Setting {key} to {value}')
        self.repository.updateOne(key, value)

    def __init__(self):
        self.repository = Repository({'a': 1, 'b': 2})
        self.data = self.repository.find()


if __name__ == "__main__":
    m = MongoDict()
