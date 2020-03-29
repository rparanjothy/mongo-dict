import collections.abc
import pymongo


class Repository(object):
    """
    Now connects to local mongo
    """

    def __init__(self, schema, collection, host=None):
        # create connection to Mongo for that schema
        client = pymongo.MongoClient('localhost')
        db = client[schema]
        self.collection = db[collection]

    def find(self, x=None):
        if isinstance(x, dict):
            print('incoming dict')
            return list(self.collection.find(x))
        elif isinstance(x, str):
            print('incoming str')
            return self.collection.find_one({'_id': x})
        else:
            return self.collection.find()

    def delete(self, x):
        if isinstance(x, str):
            result = self.collection.delete_one({'_id': x})
            print(f'Deleted {x}')
        elif isinstance(x, dict):
            result = self.collection.delete_many(x)
            print(f'Deleted {x}')
        return result

    def update(self, k, v):
        print(f'{k} updated to {v}')
        if isinstance(k, dict):
            print("dict")
            result = self.collection.update_many(
                k, {'$set': v}, upsert=True)
            print(result.raw_result, result.modified_count, result.upserted_id)
        elif isinstance(k, str):
            print("str")
            result = self.collection.update_one(
                {'_id': k}, {'$set': v}, upsert=True)
            print(result.raw_result, result.modified_count, result.upserted_id)

        else:
            result = {}
        return v


class MongoDict(collections.abc.MutableMapping):
    def __init__(self, schema, collection):
        self.repository = Repository(schema, collection)
        self.schema = schema
        self.collection = collection

    @property
    def data(self):
        return self.repository.find()

    def __len__(self):
        print('I am length')
        return len(self.data)

    def __iter__(self):
        print('I am iter')
        return map(lambda x: x["_id"], self.data)

    def __getitem__(self, key):
        print(f'Fetching key {key}')
        return self.repository.find(key)

    def __delitem__(self, key):
        print(f'Deleting key {key}')
        return self.repository.delete(key)

    def __setitem__(self, key, value):
        print(f'Setting {key} to {value}')
        self.repository.update(key, value)

    def __repr__(self):
        return f'{self.schema}:{self.collection} - {len(list(self.repository.find()))} documents'

    def iterData(self):
        for x in self.repository.find():
            yield x


if __name__ == "__main__":
    m = MongoDict('cars', 'cars')
    c = MongoDict('cars', 'mycars')
