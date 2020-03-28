import collections.abc
import pymongo


class Repository(object):
    """
    Now connects to local mongo
    """

    def __init__(self, schema, collection):
        # create connection to Mongo for that schema
        client = pymongo.MongoClient()
        db = client[schema]
        self.collection = db[collection]

    def find(self):
        return self.collection.find()

    def findOne(self, x):
        return self.collection.find_one({'_id': x})

    def deleteOne(self, x):
        # del self.data[x]
        result = self.collection.delete_one({'_id': x})
        print(f'Deleted {x}')
        return result

    def updateOne(self, k, v):
        print(f'{k} updated to {v}')
        result = self.collection.update_one(
            {'_id': k}, {'$set': v}, upsert=True)
        print(result.raw_result, result.modified_count, result.upserted_id)
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
        return self.repository.findOne(key)

    def __delitem__(self, key):
        print(f'Deleting key {key}')
        return self.repository.deleteOne(key)

    def __setitem__(self, key, value):
        print(f'Setting {key} to {value}')
        self.repository.updateOne(key, value)

    def __repr__(self):
        return f'{self.schema}:{self.collection} - {len(list(self.repository.find()))} documents'


if __name__ == "__main__":
    m = MongoDict('cars', 'cars')
    c = MongoDict('cars', 'mycars')
