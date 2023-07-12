from .base import DatabaseConnector
from pymongo import MongoClient


class MongoDbConnector(DatabaseConnector):
    def connect(self):
        self.connection = MongoClient(**self.config)
    
    def close(self):
        self.connection.close()

    def create(self, collection, data):
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.insert_one(data)
        return result.inserted_id

    def read(self, collection, filters):
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.find(filters)
        return list(result)

    def update(self, collection, filters, data):
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.update_one(filters, {'$set': data})
        return result.modified_count

    def delete(self, collection, filters):
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.delete_one(filters)
        return result.deleted_count