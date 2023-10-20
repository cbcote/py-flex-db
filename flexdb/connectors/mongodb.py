from .base import DatabaseConnector
from pymongo import MongoClient


class MongoDbConnector(DatabaseConnector):
    def connect(self):
        self.connection = MongoClient(**self.config)
    
    def close(self):
        self.connection.close()

    def create(self, collection, data):
        """
        :param collection: collection name
        :param data: dict of data
        :return: id of inserted document
        
        Example:
        >>> db = MongoDbConnector()
        >>> db.connect()
        >>> db.create('users', {'name': 'John', 'age': 25})
        ObjectId('5f1b5e5a2c9b0e5c3e4f3e5f')
        """
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.insert_one(data)
        return result.inserted_id

    def read(self, collection, filters):
        """
        :param collection: collection name
        :param filters: dict of filters
        :return: list of documents
        
        Example:
        >>> db = MongoDbConnector()
        >>> db.connect()
        >>> db.read('users', {'name': 'John'})
        [{'_id': ObjectId('5f1b5e5a2c9b0e5c3e4f3e5f'), 'name': 'John', 'age': 25}]
        """
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.find(filters)
        return list(result)

    def update(self, collection, filters, data):
        """
        :param collection: collection name
        :param filters: dict of filters
        :param data: dict of data
        :return: number of modified documents
        
        Example:
        >>> db = MongoDbConnector()
        >>> db.connect()
        >>> db.update('users', {'name': 'John'}, {'age': 26})
        """
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.update_one(filters, {'$set': data})
        return result.modified_count

    def delete(self, collection, filters):
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.delete_one(filters)
        return result.deleted_count