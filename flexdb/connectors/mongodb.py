from .base import DatabaseConnector
from pymongo import MongoClient
from typing import List, Dict, Union


class MongoDbConnector(DatabaseConnector):
    def connect(self):
        """Connects to the database using the specified configuration."""
        self.connection = MongoClient(**self.config)
    
    def close(self):
        """Closes the connection to the database."""
        self.connection.close()

    def create(self, collection, data) -> str:
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

    def read(self, collection, filters: Dict[str, Union[str, int]]) -> List[Dict[str, Union[str, int]]]:
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
        result = coll.find(filters) # you can also use coll.find_one(filters) to get only one document
        return list(result)

    def update(self, collection, filters: Dict, data: Dict) -> int:
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

    def delete(self, collection, filters: Dict) -> int:
        """
        :param collection: collection name
        :param filters: dict of filters
        :return: number of deleted documents
        
        Example:
        >>> db = MongoDbConnector()
        >>> db.connect()
        >>> db.delete('users', {'name': 'John'})
        """
        db = self.connection['mydatabase']  # replace 'mydatabase' with your db name
        coll = db[collection]
        result = coll.delete_one(filters) # you can also use coll.delete_many(filters) to delete multiple documents
        return result.deleted_count