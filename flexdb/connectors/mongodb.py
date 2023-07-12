from .base import DatabaseConnector
from pymongo import MongoClient


class MongoDbConnector(DatabaseConnector):
    def connect(self):
        self.connection = MongoClient(**self.config)
    
    def close(self):
        self.connection.close()