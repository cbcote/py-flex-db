import unittest
import yaml
from flexdb.connectors.mongodb import MongoDBConnector
import os


class TestMongoDbConnector(unittest.TestCase):
    
    def test_connect(self):
        """Connection test"""
        config = {
            'host': os.environ.get("MONGODB_HOST"),
            'port': 27017,
            'dbname': os.environ.get("MONGODB_DB"),
            'user': os.environ.get("MONGODB_USER"),
            'password': os.environ.get("MONGODB_PASSWORD")
        }
        
        connector = MongoDBConnector(config)
        self.assertIsNone(connector.connect())
    
    def test_create(self):
        """Create test"""
        config = {
            'host': os.environ.get("MONGODB_HOST"),
            'port': 27017,
            'dbname': os.environ.get("MONGODB_DB"),
            'user': os.environ.get("MONGODB_USER"),
            'password': os.environ.get("MONGODB_PASSWORD")
        }
        
        connector = MongoDBConnector(config)
        connector.connect()
        connector.create('test_table', {'column1': 'value1', 'column2': 'value2'})
        connector.close()
    
    def test_read(self):
        """Read test"""
        config = {
            'host': os.environ.get("MONGODB_HOST"),
            'port': 27017,
            'dbname': os.environ.get("MONGODB_DB"),
            'user': os.environ.get("MONGODB_USER"),
            'password': os.environ.get("MONGODB_PASSWORD")
        }
        
        connector = MongoDBConnector(config)
        connector.connect()
        result = connector.read('test_table', {'column1': 'value1'})
        connector.close()
        self.assertEqual(result, [{'_id': 'value1', 'column2': 'value2'}])
    
    def test_update(self):
        """Update test"""
        config = {
            'host': os.environ.get("MONGODB_HOST"),
            'port': 27017,
            'dbname': os.environ.get("MONGODB_DB"),
            'user': os.environ.get("MONGODB_USER"),
            'password': os.environ.get("MONGODB_PASSWORD")
        }
        
        connector = MongoDBConnector(config)
        connector.connect()
        connector.update('test_table', {'column1': 'value1'}, {'column2': 'value3'})
        connector.close()
        result = connector.read('test_table', {'column1': 'value1'})
        self.assertEqual(result, [{'_id': 'value1', 'column2': 'value3'}])
    
    def test_delete(self):
        """Delete test"""
        config = {
            'host': os.environ.get("MONGODB_HOST"),
            'port': 27017,
            'dbname': os.environ.get("MONGODB_DB"),
            'user': os.environ.get("MONGODB_USER"),
            'password': os.environ.get("MONGODB_PASSWORD")
        }
        
        connector = MongoDBConnector(config)
        connector.connect()
        connector.delete('test_table', {'column1': 'value1'})
        connector.close()
        result = connector.read('test_table', {'column1': 'value1'})
        self.assertEqual(result, [])
    
    def test_close(self):
        """Close test"""
        config = {
            'host': os.environ.get("MONGODB_HOST"),
            'port': 27017,
            'dbname': os.environ.get("MONGODB_DB"),
            'user': os.environ.get("MONGODB_USER"),
            'password': os.environ.get("MONGODB_PASSWORD")
        }
        
        connector = MongoDBConnector(config)
        connector.connect()
        connector.close()
        self.assertIsNone(connector.client)