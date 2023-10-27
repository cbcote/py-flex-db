import unittest
import yaml
from flexdb.azure.access_token.postgresql import get_access_token
from flexdb.connectors.postgresql import PostgreSQLConnector
import os




class TestPostgreSQLConnector(unittest.TestCase):
    
    def test_connect(self):
        """Connection test"""
        config = {
            'host': os.environ.get("POSTGRES_HOST"),
            'port': 5432,
            'dbname': os.environ.get("POSTGRES_DB"),
            'user': os.environ.get("POSTGRES_USER"),
            'password': get_access_token()
        }
        
        connector = PostgreSQLConnector(config)
        self.assertIsNone(connector.connect())
    
    def test_create(self):
        """Create test"""
        config = {
            'host': os.environ.get("POSTGRES_HOST"),
            'port': 5432,
            'dbname': os.environ.get("POSTGRES_DB"),
            'user': os.environ.get("POSTGRES_USER"),
            'password': get_access_token()
        }
        
        connector = PostgreSQLConnector(config)
        connector.connect()
        connector.create('test_table', {'column1': 'value1', 'column2': 'value2'})
        connector.close()
    
    def test_read(self):
        """Read test"""
        config = {
            'host': os.environ.get("POSTGRES_HOST"),
            'port': 5432,
            'dbname': os.environ.get("POSTGRES_DB"),
            'user': os.environ.get("POSTGRES_USER"),
            'password': get_access_token()
        }
        
        connector = PostgreSQLConnector(config)
        connector.connect()
        result = connector.read('test_table', {'column1': 'value1'})
        connector.close()
        self.assertEqual(result, [('value1', 'value2')])
    
    def test_update(self):
        """Update test"""
        config = {
            'host': os.environ.get("POSTGRES_HOST"),
            'port': 5432,
            'dbname': os.environ.get("POSTGRES_DB"),
            'user': os.environ.get("POSTGRES_USER"),
            'password': get_access_token()
        }
        
        connector = PostgreSQLConnector(config)
        connector.connect()
        connector.update('test_table', {'column1': 'value1'}, {'column2': 'value3'})
        result = connector.read('test_table', {'column1': 'value1'})
        connector.close()
        self.assertEqual(result, [('value1', 'value3')])
    
    def test_delete(self):
        """Delete test"""
        config = {
            'host': os.environ.get("POSTGRES_HOST"),
            'port': 5432,
            'dbname': os.environ.get("POSTGRES_DB"),
            'user': os.environ.get("POSTGRES_USER"),
            'password': get_access_token()
        }
        
        connector = PostgreSQLConnector(config)
        connector.connect()
        connector.delete('test_table', {'column1': 'value1'})
        result = connector.read('test_table', {'column1': 'value1'})
        connector.close()
        self.assertEqual(result, [])
    
    def test_close(self):
        """Close test"""
        config = {
            'host': os.environ.get("POSTGRES_HOST"),
            'port': 5432,
            'dbname': os.environ.get("POSTGRES_DB"),
            'user': os.environ.get("POSTGRES_USER"),
            'password': get_access_token()
        }
        
        connector = PostgreSQLConnector(config)
        connector.connect()
        self.assertIsNone(connector.close())
