import unittest
import yaml
from flexdb.connectors.mssql import MsSQLConnector
import os


class TestMsSQLConnector(unittest.TestCase):
    
    def test_connect(self):
        """Connection test"""
        config = {
            'host': os.environ.get("MSSQL_HOST"),
            'port': 1433,
            'dbname': os.environ.get("MSSQL_DB"),
            'user': os.environ.get("MSSQL_USER"),
            'password': os.environ.get("MSSQL_PASSWORD")
        }
        
        connector = MsSQLConnector(config)
        self.assertIsNone(connector.connect())
    
    def test_create(self):
        """Create test"""
        config = {
            'host': os.environ.get("MSSQL_HOST"),
            'port': 1433,
            'dbname': os.environ.get("MSSQL_DB"),
            'user': os.environ.get("MSSQL_USER"),
            'password': os.environ.get("MSSQL_PASSWORD")
        }
        
        connector = MsSQLConnector(config)
        connector.connect()
        connector.create('test_table', {'column1': 'value1', 'column2': 'value2'})
        connector.close()
    
    def test_read(self):
        """Read test"""
        config = {
            'host': os.environ.get("MSSQL_HOST"),
            'port': 1433,
            'dbname': os.environ.get("MSSQL_DB"),
            'user': os.environ.get("MSSQL_USER"),
            'password': os.environ.get("MSSQL_PASSWORD")
        }
        
        connector = MsSQLConnector(config)
        connector.connect()
        result = connector.read('test_table', {'column1': 'value1'})
        connector.close()
        self.assertEqual(result, [('value1', 'value2')])
    
    def test_update(self):
        """Update test"""
        config = {
            'host': os.environ.get("MSSQL_HOST"),
            'port': 1433,
            'dbname': os.environ.get("MSSQL_DB"),
            'user': os.environ.get("MSSQL_USER"),
            'password': os.environ.get("MSSQL_PASSWORD")
        }
        
        connector = MsSQLConnector(config)
        connector.connect()
        connector.update('test_table', {'column1': 'value1'}, {'column1': 'value3'})
        connector.close()
    
    def test_delete(self):
        """Delete test"""
        config = {
            'host': os.environ.get("MSSQL_HOST"),
            'port': 1433,
            'dbname': os.environ.get("MSSQL_DB"),
            'user': os.environ.get("MSSQL_USER"),
            'password': os.environ.get("MSSQL_PASSWORD")
        }
        
        connector = MsSQLConnector(config)
        connector.connect()
        connector.delete('test_table', {'column1': 'value3'})
        connector.close()
    
    def test_close(self):
        """Close test"""
        config = {
            'host': os.environ.get("MSSQL_HOST"),
            'port': 1433,
            'dbname': os.environ.get("MSSQL_DB"),
            'user': os.environ.get("MSSQL_USER"),
            'password': os.environ.get("MSSQL_PASSWORD")
        }
        
        connector = MsSQLConnector(config)
        connector.connect()
        connector.close()
        self.assertIsNone(connector.connection)