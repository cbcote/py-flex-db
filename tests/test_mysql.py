import unittest
import yaml
from flexdb.connectors.mysql import MySQLConnector
import os


class MySQLConnector(unittest.TestCase):
    
    def test_connect(self):
        """Connection test"""
        config = {
            'host': os.environ.get("MYSQL_HOST"),
            'port': 3306,
            'dbname': os.environ.get("MYSQL_DB"),
            'user': os.environ.get("MYSQL_USER"),
            'password': os.environ.get("MYSQL_PASSWORD")
        }
        
        connector = MySQLConnector(config)
        self.assertIsNone(connector.connect())
    
    def test_create(self):
        """Create test"""
        config = {
            'host': os.environ.get("MYSQL_HOST"),
            'port': 3306,
            'dbname': os.environ.get("MYSQL_DB"),
            'user': os.environ.get("MYSQL_USER"),
            'password': os.environ.get("MYSQL_PASSWORD")
        }
        
        connector = MySQLConnector(config)
        connector.connect()
        connector.create('test_table', {'column1': 'value1', 'column2': 'value2'})
        connector.close()
    
    def test_read(self):
        """Read test"""
        config = {
            'host': os.environ.get("MYSQL_HOST"),
            'port': 3306,
            'dbname': os.environ.get("MYSQL_DB"),
            'user': os.environ.get("MYSQL_USER"),
            'password': os.environ.get("MYSQL_PASSWORD")
        }
        
        connector = MySQLConnector(config)
        connector.connect()
        result = connector.read('test_table', {'column1': 'value1'})
        connector.close()
        self.assertEqual(result, [('value1', 'value2')])
    
    def test_update(self):
        """Update test"""
        config = {
            'host': os.environ.get("MYSQL_HOST"),
            'port': 3306,
            'dbname': os.environ.get("MYSQL_DB"),
            'user': os.environ.get("MYSQL_USER"),
            'password': os.environ.get("MYSQL_PASSWORD")
        }
        
        connector = MySQLConnector(config)
        connector.connect()
        connector.update('test_table', {'column1': 'value1'}, {'column1': 'value3'})
        result = connector.read('test_table', {'column1': 'value3'})
        connector.close()
        self.assertEqual(result, [('value3', 'value2')])
    
    def test_delete(self):
        """Delete test"""
        config = {
            'host': os.environ.get("MYSQL_HOST"),
            'port': 3306,
            'dbname': os.environ.get("MYSQL_DB"),
            'user': os.environ.get("MYSQL_USER"),
            'password': os.environ.get("MYSQL_PASSWORD")
        }
        
        connector = MySQLConnector(config)
        connector.connect()
        connector.delete('test_table', {'column1': 'value3'})
        result = connector.read('test_table', {'column1': 'value3'})
        connector.close()
        self.assertEqual(result, [])
    
    def test_close(self):
        """Close test"""
        config = {
            'host': os.environ.get("MYSQL_HOST"),
            'port': 3306,
            'dbname': os.environ.get("MYSQL_DB"),
            'user': os.environ.get("MYSQL_USER"),
            'password': os.environ.get("MYSQL_PASSWORD")
        }
        
        connector = MySQLConnector(config)
        connector.connect()
        connector.close()
        self.assertIsNone(connector.connection)