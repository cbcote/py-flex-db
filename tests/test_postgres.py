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
