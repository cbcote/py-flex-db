import unittest
import yaml
from flexdb.azure.access_token.postgresql import get_access_token
from flexdb.connectors.postgresql import PostgreSQLConnector
import os


class TestPostgreSQLConnector(unittest.TestCase):
    
    def test_connect(self):
        script_dir = os.path.dirname(__file__)

        yaml_path = os.path.join(script_dir, '../flexdb/config/config.yaml')
        
        with open(yaml_path, 'r') as f:
            config = yaml.safe_load(f)
            postgres_config = config['postgresql']
            postgres_config['password'] = get_access_token()
        
        connector = PostgreSQLConnector(postgres_config)
        self.assertIsNone(connector.connect())  # Replace with an actual test
