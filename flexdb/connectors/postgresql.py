from .base import DatabaseConnector
import psycopg2

class PostgreSQLConnector(DatabaseConnector):
    def connect(self):
        self.connection = psycopg2.connect(**self.config)
    
    def close(self):
        self.connection.close()