from .base import DatabaseConnector
import pyodbc


class MsSQLConnector(DatabaseConnector):
    def connect(self):
        self.connection = pyodbc.connect(**self.config)
    
    def close(self):
        self.connection.close()