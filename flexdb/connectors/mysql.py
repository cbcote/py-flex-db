from .base import DatabaseConnector
import mysql.connector


class MySQLConnector(DatabaseConnector):
    def connect(self):
        self.connection = mysql.connector.connect(**self.config)
    
    def close(self):
        self.connection.close()