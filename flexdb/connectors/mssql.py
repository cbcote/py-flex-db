from .base import DatabaseConnector
import pyodbc


class MsSQLConnector(DatabaseConnector):
    def connect(self):
        self.connection = pyodbc.connect(**self.config)
    
    def close(self):
        self.connection.close()

    def create(self, table, data):
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
        cursor.execute(insert_query, list(data.values()))
        self.connection.commit()
        cursor.close()
    
    def read(self, table, filters):
        cursor = self.connection.cursor()
        columns = ', '.join(filters.keys())
        placeholders = ', '.join(['?'] * len(filters))
        select_query = f'SELECT {columns} FROM {table} WHERE {placeholders}'
        cursor.execute(select_query, list(filters.values()))
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def update(self, table, filters, data):
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        conditions = ' AND '.join([f'{key} = ?' for key in filters.keys()])
        update_query = f'UPDATE {table} SET {columns} = {placeholders} WHERE {conditions}'
        cursor.execute(update_query, list(data.values()) + list(filters.values()))
        self.connection.commit()
        cursor.close()
        
    def delete(self, table, filters):
        cursor = self.connection.cursor()
        conditions = ' AND '.join([f'{key} = ?' for key in filters.keys()])
        delete_query = f'DELETE FROM {table} WHERE {conditions}'
        cursor.execute(delete_query, list(filters.values()))
        self.connection.commit()
        cursor.close()