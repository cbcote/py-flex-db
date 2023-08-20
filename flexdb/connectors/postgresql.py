from flexdb.connectors.base import DatabaseConnector
import psycopg2


class PostgreSQLConnector(DatabaseConnector):
    def connect(self):
        self.connection = psycopg2.connect(**self.config)
    
    def close(self):
        self.connection.close()

    def create(self, table, data):
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
        cursor.execute(insert_query, list(data.values()))
        self.connection.commit()
        cursor.close()
    
    def read(self, table, filters, select_columns=None):
        cursor = self.connection.cursor()
        
        # If select_columns is None or empty, select all columns using '*'
        if not select_columns:
            select_string = '*'
        else:
            select_string = ', '.join(select_columns)

        conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
        select_query = f'SELECT {select_string} FROM {table} WHERE {conditions}'
        cursor.execute(select_query, list(filters.values()))
        results = cursor.fetchall()
        cursor.close()
        return results

    
    def update(self, table, filters, data):
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
        update_query = f'UPDATE {table} SET {columns} = {placeholders} WHERE {conditions}'
        cursor.execute(update_query, list(data.values()) + list(filters.values()))
        self.connection.commit()
        cursor.close()
        
    def delete(self, table, filters):
        cursor = self.connection.cursor()
        conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
        delete_query = f'DELETE FROM {table} WHERE {conditions}'
        cursor.execute(delete_query, list(filters.values()))
        self.connection.commit()
        cursor.close()
