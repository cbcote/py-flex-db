from .base import DatabaseConnector
import mysql.connector


class MySQLConnector(DatabaseConnector):
    def connect(self):
        """Connects to the database using the specified configuration."""
        self.connection = mysql.connector.connect(**self.config)
    
    def close(self):
        """Closes the connection to the database."""
        self.connection.close()
    
    def create(self, table, data):
        """
        Inserts a new record into the specified table.
        
        Parameters
        ----------
        table : str
            Name of the table to insert into.
        data : dict
            Dictionary of column names and values to insert.
        """
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data)) # %s is a placeholder for a value
        insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})' # INSERT INTO table (column1, column2) VALUES (%s, %s)
        cursor.execute(insert_query, list(data.values()))
        self.connection.commit()
        cursor.close()
    
    def read(self, table, filters):
        """
        Reads records from the specified table.
        
        Parameters
        ----------
        table : str
            Name of the table to read from.
        filters : dict
            Dictionary of column names and values to filter the results by.
        
        Returns
        -------
        results : list
            List of records from the specified table.
        """
        cursor = self.connection.cursor()
        columns = ', '.join(filters.keys())
        placeholders = ', '.join(['%s'] * len(filters)) # %s is a placeholder for a value
        select_query = f'SELECT {columns} FROM {table} WHERE {placeholders}' # SELECT column1, column2 FROM table WHERE column1 = %s AND column2 = %s
        cursor.execute(select_query, list(filters.values()))
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def update(self, table, filters, data):
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()]) # column1 = %s AND column2 = %s
        update_query = f'UPDATE {table} SET {columns} = {placeholders} WHERE {conditions}' # UPDATE table SET column1 = %s, column2 = %s WHERE column1 = %s AND column2 = %s
        cursor.execute(update_query, list(data.values()) + list(filters.values()))
        self.connection.commit()
        cursor.close()
        
    def delete(self, table, filters):
        cursor = self.connection.cursor()
        conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()]) # column1 = %s AND column2 = %s
        delete_query = f'DELETE FROM {table} WHERE {conditions}' # DELETE FROM table WHERE column1 = %s AND column2 = %s
        cursor.execute(delete_query, list(filters.values()))
        self.connection.commit()
        cursor.close()
        