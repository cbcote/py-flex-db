from .base import DatabaseConnector
import pyodbc
from typing import List, Dict, Union


class MsSQLConnector(DatabaseConnector):
    def connect(self):
        """Connects to the database using the specified configuration."""
        self.connection = pyodbc.connect(**self.config)
    
    def close(self):
        """Closes the connection to the database."""
        self.connection.close()

    def create(self, table: str, data: Dict) -> None:
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
        placeholders = ', '.join(['?'] * len(data))
        insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
        cursor.execute(insert_query, list(data.values()))
        self.connection.commit()
        cursor.close()
    
    def read(self, table: str, filters: Dict) -> List:
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
        placeholders = ', '.join(['?'] * len(filters))
        select_query = f'SELECT {columns} FROM {table} WHERE {placeholders}'
        cursor.execute(select_query, list(filters.values()))
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def update(self, table: str, filters: Dict, data: Dict) -> None:
        """
        Updates records in the specified table.
        
        Parameters
        ----------
        table : str
            Name of the table to update.
        filters : dict
            Dictionary of column names and values to filter the records by.
        data : dict
            Dictionary of column names and values to update.
        """
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?'] * len(data))
        conditions = ' AND '.join([f'{key} = ?' for key in filters.keys()])
        update_query = f'UPDATE {table} SET {columns} = {placeholders} WHERE {conditions}'
        cursor.execute(update_query, list(data.values()) + list(filters.values()))
        self.connection.commit()
        cursor.close()
        
    def delete(self, table: str, filters: Dict) -> None:
        """
        Deletes records from the specified table.
        
        Parameters
        ----------
        table : str
            Name of the table to delete from.
        filters : dict
            Dictionary of column names and values to filter the records by.
        """
        cursor = self.connection.cursor()
        conditions = ' AND '.join([f'{key} = ?' for key in filters.keys()])
        delete_query = f'DELETE FROM {table} WHERE {conditions}'
        cursor.execute(delete_query, list(filters.values()))
        self.connection.commit()
        cursor.close()