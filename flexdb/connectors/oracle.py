from .base import DatabaseConnector
import oracledb


class OracleConnector(DatabaseConnector):
    
    def connect(self):
        """Connects to the database using the specified configuration."""
        self.connection = oracledb.connect(**self.config)
    
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
        placeholders = ', '.join([':{}'.format(i + 1) for i in range(len(data))])
        columns = ', '.join(data.keys())
        insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
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
        """
        cursor = self.connection.cursor()
        filter_str = ' AND '.join([f"{k} = :{i+1}" for i, k in enumerate(filters.keys())])
        select_query = f"SELECT * FROM {table} WHERE {filter_str}"
        cursor.execute(select_query, list(filters.values()))
        result = cursor.fetchall()
        cursor.close()
        return result

    def update(self, table, filters, data):
        """
        Updates records in the specified table.
        
        Parameters
        ----------
        table : str
            Name of the table to update.
        filters : dict
            Dictionary of column names and values to filter the results by.
        data : dict
            Dictionary of column names and values to update.
        
        """
        cursor = self.connection.cursor()
        filter_str = ' AND '.join([f"{k} = :{i+1+len(data)}" for i, k in enumerate(filters.keys())])
        data_str = ', '.join([f"{k} = :{i+1}" for i, k in enumerate(data.keys())])
        update_query = f"UPDATE {table} SET {data_str} WHERE {filter_str}"
        cursor.execute(update_query, list(data.values()) + list(filters.values()))
        self.connection.commit()
        cursor.close()

    def delete(self, table, filters):
        """
        Deletes records from the specified table.
        
        Parameters
        ----------
        table : str
            Name of the table to delete from.
        filters : dict
            Dictionary of column names and values to filter the results by.
        """
        cursor = self.connection.cursor()
        filter_str = ' AND '.join([f"{k} = :{i+1}" for i, k in enumerate(filters.keys())])
        delete_query = f"DELETE FROM {table} WHERE {filter_str}"
        cursor.execute(delete_query, list(filters.values()))
        self.connection.commit()
        cursor.close()