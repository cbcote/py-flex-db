from flexdb.connectors.base import DatabaseConnector
import psycopg2
import pandas as pd
import polars as pl
import pyarrow as pa


class PostgreSQLConnector(DatabaseConnector):
    """Establishes the database connection."""
    def connect(self):
        self.connection = psycopg2.connect(**self.config)
    
    def close(self):
        """Closes the database connection."""
        self.connection.close()

    def create(self, table, data):
        """Inserts a new record into the specified table."""
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
        cursor.execute(insert_query, list(data.values()))
        self.connection.commit()
        cursor.close()

    def read(self, table, filters=None, select_columns=None, output_format="dataframe"):
        """Reads records from the specified table."""
        with self.connection.cursor() as cursor:
            if not select_columns:
                select_string = '*'
            else:
                select_string = ', '.join(select_columns)
            select_query = f'SELECT {select_string} FROM {table}'
            if filters:
                conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
                select_query += f' WHERE {conditions}'
                cursor.execute(select_query, list(filters.values()))
            else:
                cursor.execute(select_query)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

        return self.format_output(results, column_names, output_format)

    def format_output(self, results, column_names, output_format):
        """Formats the output of the read operation based on the specified output_format."""
        if output_format == "dataframe":
            return pd.DataFrame(results, columns=column_names)
        elif output_format == "dict":
            return [dict(zip(column_names, row)) for row in results]
        elif output_format == "polars":
            return pl.DataFrame({col: [row[i] for row in results] for i, col in enumerate(column_names)})
        elif output_format == "arrow":
            return pa.Table.from_pandas(pd.DataFrame(results, columns=column_names))
        else:
            return results

    def update(self, table, filters, data):
        """Updates records in the specified table."""
        cursor = self.connection.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
        update_query = f'UPDATE {table} SET {columns} = {placeholders} WHERE {conditions}'
        cursor.execute(update_query, list(data.values()) + list(filters.values()))
        self.connection.commit()
        cursor.close()
        
    def delete(self, table, filters):
        """Deletes records from the specified table."""
        cursor = self.connection.cursor()
        conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
        delete_query = f'DELETE FROM {table} WHERE {conditions}'
        cursor.execute(delete_query, list(filters.values()))
        self.connection.commit()
        cursor.close()
