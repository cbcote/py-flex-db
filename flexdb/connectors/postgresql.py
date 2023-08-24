from flexdb.connectors.base import DatabaseConnector
import psycopg2
import pandas as pd
import polars as pl
import pyarrow as pa


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

    def read(self, table, filters=None, select_columns=None, output_format="dataframe"):
        cursor = self.connection.cursor()
        
        # If select_columns is None or empty, select all columns using '*'
        if not select_columns:
            select_string = '*'
        else:
            select_string = ', '.join(select_columns)

        # Construct the base query
        select_query = f'SELECT {select_string} FROM {table}'

        # If filters are provided, add the WHERE clause
        if filters:
            conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
            select_query += f' WHERE {conditions}'
            cursor.execute(select_query, list(filters.values()))
        else:
            cursor.execute(select_query)

        results = cursor.fetchall()

        # Get column names from cursor description
        column_names = [desc[0] for desc in cursor.description]

        cursor.close()

        # Depending on desired output_format, return appropriate data structure
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
