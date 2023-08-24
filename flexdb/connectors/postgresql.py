from flexdb.connectors.base import DatabaseConnector
import psycopg2
import pandas as pd
import polars as pl
import pyarrow as pa
import logging


class PostgreSQLConnector(DatabaseConnector):
    """Establishes the database connection."""
    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            logging.info('Successfully connected to the database.')
        except Exception:
            logging.error(f'Failed to connect to the database: {e}.')
            raise
    
    def close(self):
        """Closes the database connection."""
        try:
            self.connection.close()
            logging.info('Database connection closed.')
        except Exception as e:
            logging.error(f'Failed to close the database connection: {e}.')
            raise

    def create(self, table, data):
        """Inserts a new record into the specified table."""
        try:
            with self.connection.cursor() as cursor:
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
                insert_query = f'INSERT INTO {table} ({columns}) VALUES ({placeholders})'
                cursor.execute(insert_query, list(data.values()))
                self.connection.commit()
            logging.info(f'Successfully inserted data into {table}.')
        except Exception as e:
            logging.error(f'Failed to insert data into {table}: {e}.')
            self.connection.rollback()
            raise
    
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
        try:
            with self.connection.cursor() as cursor:
                columns = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
                conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
                update_query = f'UPDATE {table} SET {columns} = {placeholders} WHERE {conditions}'
                cursor.execute(update_query, list(data.values()) + list(filters.values()))
                self.connection.commit()
            logging.info(f'Successfully updated data in {table}.')
        except Exception as e:
            logging.error(f'Failed to update data in {table}: {e}.')
            self.connection.rollback()
            raise
        
    def delete(self, table, filters):
        """Deletes records from the specified table."""
        try:
            with self.connection.cursor() as cursor:
                conditions = ' AND '.join([f'{key} = %s' for key in filters.keys()])
                delete_query = f'DELETE FROM {table} WHERE {conditions}'
                cursor.execute(delete_query, list(filters.values()))
                self.connection.commit()
            logging.info(f'Successfully deleted data from {table}.')
        except Exception as e:
            logging.error(f'Failed to delete data from {table}: {e}.')
            self.connection.rollback()
            raise
