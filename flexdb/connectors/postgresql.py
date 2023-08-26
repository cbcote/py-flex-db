from flexdb.connectors.base import DatabaseConnector
import psycopg2
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
    
    def read(self, table=None, filters=None, select_columns=None, output_format="dataframe", raw_sql=None):
        """Reads records from the specified table."""
        
        with self.connection.cursor() as cursor:
            
            if raw_sql:
                cursor.execute(raw_sql)
            else:
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
    
    def list_tables(self):
        """Lists all tables in the current database."""
        try:
            with self.connection.cursor() as cursor:
                # This SQL query fetches all table names for the current database
                query = """
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname != 'pg_catalog'
                AND schemaname != 'information_schema';
                """
                
                cursor.execute(query)
                tables = cursor.fetchall()
                # Unpack the list of tuples into a list of table names
                table_names = [table[0] for table in tables]
                logging.info(f"Tables in database: {table_names}")
                return table_names
        except Exception as e:
            logging.error(f"Failed to list tables: {e}")
            raise
