def test_postgresql_connection():
    """Test connection to postgresql database."""
    from flexdb.connectors.postgresql import PostgreSQLConnector
    config = {
        'host': 'localhost',
        'port': 5432,
        'user': 'postgres',
        'password': 'postgres',
        'database': 'postgres',
        'sslmode': 'prefer'
    }
    connector = PostgreSQLConnector(config)
    connector.connect()
    connector.close()
    assert connector.connection is not None and connector.connection.closed == 1