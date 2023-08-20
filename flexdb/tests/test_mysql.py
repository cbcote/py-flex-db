def test_mysql_connection():
    """Test connection to mysql database."""
    from flexdb.connectors.mysql import MySQLConnector
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': 'mysql',
        'database': 'testdb'
    }
    connector = MySQLConnector(config)
    connector.connect()
    connector.close()
    assert connector.connection is not None and connector.connection.is_connected() == False