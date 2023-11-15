# Connectors Module in FlexDB

The `connectors` module in the `flexdb` library provides a set of tools to connect to various databases. It abstracts the complexities of establishing connections and executing queries, providing a simple and consistent interface.

## Installation

To install the `flexdb` library, you can use pip:

```bash
pip install flexdb
```

Usage
Here's a basic example of how to use the connectors module:

from flexdb import connectors

# Create a connector
conn = connectors.MySQLConnector(host='localhost', user='root', password='password', database='test')

# Execute a query
results = conn.execute_query('SELECT * FROM users')

# Close the connection
conn.close()

Classes
MySQLConnector
This class provides a connection to a MySQL database.

Methods
__init__(self, host: str, user: str, password: str, database: str)
Initializes a new instance of the MySQLConnector class.

execute_query(self, query: str) -> List[Dict]
Executes a SQL query and returns the results as a list of dictionaries.

close(self)
Closes the database connection.

Contributing
We welcome contributions! Please see our contributing guidelines for details.

License
This project is licensed under the MIT License. See the LICENSE file for details.