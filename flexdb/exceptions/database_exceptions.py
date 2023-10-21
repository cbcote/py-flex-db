class DatabaseException(Exception):
    """Base class for exceptions in this module."""
    pass

class DatabaseConnectionError(DatabaseException):
    """Exception raised for errors in the database connection.

    Attributes
    ----------
    message : str
        Explanation of the error.
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

class DatabaseQueryError(DatabaseException):
    """Exception raised for errors in the database query.

    Attributes
    ----------
    message : str
        Explanation of the error.
    query : str, optional
        SQL query that caused the error.
    """

    def __init__(self, message, query=None):
        super().__init__(message) # Using built-in Exception class to set message
        self.query = query # Adding query attribute to provide more context
    
    def __str__(self) -> str:
        if self.query:
            return f'{self.message} Query: {self.query}'
        return self.message

class DatabaseUpdateError(DatabaseException):
    """Exception raised for errors in the database update.

    Attributes
    ----------
    message : str
        Explanation of the error.
    table : str, optional
        Name of the table that was updated.
    """

    def __init__(self, message, table=None):
        super().__init__(message)
        self.table = table
    
    def __str__(self) -> str:
        if self.table:
            return f'{self.message} Table: {self.table}'
        return self.message

class DatabaseDeleteError(DatabaseException):
    """Exception raised for errors in the database delete.

    Attributes
    ----------
    message : str
        Explanation of the error.
    table : str, optional
        Name of the table that was deleted from.
    """

    def __init__(self, message, table=None):
        super().__init__(message)
        self.table = table
    
    def __str__(self) -> str:
        if self.table:
            return f'{self.message} Table: {self.table}'
        return self.message

class DatabaseListTablesError(DatabaseException):
    """Exception raised for errors in the database list tables.

    Attributes
    ----------
    message : str
        Explanation of the error.
    database : str, optional
        Name of the database that was queried.
    """

    def __init__(self, message, database=None):
        super().__init__(message)
        self.database = database
    
    def __str__(self) -> str:
        if self.database:
            return f'{self.message} Database: {self.database}'
        return self.message

class DatabaseListColumnsError(DatabaseException):
    """Exception raised for errors in the database list columns.

    Attributes
    ----------
    message : str
        Explanation of the error.
    database : str, optional
        Name of the database that was queried.
    """

    def __init__(self, message, database=None):
        super().__init__(message)
        self.database = database
    
    def __str__(self) -> str:
        if self.database:
            return f'{self.message} Database: {self.database}'
        return self.message

class DatabaseCloseError(DatabaseException):
    """Exception raised for errors in the database connection close.

    Attributes
    ----------
    message : str
        Explanation of the error.
    """

    def __init__(self, message, database=None):
        super().__init__(message)
        self.database = database
    
    def __str__(self) -> str:
        if self.database:
            return f'{self.message} Database: {self.database}'
        return self.message