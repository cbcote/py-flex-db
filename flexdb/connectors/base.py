class DatabaseConnector:
    def __init__(self, config) -> None:
        self.config = config
    
    def connect(self):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError
    
    def create(self, table, data):
        raise NotImplementedError
    
    def read(self, table, filters):
        raise NotImplementedError
    
    def update(self, table, filters, data):
        raise NotImplementedError
    
    def delete(self, table, filters):
        raise NotImplementedError