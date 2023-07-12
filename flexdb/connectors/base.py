class DatabaseConnector:
    def __init__(self, config) -> None:
        self.config = config
    
    def connect(self):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError