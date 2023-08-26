from abc import ABC, abstractmethod


class DatabaseConnector(ABC):
    def __init__(self, config) -> None:
        self.config = config
        self.format_functions = {
            "dataframe": self.format_dataframe,
            "dict": self.format_dict,
            "polars": self.format_polars,
            "arrow": self.format_arrow,
        }
    
    @abstractmethod
    def connect(self):
        raise NotImplementedError
    
    @abstractmethod
    def close(self):
        raise NotImplementedError
    
    @abstractmethod
    def create(self, table, data):
        raise NotImplementedError
    
    @abstractmethod
    def read(self, table, filters):
        raise NotImplementedError
    
    @abstractmethod
    def update(self, table, filters, data):
        raise NotImplementedError
    
    @abstractmethod
    def delete(self, table, filters):
        raise NotImplementedError
    
    @abstractmethod
    def list_tables(self):
        raise NotImplementedError

    def format_output(self, results, column_names, output_format):
        format_func = self.format_functions.get(output_format)
        
        if format_func:
            return format_func(results, column_names)
        else:
            raise ValueError(f"Invalid output_format: {output_format}")

    def format_dataframe(self, results, column_names):
        import pandas as pd
        return pd.DataFrame(results, columns=column_names)

    def format_dict(self, results, column_names):
        return [dict(zip(column_names, row)) for row in results]

    def format_polars(self, results, column_names):
        import polars as pl
        return pl.DataFrame({col: [row[i] for row in results] for i, col in enumerate(column_names)})

    def format_arrow(self, results, column_names):
        import pyarrow as pa
        import pandas as pd
        return pa.Table.from_pandas(pd.DataFrame(results, columns=column_names))