import json
import pandas as pd
import pyarrow as pa
import logging
from datetime import datetime, date
from decimal import Decimal

class DataExporter:
    def __init__(self) -> None:
        self.format_map = {
            "json": self.to_json,
            "csv": self.to_csv,
            "excel": self.to_excel,
        }

    def export_data(self, data, format_type, filename, **kwargs):
        """
        :param data: list of dicts, pandas dataframe, or pyarrow table
        :param format_type: json, csv, or excel
        :param filename: filename to save to
        :return: None
        """
        exporter_fn = self.format_map.get(format_type)
        if exporter_fn:
            exporter_fn(data, filename, **kwargs)
        else:
            logging.error(f"Format {format_type} not supported.")
            return None

    def to_json(self, data, filename):
        """
        :param data: list of dicts, pandas dataframe, or pyarrow table
        :param filename: filename to save to
        :return: None
        """
        def custom_serializer(obj):
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()
            elif isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(f"Type {type(obj)} not serializable")

        if isinstance(data, pa.lib.Table):
            data = data.to_pandas()

        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')

        try:
            with open(filename, 'w') as f:
                json.dump(data, f, default=custom_serializer, indent=4)
        except Exception as e:
            logging.error(f"An error occurred while exporting to JSON: {e}")

    def to_csv(self, data, filename):
        """
        :param data: list of dicts, pandas dataframe, or pyarrow table
        :param filename: filename to save to
        :return: None
        """
        try:
            data = self.convert_to_dataframe(data)
            data.to_csv(filename, index=False)
        except Exception as e:  # Adjusted from PandasError as it's not specific enough
            logging.error(f"An error occurred while exporting to CSV: {e}")

    def to_excel(self, data, filename):
        """
        :param data: list of dicts, pandas dataframe, or pyarrow table
        :param filename: filename to save to
        :return: None
        """
        try:
            data = self.convert_to_dataframe(data)
            data.to_excel(filename, index=False, engine='openpyxl')
        except Exception as e:  # Adjusted from PandasError as it's not specific enough
            logging.error(f"An error occurred while exporting to Excel: {e}")

    @staticmethod
    def convert_to_dataframe(data):
        """
        :param data: list of dicts, pandas dataframe, or pyarrow table
        :return: pandas dataframe
        """
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, pa.lib.Table):
            return data.to_pandas()
        elif isinstance(data, pd.DataFrame):
            return data
        else:
            raise TypeError("Data format not recognized.")
