import json
import datetime
from decimal import Decimal
from io import StringIO, BytesIO
import pandas as pd
import polars as pl
import pyarrow as pa


class DataExporter:

    @staticmethod
    def to_json(data):
        def custom_serializer(obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()
            elif isinstance(obj, Decimal):
                return float(obj)  # or str(obj) if you prefer string representation
            raise TypeError(f"Type {type(obj)} not serializable")

        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')
        
        try:
            return json.dumps(data, default=custom_serializer, indent=4)
        except Exception as e:
            print(f"An error occurred while exporting to JSON: {e}")
            return None

    @staticmethod
    def to_csv(data, column_names=None):
        if isinstance(data, list):
            data = pd.DataFrame(data, columns=column_names)
        elif not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        output = StringIO()
        try:
            data.to_csv(output, index=False)
            print(f"Data exported to {output}")
        except Exception as e:
            print(f"An error occurred while exporting to CSV: {e}")
            return None
        return output.getvalue()

    @staticmethod
    def to_excel(data, column_names=None):
        if isinstance(data, list):
            data = pd.DataFrame(data, columns=column_names)
        elif not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        output = BytesIO()
        try:
            data.to_excel(output, index=False, engine='openpyxl')
        except Exception as e:
            print(f"An error occurred while exporting to Excel: {e}")
            return None
        return output.getvalue()
