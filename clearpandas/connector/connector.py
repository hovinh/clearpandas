import duckdb
import pandas as pd

"""This module provides data connectors for reading and writing data in various formats.
It includes connectors for CSV and Excel files, allowing for data extraction and loading."""
class DataConnector:
    def __init__(self, path):
        self.path = path

    def get_data(self, query: str = None) -> pd.DataFrame:
        pass
    
    def write_data(self, pd_table: pd.DataFrame):
        pass
        
class CSVConnector:
    def __init__(self, path):
        self.path = path

    def get_data(self, query: str = None)-> pd.DataFrame:
        df = pd.read_csv(self.path)
        if (query):
            con = duckdb.connect()
            df = con.execute(query).df()
        return df
    
    def write_data(self, df: pd.DataFrame):
        df.to_csv(self.path, index=False)

class ExcelConnector:
    def __init__(self, path):
        self.path = path

    def get_data(self, query: str = None) -> pd.DataFrame:
        df = pd.read_excel(self.path)
        if (query):
            con = duckdb.connect()
            df = con.execute(query).df()
        return df
    
    def write_data(self, df: pd.DataFrame):
        df.to_excel(self.path, index=False)

"""This factory class creates instances of data connectors based on the file type."""
class ConnectorFactory:
    @staticmethod
    def create_connector(path: str) -> DataConnector:
        if path.endswith('.csv'):
            return CSVConnector(path)
        elif path.endswith('.xlsx'):
            return ExcelConnector(path)
        else:
            raise ValueError("Unsupported file format. Use .csv or .xlsx") 