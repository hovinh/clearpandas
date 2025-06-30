import duckdb
import pandas as pd

"""This module provides data connectors for reading and writing data in various formats.
It includes connectors for CSV and Excel files, allowing for data extraction and loading."""
class DataConnector:
    def __init__(self, source):
        self.source = source

    def get_data(self, query: str = None) -> pd.DataFrame:
        pass
    
    def write_data(self, pd_table: pd.DataFrame):
        pass
        
class CSVConnector(DataConnector):
    def __init__(self, source):
        self.source = source

    def get_data(self, query: str = None)-> pd.DataFrame:
        df = pd.read_csv(self.source)
        if (query):
            con = duckdb.connect()
            df = con.execute(query).df()
        return df
    
    def write_data(self, df: pd.DataFrame):
        df.to_csv(self.source, index=False)

class ExcelConnector(DataConnector):
    def __init__(self, source):
        self.source = source

    def get_data(self, query: str = None) -> pd.DataFrame:
        df = pd.read_excel(self.source)
        if (query):
            con = duckdb.connect()
            df = con.execute(query).df()
        return df
    
    def write_data(self, df: pd.DataFrame):
        df.to_excel(self.source, index=False)

"""This factory class creates instances of data connectors based on the file type."""
class ConnectorFactory:
    @staticmethod
    def create_connector(source: str) -> DataConnector:
        if source.endswith('.csv'):
            return CSVConnector(source)
        elif source.endswith('.xlsx'):
            return ExcelConnector(source)
        else:
            raise ValueError("Unsupported file format. Use .csv or .xlsx") 