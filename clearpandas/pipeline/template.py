from typing import Dict

import pandas as pd

from clearpandas.connector.connector import ConnectorFactory

"""A base class for data pipelines.
This class provides a template for implementing data pipelines with methods for extracting,
transforming, and loading data."""
class Pipeline:
    def __init__(self, ):
        self.connector_factory = ConnectorFactory()

    def execute(self):
        data = self.extract()
        transformed_data = self.transform(data)
        self.load(transformed_data)

    def extract(self) -> Dict[str, pd.DataFrame]:
        pass

    def transform(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        pass

    def load(self, data: Dict[str, pd.DataFrame]):
        pass