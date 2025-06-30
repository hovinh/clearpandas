from typing import Dict

import pandas as pd
from clearpandas.pipeline.template import Pipeline
from clearpandas import transform 

"""Data Engineering Pipeline
This pipeline extracts customer and order data, transforms it to filter urgent orders,
and loads the results into a CSV file."""
class DataEngineeringPipeline(Pipeline):
    def __init__(self):
        super().__init__()

    def extract(self) -> Dict[str, pd.DataFrame]:
        customer_connector = self.connector_factory.create_connector('data/customers.csv')
        customer_table = customer_connector.get_data("SELECT * FROM df WHERE c_acctbal > 200")
        customer_table = transform.base.remove_nulls(customer_table)

        order_connector = self.connector_factory.create_connector('data/orders.csv')
        order_table = order_connector.get_data()
        order_table = transform.base.remove_nulls(order_table)
        data = {
            'customers': customer_table,
            'orders': order_table,
        }
        return data

    def transform(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        customer_table = (
            data['customers']
            .pipe(transform.customers.round_up_acctbal)
            .pipe(transform.customers.filter_customers, max_custkey=1000)
            )

        order_table = (
            data['orders']
            .pipe(transform.orders.filter_urgent_orders)
        )

        urgent_order_details_table = pd.merge(
            order_table,
            customer_table,
            left_on='o_custkey',
            right_on='c_custkey',
            how='inner'
        )

        data = {
            'urgent_order_details': urgent_order_details_table,
        }
        return data

    def load(self, data: Dict[str, pd.DataFrame]):
        connector = self.connector_factory.create_connector('data/urgent_order_details.csv')
        connector.write_data(data['urgent_order_details']) 
