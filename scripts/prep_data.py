import duckdb
import os

con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch;")
con.execute("CALL dbgen(sf=0.01);")

print ("Creating customers table...")
customers_df = con.execute("SELECT * FROM customer").df()
file_path = 'data/customers.csv'
customers_df.to_csv(file_path, index=False)
print (f"Number of customers: {len(customers_df)}")
print (customers_df.head())

print ("Creating orders table...")
orders_df = con.execute("SELECT * FROM orders").df()
file_path = 'data/orders.csv'
orders_df.to_csv(file_path, index=False)
print (f"Number of orders: {len(orders_df)}")
print (orders_df.head())

print ("Done creating customers and orders tables.")