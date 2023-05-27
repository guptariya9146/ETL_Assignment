import sqlite3
import pandas as pd

# Connect to the SQLite database
connection = sqlite3.connect('S30 ETL Assignment.db')
# Create a cursor
cursor = connection.cursor()
# Get the list of table names in the database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
table_names = cursor.fetchall()
table_names = [name[0] for name in table_names]
cursor.close()

# Create a dictionary to store the DataFrames
dataframes = {}
# Iterate over the table names
for table_name in table_names:
    # Execute a SELECT query to fetch the data from the table
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, connection)
    
    # Store the DataFrame in the dictionary
    dataframes[table_name] = df
# Close the connection
connection.close()

for df in dataframes.items():
    print(df)
# we have dataframe with these name: [items_df,orders_df,customers_df,sales_df]

# Perform the equivalent operations using pandas dataframe.
items_orders = pd.merge(items_df, orders_df, on='item_id', how='left')
customers_sales = pd.merge(customers_df, sales_df, on='customer_id', how='left')
customers_sales = customers_sales[(customers_sales['age'] >= 18) & (customers_sales['age'] <= 35)]
final_table = pd.merge(customers_sales, items_orders, on='sales_id', how='left')
final_table['Quantity'] = final_table.groupby(['customer_id', 'item_id'])['quantity'].transform('sum')
final_table = final_table[['customer_id', 'age', 'item_name', 'Quantity']].drop_duplicates()
final_table = final_table[final_table['Quantity'] > 0]

# Rename columns 
final_table = final_table.rename(columns={'customer_id': 'Customer', 'age': 'Age', 'item_name': 'Item'})

# Select columns and drop duplicates
final_table = final_table[['Customer', 'Age', 'Item', 'Quantity']].drop_duplicates()

# Store the DataFrame to a CSV file delimited by ';'
output_file = 'output.csv'
final_table.to_csv(output_file, sep=';', index=False)

print(f"The result has been stored in '{output_file}'.")

