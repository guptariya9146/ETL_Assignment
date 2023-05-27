import sqlite3
import pandas as pd
try:
    # Establish a connection to your database
    sqliteConnection = sqlite3.connect('S30 ETL Assignment.db')
    # Create a cursor
    cursor = sqliteConnection.cursor()
    # Execute the SQL query and store the result in a DataFrame
    query = '''
        WITH items_orders as(
          SELECT i.item_id as item_id,i.item_name as item_name,o.sales_id as sales_id,o.quantity as quantity
          FROM items i
          LEFT join orders o 
          on i.item_id=o.item_id
        ),
        customers_sales AS(
          SELECT c.customer_id as customer_id,c.age as age,s.sales_id as sales_id
          FROM customers c
          LEFT join sales s
          on c.customer_id=s.customer_id
          where c.age BETWEEN 18 and 35
        ),
        final_table as(
            SELECT customers_sales.customer_id as Customer,customers_sales.age as Age,items_orders.item_name as Item
            ,sum(items_orders.quantity) over(partition by customers_sales.customer_id,items_orders.item_id order by customers_sales.customer_id) as Quantity
            from customers_sales LEFT join items_orders 
            on customers_sales.sales_id=items_orders.sales_id
        )
 	SELECT DISTINCT Customer,Age,Item,Quantity
        from final_table
        where quantity>0;
    '''
    cursor.execute(query)
    # Fetch all the results as a list of tuples
    results = cursor.fetchall()
    # Define column names
    columns = ['Customer', 'Age', 'Item', 'Quantity']
    # Create a DataFrame from the results
    result_df = pd.DataFrame(results, columns=columns)
    # Store the DataFrame to a CSV file delimited by ';'
    output_file = 'output.csv'
    result_df.to_csv(output_file, sep=';', index=False)
    print(f"The result has been stored in '{output_file}'.")

    # Close the cursor and the connection
    cursor.close()

# Handle errors
except sqlite3.Error as error:
    print('Error occurred - ', error)
 
# Close DB Connection irrespective of success
# or failure
finally:
   
    if sqliteConnection:
        sqliteConnection.close()
        print('SQLite Connection closed')
    
    
    
 
