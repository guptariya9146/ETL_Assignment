# ETL_Assignment
# SQLite to Pandas DataFrame

This repository demonstrates how to read tables from an SQLite 3 database and convert them into DataFrames using pandas.

## Prerequisites

- Python 3.x
- pandas
- sqlite3

## Instructions

1. Clone the repository:


2. Navigate to the project directory:


3. Install the required packages:


4. Update the database path:

Open the `main.py` file and replace `'your_database.db'` with the path to your SQLite 3 database file.

5. Run the script:


The script will connect to the SQLite database, fetch the tables, and convert them into pandas DataFrames.

6. View the DataFrames:

The DataFrames will be printed in the console, showing the data from each table.

## Explanation

The script follows the following steps to read tables from SQLite and convert them into DataFrames:

1. Connect to the SQLite database using `sqlite3.connect()`.

2. Retrieve the list of table names from the database using a SQL query.

3. Iterate over the table names and perform the following steps for each table:
- Execute a SELECT query to fetch the data from the table using `pd.read_sql_query()`.
- Store the resulting DataFrame in a dictionary, using the table name as the key.

4. Close the database connection.

5. Print the DataFrames stored in the dictionary, showing the data from each table.

## License

This project is licensed under the [MIT License](LICENSE).

