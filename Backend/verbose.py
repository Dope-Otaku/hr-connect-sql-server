#only for test

import pyodbc

# Define the database configuration
config = {
    'driver': 'SQL Server',  
    'server': 'localhost',
    'database': 'ITP_1482',
    'username': 'RAJ',
    'password': 'root@123',
    'port': 1433
}

# Create a connection string
conn_str = (
    f"DRIVER={config['driver']};"
    f"SERVER={config['server']},{config['port']};"
    f"DATABASE={config['database']};"
    f"UID={config['username']};"
    f"PWD={config['password']};"
    "trusted_connection=yes"  # Add this line
)

conn = None

try:
    # Establish the connection
    conn = pyodbc.connect(conn_str)
    print("Connection successful!")

    # Create a cursor object
    cursor = conn.cursor()

    # Define and execute the query
    query = "SELECT * FROM dbo.GW_1_P10_HR_2024_Ref"
    # query = "SELECT * FROM dbo.ConnectedPLCs"
    cursor.execute(query)

    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Print the fetched rows
    for row in rows:
        print(row)

except pyodbc.Error as err:
    print("Error: ", err)

finally:
    # Close the connection
    if conn:
        conn.close()
        print("Connection closed.")