import pyodbc
import random
import datetime

import os
from dotenv import load_dotenv

load_dotenv()


'''
will generate a random number to fill all the c001, c002, ... etc
and insert it into the database

created this file just to check whether the table is accepting insert queries

'''



# Define the database configuration
config = {
    'driver': os.getenv('DRIVER'),
    'server': os.getenv('SERVER'),
    'database': os.getenv('DATABASE'),
    'username': os.getenv('USERNAME'),
    'password': os.getenv('PASSWORD'),
    'port': os.getenv('PORT')
}

# Create a connection string
conn_str = (
    f"DRIVER={config['driver']};"
    f"SERVER={config['server']},{config['port']};"
    f"DATABASE={config['database']};"
    f"UID={config['username']};"
    f"PWD={config['password']};"
    "trusted_connection=yes"
)

def generate_value(column_name):
    """Generate an appropriate value based on the column name."""
    if column_name == 'C001':
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elif column_name in ['C002', 'C003', 'C004']:
        return datetime.date.today().strftime("%Y-%m-%d")
    elif column_name == 'C005':
        return random.choice(['Shift 1', 'Shift 2', 'Shift 3', 'Shift 4', 'Shift 5'])
    else:
        return str(random.randint(0, 1000))

try:
    # Establish a connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get the last ID from the table
    cursor.execute("SELECT MAX(id) FROM [GW_2_P10_HR_2024]")
    last_id = cursor.fetchone()[0]
    new_id = (last_id or 0) + 1

    # Prepare the INSERT query
    query = "INSERT INTO [GW_2_P10_HR_2024] (id, ColumnName, Value) VALUES (?, ?, ?)"

    # Generate and insert data for each column
    for i in range(1, 464):  # C001 to C463
        column_name = f"C{str(i).zfill(3)}"
        value = generate_value(column_name)
        
        cursor.execute(query, (new_id, column_name, value))

    # Commit the transaction
    conn.commit()
    print(f"Data inserted successfully. 463 rows added with ID: {new_id}")

except pyodbc.Error as e:
    print(f"An error occurred: {e}")

finally:
    if 'conn' in locals():
        conn.close()