import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()
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

# Number of rows inserted in the last operation
ROWS_INSERTED = 463

try:
    # Establish a connection
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get the maximum ID from the table
    cursor.execute("SELECT MAX(id) FROM [GW_2_P10_HR_2024]")
    max_id = cursor.fetchone()[0]

    if max_id is not None:
        # Calculate the starting ID of the rows to delete
        start_id = max_id - ROWS_INSERTED + 1

        # Prepare the DELETE query
        delete_query = "DELETE FROM [GW_2_P10_HR_2024] WHERE id BETWEEN ? AND ?"

        # Execute the DELETE query
        cursor.execute(delete_query, (start_id, max_id))

        # Commit the transaction
        conn.commit()

        print(f"Deleted {ROWS_INSERTED} rows with IDs from {start_id} to {max_id}")
    else:
        print("The table is empty. No rows to delete.")

except pyodbc.Error as e:
    print(f"An error occurred: {e}")

finally:
    if 'conn' in locals():
        conn.close()