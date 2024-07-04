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

def delete_last_entry():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        # Find the highest ID
        cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
        max_id = cursor.fetchone()[0]

        if max_id is not None:
            # Delete all rows with the highest ID
            cursor.execute("DELETE FROM GW_2_P10_HR_2024_old WHERE Id = ?", (max_id,))
            conn.commit()
            print(f"Successfully deleted all entries with Id: {max_id}")
        else:
            print("No entries found in the table.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    delete_last_entry()