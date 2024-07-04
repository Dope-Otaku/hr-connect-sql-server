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

def delete_ids():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        # IDs to delete
        # ids_to_delete = [28, 29, 30]
        ids_to_delete = [25, 24]

        # Delete the specified IDs
        for id in ids_to_delete:
            cursor.execute("DELETE FROM GW_2_P10_HR_2024_old WHERE Id = ?", (id,))
            print(f"Deleted entries with Id: {id}")

        # Commit the changes
        conn.commit()
        print("Deletion completed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    delete_ids()