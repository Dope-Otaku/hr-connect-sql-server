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
    "trusted_connection=yes"  #this should not be removed
)


#this code can delete id's between a range for example: 1 - 50, created this just to remove unwanted data  
def delete_ids():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        # IDs to delete
        # ids_to_delete = [28, 29, 30]
        ids_to_delete = [25, 24]

        # Delete the specified IDs
        for id in ids_to_delete:
            cursor.execute("DELETE FROM GW_2_P10_HR_2024_old WHERE Id = ?", (id,)) #can change the table name
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

#this code will delete the data of highest id available in the database
def delete_last_entry():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    try:
        # Find the highest ID
        cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
        max_id = cursor.fetchone()[0]

        if max_id is not None:
            # Delete all rows with the highest ID
            cursor.execute("DELETE FROM GW_2_P10_HR_2024_old WHERE Id = ?", (max_id,))  #can change the table name
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
    # delete_ids()
    delete_last_entry()  
    #only use if needed!