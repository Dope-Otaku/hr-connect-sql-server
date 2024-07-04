import pyodbc
from datetime import datetime, timedelta
import time

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

# def insert_or_update_data():
#     conn = pyodbc.connect(conn_str)
#     cursor = conn.cursor()

#     # Get current date and time
#     now = datetime.now()
#     current_date = now.strftime("%Y-%m-%d")
#     current_time = now.strftime("%H:%M")
#     end_time = (now + timedelta(hours=1, minutes=00)).strftime("%H:%M")

#     # Prepare data
#     data = [
#         ('C001', f"{current_date} {current_time}:00"),
#         ('C002', current_date),
#         ('C003', current_time),
#         ('C004', end_time),
#         ('C005', 'Shift 2'),
#         ('C006', '8'),
#         ('C007', '5068'),
#         ('C008', '1'),
#         ('C009', '4390'),
#         ('C010', '62'),
#         ('C011', 'TPM'),
#         ('C012', None),
#         ('C013', None)
#     ]

#     try:
#         # Check if an entry for today already exists
#         cursor.execute(f"SELECT TOP 1 Id FROM GW_2_P10_HR_2024_old WHERE CONVERT(date, Value) = '{current_date}' AND ColumnName = 'C001' ORDER BY Id DESC")
#         existing_id = cursor.fetchone()

#         if existing_id:
#             # Update existing entry
#             for column_name, value in data:
#                 if value is not None:
#                     cursor.execute(f"""
#                         UPDATE GW_2_P10_HR_2024_old 
#                         SET Value = ? 
#                         WHERE Id = ? AND ColumnName = ?
#                     """, (value, existing_id[0], column_name))
#             print(f"Updated existing entry with Id: {existing_id[0]}")
#         else:
#             # Insert new entry
#             cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
#             max_id = cursor.fetchone()[0]
#             new_id = (max_id or 0) + 1

#             for column_name, value in data:
#                 cursor.execute("""
#                     INSERT INTO GW_2_P10_HR_2024_old (Id, ColumnName, Value)
#                     VALUES (?, ?, ?)
#                 """, (new_id, column_name, value))
#             print(f"Inserted new entry with Id: {new_id}")

#         conn.commit()
#         print("Data insertion/update completed successfully.")

#     except Exception as e:
#         conn.rollback()
#         print(f"An error occurred: {e}")

#     finally:
#         cursor.close()
#         conn.close()



# def insert_or_update_data():
#     conn = pyodbc.connect(conn_str)
#     cursor = conn.cursor()

#     # Get current date and time
#     now = datetime.now()
#     current_date = now.strftime("%Y-%m-%d")
    
#     # Round down current time to the nearest hour
#     current_hour = now.replace(minute=0, second=0, microsecond=0)
#     current_time = current_hour.strftime("%H:%M")
    
#     # Set end time to the next hour
#     end_time = (current_hour + timedelta(hours=1)).strftime("%H:%M")

#     # Prepare data
#     data = [
#         ('C001', f"{current_date} {now.strftime('%H:%M')}:00"),  # Keep actual minutes for C001
#         ('C002', current_date),
#         ('C003', current_time),  # Rounded down to hour
#         ('C004', end_time),  # Next hour
#         ('C005', 'Shift 2'), #dynamic shift 
#         # select value from CT0013 where id = (select id from CT0013 where ColumnName = 'C001_FromTime' and value = '13:00') and ColumnName = 'C004_Shift'
#         ('C006', '8'),
#         ('C007', '5068'), #dur
#         ('C008', '1'),
#         ('C009', '4390'),
#         ('C010', '62'),
#         ('C011', 'TPM'),
#         ('C012', None),
#         ('C013', None)
#     ]

#     try:
#         # Check if an entry for today already exists
#         cursor.execute(f"SELECT TOP 1 Id FROM GW_2_P10_HR_2024_old WHERE CONVERT(date, Value) = '{current_date}' AND ColumnName = 'C001' ORDER BY Id DESC")
#         existing_id = cursor.fetchone()

#         if existing_id:
#             # Update existing entry
#             for column_name, value in data:
#                 if value is not None:
#                     cursor.execute(f"""
#                         UPDATE GW_2_P10_HR_2024_old 
#                         SET Value = ? 
#                         WHERE Id = ? AND ColumnName = ?
#                     """, (value, existing_id[0], column_name))
#             print(f"Updated existing entry with Id: {existing_id[0]}")
#         else:
#             # Insert new entry
#             cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
#             max_id = cursor.fetchone()[0]
#             new_id = (max_id or 0) + 1

#             for column_name, value in data:
#                 cursor.execute("""
#                     INSERT INTO GW_2_P10_HR_2024_old (Id, ColumnName, Value)
#                     VALUES (?, ?, ?)
#                 """, (new_id, column_name, value))
#             print(f"Inserted new entry with Id: {new_id}")

#         conn.commit()
#         print("Data insertion/update completed successfully.")

#     except Exception as e:
#         conn.rollback()
#         print(f"An error occurred: {e}")

#     finally:
#         cursor.close()
#         conn.close()


def get_shift_data(cursor, current_time):
    query = """
    SELECT TOP 1 Value
    FROM CT0013  -- Replace with your actual table name
    WHERE ColumnName = 'C004_Shift'
      AND Id IN (
        SELECT Id
        FROM CT0013
        WHERE ColumnName = 'C001_FromTime'
          AND CAST(Value AS TIME) <= CAST(? AS TIME)
      )
    ORDER BY (
        SELECT CAST(Value AS TIME)
        FROM CT0013 t2
        WHERE t2.Id = CT0013.Id AND t2.ColumnName = 'C001_FromTime'
    ) DESC
    """
    cursor.execute(query, (current_time,))
    result = cursor.fetchone()
    return result[0] if result else 'Unknown Shift'

def insert_or_update_data():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get current date and time
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    
    # Round down current time to the nearest hour
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    current_time = current_hour.strftime("%H:%M")
    
    # Set end time to the next hour
    end_time = (current_hour + timedelta(hours=1)).strftime("%H:%M")

    # Get dynamic shift data
    shift_name = get_shift_data(cursor, now.strftime("%H:%M"))

    # Prepare data
    data = [
        ('C001', f"{current_date} {now.strftime('%H:%M')}:00"),
        ('C002', current_date),
        ('C003', current_time),
        ('C004', end_time),
        ('C005', shift_name),  # Dynamic shift name
        ('C006', '8'),
        ('C007', '5068'),
        ('C008', '1'),
        ('C009', '4390'),
        ('C010', '62'),
        ('C011', 'TPM'),
        ('C012', None),
        ('C013', None)
    ]

    try:
        # Always insert a new entry
        cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
        max_id = cursor.fetchone()[0]
        new_id = (max_id or 0) + 1

        for column_name, value in data:
            cursor.execute("""
                INSERT INTO GW_2_P10_HR_2024_old (Id, ColumnName, Value)
                VALUES (?, ?, ?)
            """, (new_id, column_name, value))
        print(f"Inserted new entry with Id: {new_id}")

        conn.commit()
        print("Data insertion completed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        conn.close()

def run_with_timer():
    while True:
        current_time = datetime.now()
        
        # Run the first time
        print(f"Running at {current_time}")
        insert_or_update_data()
        
        # Calculate the time for the second run (20 minutes later)
        second_run_time = current_time + timedelta(minutes=20)
        
        # Sleep until it's time for the second run
        sleep_time = (second_run_time - datetime.now()).total_seconds()
        if sleep_time > 0:
            print(f"Sleeping for {sleep_time} seconds until {second_run_time}")
            time.sleep(sleep_time)
        
        # Run the second time
        print(f"Running at {datetime.now()}")
        insert_or_update_data()
        
        # Calculate the start of the next hour
        next_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        
        # Sleep until the start of the next hour
        sleep_time = (next_hour - datetime.now()).total_seconds()
        if sleep_time > 0:
            print(f"Sleeping for {sleep_time} seconds until {next_hour}")
            time.sleep(sleep_time)

if __name__ == "__main__":
    # insert_or_update_data()
    run_with_timer()