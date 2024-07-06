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


# Global variables
downtime = 0
product_count = 0
product_type = 1
cur_DT = 0
cur_PC = 0
last_reset_time = datetime.now()




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

def cal_dur(start_time):
    if start_time == 'N/A':
        return "N/A"
    
    try:
        start = datetime.strptime(start_time, "%H:%M")
        current = datetime.now()
        
        # Combine the start time with today's date
        start = datetime.combine(current.date(), start.time())
        
        # If start time is in the future, assume it's from yesterday
        if start > current:
            start -= timedelta(days=1)
        
        duration = current - start
        total_seconds = int(duration.total_seconds())
        return str(total_seconds)  # Convert to string for consistency with other data
    except ValueError:
        return 'Invalid time format'


def reset_values():
    global downtime, product_count, product_type, cur_DT, cur_PC, last_reset_time
    downtime = 0
    product_count = 0
    product_type = 1
    cur_DT = 0
    cur_PC = 0
    last_reset_time = datetime.now()
    print("Values reset to default")


def cal_dur(start_time, end_time):
    if start_time == 'N/A':
        return "N/A"
    
    try:
        start = datetime.strptime(start_time, "%H:%M")
        end = datetime.strptime(end_time, "%H:%M") if end_time != 'N/A' else None
        current = datetime.now()
        
        # Combine the times with today's date
        start = datetime.combine(current.date(), start.time())
        if end:
            end = datetime.combine(current.date(), end.time())
        
        # If start time is in the future, assume it's from yesterday
        if start > current:
            start -= timedelta(days=1)
        
        # If end time is earlier than start time, assume it's for the next day
        if end and end < start:
            end += timedelta(days=1)
        
        # Calculate duration
        if end and current >= end:
            # Reset to 0 if current time is past end time
            return 0
        else:
            duration = current - start
            total_seconds = int(duration.total_seconds())
            return min(total_seconds, 3600)  # Cap at 3600 seconds
    except ValueError:
        return 'Invalid time format'

def insert_or_update_data():
    global downtime, product_count, product_type, cur_DT, cur_PC, last_reset_time

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Check if an hour has passed
    if datetime.now() - last_reset_time >= timedelta(hours=1):
        reset_values()

    # Increment values
    prev_downtime = downtime
    prev_pc = product_count

    downtime += 1
    product_count += 2

    cur_DT += (downtime - prev_downtime)
    cur_PC += (product_count - prev_pc)

    print(f"product count: {product_count} |  Downtime: {downtime}")

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

    # Calculate duration
    duration = cal_dur(current_time, end_time)
    # print(duration)
    cur_duration = str(duration - downtime)

    #get plan
    cycle_time = 60
    plan = (str(int(cur_duration) // cycle_time))

    try:
        # Check if an entry for the current hour already exists
        cursor.execute("""
            SELECT DISTINCT Id 
            FROM GW_2_P10_HR_2024_old 
            WHERE ColumnName = 'C003' 
            AND Value = ? 
            AND Id IN (
                SELECT Id 
                FROM GW_2_P10_HR_2024_old 
                WHERE ColumnName = 'C002' 
                AND Value = ?
            )
        """, (current_time, current_date))
        existing_id = cursor.fetchone()

        if existing_id:
            # Update existing entry
            existing_id = existing_id[0]
            print(f"Updating existing entry with Id: {existing_id}")
            
            update_data = [
                ('C001', f"{current_date} {now.strftime('%H:%M')}:00"),
                ('C004', end_time),
                ('C005', shift_name),
                ('C006', '0'),
                ('C007', cur_DT),
                ('C008', product_type),
                ('C009', cur_duration),
                ('C010', cur_PC),
                ('C011', 'TPM'),
                ('C012', plan),
                ('C013', cur_DT                                                                                                )
            ]
            
            for column_name, value in update_data:
                cursor.execute("""
                    UPDATE GW_2_P10_HR_2024_old 
                    SET Value = ? 
                    WHERE Id = ? AND ColumnName = ?
                """, (value, existing_id, column_name))
        else:
            # Insert new entry
            cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
            max_id = cursor.fetchone()[0]
            new_id = (max_id or 0) + 1
            print(f"Inserting new entry with Id: {new_id}")

            insert_data = [
                ('C001', f"{current_date} {now.strftime('%H:%M')}:00"),
                ('C002', current_date),
                ('C003', current_time),
                ('C004', end_time),
                ('C005', shift_name),
                ('C006', '0'),
                ('C007', cur_DT),
                ('C008', product_type),
                ('C009', cur_duration),
                ('C010', cur_PC),
                ('C011', 'TPM'),
                ('C012', plan),
                ('C013', cur_DT)
            ]

            for column_name, value in insert_data:
                cursor.execute("""
                    INSERT INTO GW_2_P10_HR_2024_old (Id, ColumnName, Value)
                    VALUES (?, ?, ?)
                """, (new_id, column_name, value))

        conn.commit()
        print("Data insertion/update completed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        conn.close()

# def run_with_timer():
#     while True:
#         current_time = datetime.now()
        
#         # Run the first time
#         print(f"Running at {current_time}")
#         insert_or_update_data()
        
#         # Calculate the time for the second run (20 minutes later)
#         second_run_time = current_time + timedelta(minutes=2)
        
#         # Sleep until it's time for the second run
#         sleep_time = (second_run_time - datetime.now()).total_seconds()
#         if sleep_time > 0:
#             print(f"Sleeping for {sleep_time} seconds until {second_run_time}")
#             time.sleep(sleep_time)
        
#         # Run the second time
#         print(f"Running at {datetime.now()}")
#         insert_or_update_data()
        
#         # Calculate the start of the next hour
#         next_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        
#         # Sleep until the start of the next hour
#         sleep_time = (next_hour - datetime.now()).total_seconds()
#         if sleep_time > 0:
#             print(f"Sleeping for {sleep_time} seconds until {next_hour}")
#             time.sleep(sleep_time)

def run_with_timer():
    while True:
        current_time = datetime.now()
        
        # Calculate the start of the next hour
        next_hour = current_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        
        # Run and update until the next hour
        while datetime.now() < next_hour:
            print(f"Running at {datetime.now()}")
            insert_or_update_data()
            
            # Sleep for 2 minutes
            time.sleep(120)  # 120 seconds = 2 minutes
        
        # Calculate sleep time until the start of the next hour
        sleep_time = (next_hour - datetime.now()).total_seconds()
        if sleep_time > 0:
            print(f"Sleeping for {sleep_time} seconds until {next_hour}")
            time.sleep(sleep_time)

if __name__ == "__main__":
    run_with_timer()