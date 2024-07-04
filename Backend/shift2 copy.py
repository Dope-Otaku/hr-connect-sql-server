from flask import Flask, jsonify, request
import time
from datetime import datetime, timedelta
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

app = Flask(__name__)

def get_db_connection():
    conn = pyodbc.connect(conn_str)
    return conn

# def fetch_latest_data(cursor):
#     query = """
#     SELECT TOP 1000 *
#     FROM GW_2_P10_HR_2024_old
#     WHERE Id = (SELECT MAX(Id) FROM GW_2_P10_HR_2024_old)
#     ORDER BY ColumnName
#     """
#     # query = """
#     # SELECT TOP 1000 *
#     # FROM GW_2_P10_HR_2024_old
#     # WHERE Id = (SELECT MAX(Id) FROM GW_2_P10_HR_2024_old)
#     # ORDER BY ColumnName and order by id desc
#     # """
#     # WHERE Id = (SELECT MAX(Id) FROM GW_2_P10_HR_2024)

#     cursor.execute(query)
#     rows = cursor.fetchall()
#     return rows

def fetch_latest_data(cursor):
    query = """
    SELECT TOP 1000 Id, ColumnName, Value
    FROM GW_2_P10_HR_2024_old
    WHERE Id = (SELECT MAX(Id) FROM GW_2_P10_HR_2024_old)
    ORDER BY ColumnName
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

def calculate_product_count(rows):
    temp_pc = 0
    temp_rem = 0
    column_indices = ['009', '059', '109', '159', '209', '259', '309', '359', '409', '459']
    values = {}

    for row in rows:
        if row.ColumnName[1:] in column_indices:
            values[row.ColumnName] = float(row.Value)

    for i in range(len(column_indices) - 1):
        current_col = f'C{column_indices[i]}'
        next_col = f'C{column_indices[i+1]}'
        if current_col in values and next_col in values:
            temp_pc = values[next_col] - values[current_col]
            temp_rem += temp_pc

    return temp_rem

# def calculate_duration(start_time, end_time):
#     if start_time == 'N/A' or end_time == 'N/A':
#         return 'N/A'
#     try:
#         start = datetime.strptime(start_time, "%H:%M")
#         current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#         current = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
#         duration = current - start
#         total_seconds = int(duration.total_seconds())

#         if total_seconds <= 3600:
#             return total_seconds
#         else:
#             hours, remainder = divmod(total_seconds, 3600)
#             minutes, seconds = divmod(remainder, 60)
#             return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
#             # return f"{seconds:02d} Seconds"
#     except ValueError:
#         return 'Invalid time format'

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


def monitor_shifts():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    while True:
        latest_data = fetch_latest_data(cursor)
        
        if latest_data:
            data_dict = {row.ColumnName: row.Value for row in latest_data}

            # last_id = cursor.fetchone()[0]
            # new_id = (last_id or 0) + 1
            # id = data_dict.get('C001', 'N/A')

            id = latest_data[0].Id if latest_data else 'N/A'
            date = data_dict.get('C002', 'N/A')
            start_time = data_dict.get('C003', 'N/A')
            end_time = data_dict.get('C004', 'N/A')
            shift = data_dict.get('C005', 'N/A')
            product_count = data_dict.get('C010', 'N/A')
            downtime = data_dict.get('C007', 'N/A')
            DURATION = data_dict.get('C009', 'N/A')
            product_type = data_dict.get('C008', 'N/A')
            plan = data_dict.get('C012', 'N/A')
            aothrqty = data_dict.get('C013', 'N/A')
            # cycle_time = 60
            # plan = (str(int(DURATION) // cycle_time))
            # product_count = calculate_product_count(latest_data)
            # duration = calculate_duration(start_time, end_time)
            # duration = data_dict.get('C007', 'N/A')
            duration = cal_dur(start_time, end_time)

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            print(f"Current Time: {current_time}")
            print(f"Latest Data:")
            print(f"ID: {id}")
            # print(f"ID: {last_id}")
            print(f"Date: {date}")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")
            print(f"Duration: {DURATION} Seconds!")
            # print(f"Duration: {int(DURATION) - int(downtime)} Seconds!")
            # print(f"Duration: {3600 - int(duration)} Seconds!")
            # print(f"Downtime: {duration} Seconds!")
            print(f"Downtime: {downtime} Sec")
            print(f"Shift: {shift}")
            print(f"Product Count: {product_count}")
            print(f"Product Type: {product_type}")
            print(f"PLAN: {plan}")
            print(f"AOTHRQTY: {aothrqty}")
            # print(f"Product Count: Will Be Updated Later...!")
            print("--------------------")
        else:
            print("No data found")
        
        time.sleep(5)

if __name__ == "__main__":
    monitor_shifts()