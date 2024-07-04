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

def fetch_data(cursor, shift_start_time, shift_end_time):
    query = f"""
    SELECT *
    FROM GW_2_P10_HR_2024
    WHERE ColumnName IN ('C001', 'C002', 'C003', 'C004', 'C005')
    AND Value BETWEEN '{shift_start_time}' AND '{shift_end_time}'
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows

def monitor_shifts():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    while True:
        now = datetime.now()
        shift_start_time = now - timedelta(hours=1)
        
        logs = fetch_data(cursor, shift_start_time, now)
        
        shift_start = shift_start_time.strftime("%H:%M")
        shift_end = now.strftime("%H:%M")
        duration = now - shift_start_time
        
        print(f"Shift: Shift 1")
        print(f"Start time: {shift_start}")
        print(f"End time: {shift_end}")
        print(f"Duration: {duration.total_seconds() / 3600:.2f} hrs")
        print("Logs:")
        for log in logs:
            print(log)
            print("it ")
        
        time.sleep(5)

if __name__ == "__main__":
    monitor_shifts()
