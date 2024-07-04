from flask import Flask, jsonify, request
import time
from datetime import datetime, timedelta
import pyodbc
import threading

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
    SELECT ColumnName, Value, id
    FROM dbo.GW_2_P10_HR_2024
    WHERE ColumnName IN ('c009', 'c010')
    AND C001 BETWEEN '{shift_start_time}' AND '{shift_end_time}'
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    return rows
    # Get column names
    # cursor.execute("SELECT TOP 1 * FROM dbo.GW_2_P10_HR_2024")
    # columns = [column[0] for column in cursor.description]
    # print("Available columns:", columns)

def monitor_shifts():
    conn = get_db_connection()
    cursor = conn.cursor()

    initial_dur = None
    total_dur_diff = 0
    shift_duration_seconds = 3600
    
    while True:
        now = datetime.now()
        shift_start_time = now - timedelta(hours=1)
        
        logs = fetch_data(cursor, shift_start_time, now)
        
        shift_start = shift_start_time.strftime("%H:%M")
        shift_end = now.strftime("%H:%M")
        total_pc = 0
        
        for log in logs:
            column_name = log[0]
            value = log[1]
            timestamp = log[2]
            
            if column_name == 'c009':  # DUR value
                dur = value
                if initial_dur is None:
                    initial_dur = dur
                else:
                    dur_diff = dur - initial_dur
                    total_dur_diff += dur_diff
                    initial_dur = dur
            
            if column_name == 'c010':  # PC value
                total_pc += value
        
        # Convert total_dur_diff to hours and minutes
        total_dur_hours = total_dur_diff // 3600
        total_dur_minutes = (total_dur_diff % 3600) // 60
        
        print(f"Shift: Shift 1")
        print(f"Start time: {shift_start}")
        print(f"End time: {shift_end}")
        print(f"Total DUR: {total_dur_hours} hrs {total_dur_minutes} mins")
        print(f"Total PC: {total_pc}")
        
        # Reset for the next shift
        initial_dur = None
        total_dur_diff = 0
        
        # Sleep until the next shift (1 hour)
        time.sleep(shift_duration_seconds)

@app.route('/add_data', methods=['POST'])
def add_data():
    data = request.json
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Insert data into ct0013 table
        cursor.execute('''
            INSERT INTO dbo.ct0013 (C001_FromTime, C002_ToTime, C003_TimeDisplay, C004_Shift, C005_Remark, C006_BreakTime)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (data['from_time'], data['to_time'], data['time_display'], data['shift'], data['remark'], data['break_time']))
        
        # Get the ID of the last inserted row
        cursor.execute("SELECT @@IDENTITY AS ID")
        ct0013_id = cursor.fetchone()[0]
        
        # Insert data into GW_2_P10_HR_2024 table
        current_time = datetime.now()
        for i in range(8):  # Assuming 8 entries per hour as in your example
            cursor.execute('''
                INSERT INTO dbo.GW_2_P10_HR_2024 (C001, C002, C003, C004, C005, C006, C007, C008, C009, C010, C011, C012, C013)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                current_time.strftime("%Y-%m-%d %H:%M:%S"),
                current_time.strftime("%Y-%m-%d"),
                data['from_time'],
                data['to_time'],
                data['shift'],
                0,  # C006
                data.get('c007_value', 0),  # You may need to adjust this
                1,  # C008
                data.get('c009_value', 0),  # You may need to adjust this
                data.get('c010_value', 0),  # You may need to adjust this
                data['remark'],
                40,  # C012
                data.get('c007_value', 0)  # C013, same as C007
            ))
            current_time += timedelta(minutes=7.5)  # Increment time for next entry
        
        conn.commit()
        return jsonify({"message": "Data added successfully", "ct0013_id": ct0013_id}), 200
    
    except pyodbc.Error as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    
    finally:
        conn.close()

@app.route('/get_latest_data', methods=['GET'])
def get_latest_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get the latest entry from ct0013 table
        cursor.execute('SELECT TOP 1 * FROM dbo.ct0013 ORDER BY id DESC')
        latest_ct0013 = cursor.fetchone()
        
        if latest_ct0013:
            # Get corresponding entries from GW_2_P10_HR_2024 table
            cursor.execute('''
                SELECT TOP 8 * FROM dbo.GW_2_P10_HR_2024 
                WHERE C003 = ? AND C004 = ? 
                ORDER BY C001 DESC
            ''', (latest_ct0013.C001_FromTime, latest_ct0013.C002_ToTime))
            hr_entries = cursor.fetchall()
            
            return jsonify({
                "ct0013": {column[0]: value for column, value in zip(cursor.description, latest_ct0013)},
                "hr_entries": [{column[0]: value for column, value in zip(cursor.description, row)} for row in hr_entries]
            }), 200
        else:
            return jsonify({"message": "No data found"}), 404
    
    except pyodbc.Error as e:
        return jsonify({"error": str(e)}), 500
    
    finally:
        conn.close()

if __name__ == '__main__':
    # Start the monitoring thread
    monitoring_thread = threading.Thread(target=monitor_shifts)
    monitoring_thread.start()
    
    # Run the Flask app
    app.run(debug=True)