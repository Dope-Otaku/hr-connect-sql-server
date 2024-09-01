from flask import Flask, jsonify, request
import pyodbc
import time
from datetime import datetime

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

def fetch_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch data from the GW_2_P10_HR_2024 table
    cursor.execute("SELECT * FROM dbo.GW_2_P10_HR_2024")
    data_rows = cursor.fetchall()

    # Fetch column names from the GW_2_P10_HR_2023_Ref table
    cursor.execute("SELECT ColumnName, ColumnNameAbb FROM dbo.GW_2_P10_HR_2023_Ref")
    column_names = {row.ColumnName: row.ColumnNameAbb for row in cursor.fetchall()}

    # Fetch data from the CT0013 table
    cursor.execute("SELECT * FROM dbo.CT0013")
    ct0013_rows = cursor.fetchall()


    conn.close()

    # Prepare the data for response
    data = {}

    current_ct0013_row = ct0013_rows[0]  # Start with the first row from CT0013

    for row in data_rows:
        row_id, column_name, value = row

        column_desc = column_names.get(column_name, column_name)
        if row_id in data:
            data[row_id][column_desc] = value
        else:
            data[row_id] = {'id': row_id, column_desc: value}

        # Check if the current time is outside the FromTime and ToTime range
        if column_name == 'C001':  # Assuming 'C001' is the DateTime column
            current_time = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
            from_time = datetime.strptime(f"{current_ct0013_row[1]} {current_ct0013_row[12]}", '%Y-%m-%d %H:%M')
            to_time = datetime.strptime(f"{current_ct0013_row[1]} {current_ct0013_row[13]}", '%Y-%m-%d %H:%M')

            if current_time < from_time or current_time >= to_time:
                # Move to the next row in CT0013
                current_ct0013_row = ct0013_rows[(ct0013_rows.index(current_ct0013_row) + 1) % len(ct0013_rows)]

    return list(data.values()), current_ct0013_row

@app.route('/', methods=['GET'])
def get_data():
    data = fetch_data()
    return jsonify(data)

# @app.route('/add-data', methods=['POST'])
# def add_data():
#     conn = get_db_connection()
#     cursor = conn.cursor()
#     data = request.get_json()

#     # Prepare the INSERT query
#     columns = ', '.join(data.keys())
#     values = ', '.join(['?'] * len(data))
#     query = f"INSERT INTO dbo.GW_2_P10_HR_2024 ({columns}) VALUES ({values})"
#     values_list = list(data.values())

#     try:
#         cursor.execute(query, values_list)
#         conn.commit()
#         return jsonify({'message': 'Data added successfully'})
#     except Exception as e:
#         conn.rollback()
#         return jsonify({'error': str(e)}), 500
#     finally:
#         conn.close()

if __name__ == '__main__':
    while True:
        app.run(debug=True, use_reloader=False)
        time.sleep(5)  # Wait for 5 seconds before checking the database again