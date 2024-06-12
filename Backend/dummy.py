from flask import Flask, jsonify, request
import pyodbc
import time

# Define the database configuration
config = {
    'driver': 'SQL Server',
    'server': 'localhost',
    'database': 'ITP_1482',
    'username': 'RAJ',
    'password': 'root@123',
    'port': 1433
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

    conn.close()

    # Prepare the data for response
    data = {}

    for row in data_rows:
        row_id, column_name, value = row

        column_desc = column_names.get(column_name, column_name)
        if row_id in data:
            data[row_id][column_desc] = value
        else:
            data[row_id] = {'id': row_id, column_desc: value}

    return list(data.values())

@app.route('/data', methods=['GET'])
def get_data():
    data = fetch_data()
    return jsonify(data)

@app.route('/add-data', methods=['POST'])
def add_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    data = request.get_json()

    # Prepare the INSERT query
    columns = ', '.join(data.keys())
    values = ', '.join(['?'] * len(data))
    query = f"INSERT INTO dbo.GW_2_P10_HR_2024 ({columns}) VALUES ({values})"
    values_list = list(data.values())

    try:
        cursor.execute(query, values_list)
        conn.commit()
        return jsonify({'message': 'Data added successfully'})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    while True:
        app.run(debug=True, use_reloader=False)
        time.sleep(5)  # Wait for 5 seconds before checking the database again