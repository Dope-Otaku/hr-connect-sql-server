from flask import Flask, jsonify, request
import pyodbc

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


#gw_2 table in use
@app.route('/data', methods=['GET'])  #this endpoint will fetch all the data from the table/database
def get_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.GW_2_P10_HR_2024")
    rows = cursor.fetchall()
    conn.close()
    data = [dict(zip([column[0] for column in cursor.description], row)) for row in rows]
    return jsonify(data)

@app.route('/data', methods=['POST']) #this endpoint will add or create a new row into database
def add_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    data = request.get_json()
    columns = ', '.join(data.keys())
    values = ', '.join(['?'] * len(data))
    query = f"INSERT INTO dbo.GW_1_P10_HR_2024_Ref ({columns}) VALUES ({values})"
    values_list = list(data.values())
    cursor.execute(query, values_list)
    conn.commit()
    conn.close()
    return jsonify({'message': 'Data added successfully'})

@app.route('/data/<string:column_name>', methods=['PUT'])  #this endpoint will update any 
def update_data(column_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    data = request.get_json()
    set_clause = ', '.join([f"{column} = ?" for column in data.keys()])
    values_list = list(data.values()) + [column_name]
    query = f"UPDATE dbo.GW_1_P10_HR_2024_Ref SET {set_clause} WHERE columnName = ?"
    cursor.execute(query, values_list)
    conn.commit()
    conn.close()
    return jsonify({'message': 'Data updated successfully'})

@app.route('/data/<string:column_name>', methods=['DELETE'])
def delete_data(column_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = f"DELETE FROM dbo.GW_1_P10_HR_2024_Ref WHERE columnName = ?"
    cursor.execute(query, [column_name])
    conn.commit()
    conn.close()
    return jsonify({'message': 'Data deleted successfully'})

if __name__ == '__main__':
    app.run(debug=True)