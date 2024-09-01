# DRIVER  using driver approach since this is the only working for me
import pyodbc

try:
    conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={0}; database={1}; \
       trusted_connection=yes;UID={2};PWD={3}".format('localhost','ITP_1482','RAJ','root@123'))
    cursor = conn.cursor()
    # Define and execute the query
    query = "SELECT * FROM dbo.GW_1_P10_HR_2024_Ref"
    cursor.execute(query)

    # Fetch all rows from the executed query
    rows = cursor.fetchall()

    # Print the fetched rows
    for row in rows:
        print(row)

except pyodbc.Error as err:
    print("Error: ", err)

finally:
    # Close the connection
    if conn:
        conn.close()
        print("Connection closed.")