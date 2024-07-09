import pyodbc
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection configuration
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

def param_combi(paramID):
    try:
        # Establish a connection to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # SQL query to fetch data from MainTagList based on paramID
        query = """
        SELECT UniqueID, Object
        FROM MainTagList
        WHERE ParameterID = ?
        """

        # Execute the query with the provided paramID
        cursor.execute(query, (paramID,))

        # Fetch all rows from the result
        rows = cursor.fetchall()

        if not rows:
            return None, None

        # Convert the rows to a list of dictionaries
        result = [{'UniqueID': row.UniqueID, 'Object': row.Object} for row in rows]
        # uniqueID = [{row.UniqueID} for row in rows]
        # ObjectTable = [{row.Object} for row in rows]

        # return result, uniqueID, ObjectTable
        # return result

        # Get the ObjectTable name (assuming it's the same for all rows)
        object_table = rows[0].Object if rows else None

        return result, object_table


    except Exception as e:
        print(f"An error occurred in param_combi: {e}")
        return None, None

    finally:
        if conn:
            conn.close()


def get_object_values(param_results, object_value_table):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        object_values = []

        for item in param_results:
            unique_id = item['UniqueID']
            object_name = item['Object']

            # SQL query to fetch the latest value for the given UniqueID
            query = f"""
            SELECT TOP 1 Value, TimeStamp, ParameterID
            FROM [{object_value_table}]
            WHERE ParameterID = ?
            ORDER BY TimeStamp DESC
            """

            cursor.execute(query, (unique_id,))
            row = cursor.fetchone()

            if row:
                object_values.append({
                    'ParameterID': unique_id,
                    'Object': object_name,
                    'Value': row.Value,
                    'TimeStamp': row.TimeStamp
                })
            else:
                object_values.append({
                    'UniqueID': unique_id,
                    'Object': object_name,
                    'Value': None,
                    'TimeStamp': None
                })

        return object_values

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        if conn:
            conn.close()


def cal_for_table(paramId):
    cons_start = 50   #these are yet to be  dynamic
    xp = 6   #these are yet to be  dynamic
    yp = (int(paramId) - 1)

    calculation = cons_start + (xp * yp)
    print(f"calculation: {calculation}")


    return (calculation)





if __name__ == "__main__":
    paramID = 1  # Replace with the desired parameter ID
    
    cal_for_table(paramId=paramID)



    param_results, object_value_table = param_combi(paramID)
    
    if param_results and object_value_table:
        print(f"Using object value table: {object_value_table}")
        object_values = get_object_values(param_results, object_value_table)
        if object_values:
            # print(f"Object values for ParameterID {paramID}:")
            # for value in object_values:
            #     print(value)
            print(f"\nValues for ParameterID {paramID}:")
            names = ["PRODUCT", "PRODUCT_TYPE", "DOWNTIME"]
            for name, value in zip(names, object_values):
                print(f"{name:<15}: {value['Value']} (TimeStamp: {value['TimeStamp']})")
        else:
            print("No object values found or an error occurred.")
    else:
        print("No parameter results found or an error occurred.")