import pyodbc
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import time
import csv
import tempfile
import multiprocessing
from functools import partial

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
                print(f"Query result for {object_name} (UniqueID: {unique_id}):")
                print(f"  Value: {row.Value}, TimeStamp: {row.TimeStamp}, ParameterID: {row.ParameterID}")
            else:
                print(f"No data found for {object_name} (UniqueID: {unique_id})")

        return object_values

    except Exception as e:
        print(f"An error occurred in get_object_values: {e}")
        return None

    finally:
        if conn:
            conn.close()

def cal_for_table(paramId):
    cons_start = 1
    const_end = 55
    xp = 50
    yp = (int(paramId) - 1)

    new_start = cons_start + (xp * yp)
    new_end = const_end + (xp * yp)

    start_formatted = f"C{new_start:03d}"
    end_formatted = f"C{new_end:03d}"

    # print(f"calculation: {start_formatted} to {end_formatted}")

    return (start_formatted, end_formatted)


def print_range_with_values(start, end, object_values):
    # Always print C001 to C005 with None values
    # for i in range(1, 6):
    #     formatted = f"C{i:03d}"
    #     print(f"{formatted}: None")

    start_num = int(start[1:])
    end_num = int(end[1:])

    # Print C001 to C005 only if the start is C006
    if start_num == 6:
        for i in range(1, 6):
            formatted = f"C{i:03d}"
            print(f"{formatted}: None")
    
    for i in range(start_num, end_num + 1):
        formatted = f"C{i:03d}"
        
        if i == start_num + 1:  # Second C-code (e.g., C007 for parameter 1)
            value = object_values[2]['Value'] if len(object_values) > 2 else None  # DOWNTIME
        elif i == start_num + 4:  # Third C-code (e.g., C008 for parameter 1)
            value = object_values[0]['Value'] if len(object_values) > 0 else None  # PRODUCT
        elif i == start_num + 2:  # Third C-code (e.g., C008 for parameter 1)
            value = object_values[1]['Value'] if len(object_values) > 0 else None  # PRODUCT_Count
        else:
            value = None
        
        print(f"{formatted}: {value}")


def get_shift_data(cursor, current_time):
    query = """
    SELECT TOP 1 Value
    FROM CT0013  
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


def cal_dur(start_time, end_time):
    print(f"cal_dur input - start_time: {start_time}, end_time: {end_time}")
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
            print("Current time is past end time, returning 0")
            return 0
        else:
            duration = current - start
            total_seconds = int(duration.total_seconds())
            result = min(total_seconds, 3600)  # Cap at 3600 seconds
            print(f"Calculated duration: {result} ")
            return result
    except ValueError as e:
        print(f"Error in cal_dur: {e}")
        return 'Invalid time format'




def insert_or_update_data(paramID, object_values):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Get current date and time
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")
    
    # Set end time to the next hour
    end_time = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).strftime("%H:%M")

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

        # Get values from object_values
        downtime = next((item['Value'] for item in object_values if item['Object'] == 'DOWNTIME'), None)
        product_type = next((item['Value'] for item in object_values if item['Object'] == 'PRODUCT_TYPE'), None)
        product_count = next((item['Value'] for item in object_values if item['Object'] == 'PRODUCT'), None)


        #get duration
        duration = cal_dur(start_time_str, end_time_str)

        #plan
        cycle_time = 60
        plan = (str(int(duration) // cycle_time))
        # Get dynamic shift data
        shift_name = get_shift_data(cursor, now.strftime("%H:%M"))

        if existing_id:
            # Update existing entry
            existing_id = existing_id[0]
            print(f"Updating existing entry with Id: {existing_id}")
            
            update_data = [
                ('C001', f"{current_date} {now.strftime('%H:%M')}:00"),
                ('C004', end_time),
                ('C007', downtime),
                ('C008', product_type),
                ('C010', product_count)
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
                ('C007', downtime),
                ('C008', product_type),
                ('C009', duration),  # You might want to implement duration calculation
                ('C010', product_count),
                ('C011', 'TPM'),
                ('C012', plan),  # You might want to implement plan calculation
                ('C013', downtime)
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


#approach 1 batch pushing
# def batch_insert_with_temp_table(param_ids):
#     conn = pyodbc.connect(conn_str)
#     cursor = conn.cursor()
    
#     try:
#         # Create a temporary table
#         cursor.execute("""
#             CREATE TABLE #TempHRData (
#                 Id INT,
#                 ColumnName VARCHAR(10),
#                 Value VARCHAR(100)
#             )
#         """)
        
#         for param_id in param_ids:
#             data = collect_data_for_param(param_id, cursor)
            
#             print(f"\nInserting data for param_id {param_id}:")
#             for row in data:
#                 print(f"  {row[1]}: {row[2]}")
            
#             cursor.executemany("""
#                 INSERT INTO #TempHRData (Id, ColumnName, Value)
#                 VALUES (?, ?, ?)
#             """, data)
        
#         cursor.execute("""
#             INSERT INTO GW_2_P10_HR_2024_old (Id, ColumnName, Value)
#             SELECT * FROM #TempHRData
#         """)
        
#         conn.commit()
#         print("Batch insert completed successfully.")
#     except Exception as e:
#         conn.rollback()
#         print(f"An error occurred during batch insert: {e}")
#     finally:
#         cursor.close()
#         conn.close()

def batch_insert_with_temp_table(param_ids):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    try:
        # Create a temporary table
        cursor.execute("""
            CREATE TABLE #TempHRData (
                Id INT,
                ColumnName VARCHAR(10),
                Value VARCHAR(100)
            )
        """)
        
        # Get the maximum ID from the main table
        cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
        max_id = cursor.fetchone()[0]
        new_id = (max_id or 0) + 1

        print(f"Collecting data with ID: {new_id}")
        
        all_data = []
        start_c_num = 1  # Start from C001
        for param_id in param_ids:
            print(f"\nCollecting data for param_id {param_id}:")
            data, start_c_num = collect_data_for_param(param_id, new_id, cursor, start_c_num)
            all_data.extend(data)
        
        print("\nInserting all collected data:")
        cursor.executemany("""
            INSERT INTO #TempHRData (Id, ColumnName, Value)
            VALUES (?, ?, ?)
        """, all_data)
        
        # Insert from temp table to main table
        cursor.execute("""
            INSERT INTO GW_2_P10_HR_2024_old (Id, ColumnName, Value)
            SELECT * FROM #TempHRData
        """)
        
        conn.commit()
        print("Batch insert completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred during batch insert: {e}")
    finally:
        cursor.close()
        conn.close()


#approach 2 with csv
def bulk_insert_with_csv(param_ids):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, newline='') as temp_file:
            csv_writer = csv.writer(temp_file)
            
            for param_id in param_ids:
                data = collect_data_for_param(param_id)
                csv_writer.writerows(data)
            
            temp_file_path = temp_file.name
        
        cursor.execute(f"""
            BULK INSERT GW_2_P10_HR_2024_old
            FROM '{temp_file_path}'
            WITH (
                FIELDTERMINATOR = ',',
                ROWTERMINATOR = '\\n',
                FIRSTROW = 1
            )
        """)
        
        conn.commit()
        print("Bulk insert completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred during bulk insert: {e}")
    finally:
        cursor.close()
        conn.close()
        os.unlink(temp_file_path)

#approch 3 with multithreading
def async_insert_with_multiprocessing(param_ids):
    pool = multiprocessing.Pool()
    
    try:
        worker_func = partial(process_param_id, conn_str=conn_str)
        pool.map(worker_func, param_ids)
        print("Asynchronous insert completed successfully.")
    except Exception as e:
        print(f"An error occurred during asynchronous insert: {e}")
    finally:
        pool.close()
        pool.join()

def process_param_id(param_id, conn_str):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    try:
        data = collect_data_for_param(param_id, cursor)
        
        cursor.executemany("""
            INSERT INTO GW_2_P10_HR_2024_old (Id, ColumnName, Value)
            VALUES (?, ?, ?)
        """, data)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"An error occurred for param_id {param_id}: {e}")
    finally:
        cursor.close()
        conn.close()

# def collect_data_for_param(param_id, cursor):
#     now = datetime.now()
#     current_date = now.strftime("%Y-%m-%d")
#     current_time = now.strftime("%H:%M")
#     end_time = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).strftime("%H:%M")

#     param_results, object_value_table = param_combi(param_id)
#     print(param_results)
#     object_values = get_object_values(param_results, object_value_table)

#     # Create a dictionary to store values with default None
#     values = {
#         'DOWNTIME': None,
#         'PRODUCT_TYPE': None,
#         'PRODUCT': None
#     }


# #just for debugging 
#     downtime = next((item['Value'] for item in object_values if item['Object'] == 'DOWNTIME'), None)
#     product_type = next((item['Value'] for item in object_values if item['Object'] == 'PRODUCT_TYPE'), None)
#     product_count = next((item['Value'] for item in object_values if item['Object'] == 'PRODUCT'), None)

#     print("object_values:", object_values)
#     print("downtime:", downtime)
#     print("product_type:", product_type)
#     print("product_count:", product_count)

#     shift_name = get_shift_data(cursor, now.strftime("%H:%M"))

#     cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
#     max_id = cursor.fetchone()[0]
#     new_id = (max_id or 0) + 1

#     data = [
#         (new_id, 'C001', f"{current_date} {now.strftime('%H:%M')}:00"),
#         (new_id, 'C002', current_date),
#         (new_id, 'C003', current_time),
#         (new_id, 'C004', end_time),
#         (new_id, 'C005', shift_name),
#         (new_id, 'C006', '0'),
#         (new_id, 'C007', downtime),
#         (new_id, 'C008', product_type),
#         (new_id, 'C009', '0'),
#         (new_id, 'C010', product_count),
#         (new_id, 'C011', 'TPM'),
#         (new_id, 'C012', '0'),
#         (new_id, 'C013', downtime)
#     ]

#     return data

# def collect_data_for_param(param_id, cursor):
#     now = datetime.now()
#     current_date = now.strftime("%Y-%m-%d")
#     current_time = now.strftime("%H:%M")
#     end_time = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).strftime("%H:%M")

#     param_results, object_value_table = param_combi(param_id)
#     object_values = get_object_values(param_results, object_value_table)

#     # Initialize variables
#     product_count = None
#     product_type = None
#     downtime = None

#     # Assign values based on their position in the list
#     if len(object_values) >= 3:
#         product_count = object_values[0]['Value']
#         product_type = object_values[1]['Value']
#         downtime = object_values[2]['Value']

#     # Print the values with their respective names
#     print(f"Product Count: {product_count}")
#     print(f"Product Type: {product_type}")
#     print(f"Downtime: {downtime}")

#     shift_name = get_shift_data(cursor, now.strftime("%H:%M"))

#     cursor.execute("SELECT MAX(Id) FROM GW_2_P10_HR_2024_old")
#     max_id = cursor.fetchone()[0]
#     new_id = (max_id or 0) + 1

#     data = [
#         (new_id, 'C001', f"{current_date} {now.strftime('%H:%M')}:00"),
#         (new_id, 'C002', current_date),
#         (new_id, 'C003', current_time),
#         (new_id, 'C004', end_time),
#         (new_id, 'C005', shift_name),
#         (new_id, 'C006', '0'),
#         (new_id, 'C007', str(downtime) if downtime is not None else None),
#         (new_id, 'C008', str(product_type) if product_type is not None else None),
#         (new_id, 'C009', '0'),
#         (new_id, 'C010', str(product_count) if product_count is not None else None),
#         (new_id, 'C011', 'TPM'),
#         (new_id, 'C012', '0'),
#         (new_id, 'C013', str(downtime) if downtime is not None else None)
#     ]

#     return data

def collect_data_for_param(param_id, new_id, cursor, start_c_num):
    param_results, object_value_table = param_combi(param_id)
    object_values = get_object_values(param_results, object_value_table)
    
    # Initialize variables
    product_count = product_type = downtime = start_time = duration = plan = None
    

    # Assign values based on their position in the list
    if len(object_values) >= 3:
        product_count = object_values[0]['Value']
        product_type = object_values[1]['Value']
        downtime = object_values[2]['Value']
        start_time = object_values[0]['TimeStamp']

    if start_time is None:
        start_time = datetime.now()

    current_date, start_time_str = str(start_time).split()
    start_time_str = start_time_str.split('.')[0]
    original_time_obj = datetime.strptime(start_time_str, "%H:%M:%S")
    start_time_obj = original_time_obj.replace(minute=0, second=0, microsecond=0)
    start_time_str = start_time_obj.strftime("%H:%M")
    end_time_obj = start_time_obj + timedelta(hours=1)
    end_time_str = end_time_obj.strftime("%H:%M")

    shift_name = get_shift_data(cursor, start_time_str)
    
    print(f"start_time_str: {start_time_str}, end_time_str: {end_time_str}")
    duration = cal_dur(start_time_str, end_time_str)
    print(f"Raw duration: {duration}")
    cycle_time = 60

    if isinstance(duration, (int, float)):
        plan = str(int(duration) // cycle_time)
    elif isinstance(duration, str) and duration.isdigit():
        plan = str(int(duration) // cycle_time)
    else:
        plan = '0'
        print(f"Duration is not a number: {duration}")

    print(f"{plan}:plan, {duration}:duration")

    data = []

    for i in range(55):  # Always generate 55 values
        c_code = f'C{start_c_num + i:03d}'
        value = None

        if i == 0:
            value = f"{start_time}"
        elif i == 1:
            value = current_date
        elif i == 2:
            value = start_time_str
        elif i == 3:
            value = end_time_str
        elif i == 4:
            value = shift_name
        elif i == 5:
            value = '0'
        elif i == 6:
            value = str(downtime) if downtime is not None else None
        elif i == 7:
            value = str(product_type) if product_type is not None else None
        elif i == 8:
            value = f"{str(duration)}" if downtime is not None else None
        elif i == 9:
            value = str(product_count) if product_count is not None else None
        elif i == 10:
            value = 'TPM'
        elif i == 11:
            value = plan
        elif i == 12:
            value = str(downtime) if downtime is not None else None

        data.append((new_id, c_code, value))
        print(f"{c_code}: {value}")  # Print each C-code and its value

    return data, start_c_num + 55  # Return the next start_c_num





if __name__ == "__main__":
    param_ids = range(1, 10)

   
    #currently this one works only the other two are not tested yet!
    print("Starting batch insert with temporary table...")
    start_time = time.time()
    batch_insert_with_temp_table(param_ids)
    print(f"Batch insert completed in {time.time() - start_time:.2f} seconds")

    # print("\nStarting bulk insert with CSV...")
    # start_time = time.time()
    # bulk_insert_with_csv(param_ids)
    # print(f"Bulk insert completed in {time.time() - start_time:.2f} seconds")

    # print("\nStarting asynchronous insert with multiprocessing...")
    # start_time = time.time()
    # async_insert_with_multiprocessing(param_ids)
    # print(f"Asynchronous insert completed in {time.time() - start_time:.2f} seconds")