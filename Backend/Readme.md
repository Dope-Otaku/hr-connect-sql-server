# HR Table Calculation and Test Code

This repository contains code for all HR table calculations and their test code.

## Installation

1. **Create a Virtual Environment**:

   - Set up a virtual environment to manage dependencies.

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

2. **Install Requirements**:

   - Install the necessary Python packages from the `requirements.txt` file.

   ```bash
   pip install -r requirements.txt
   ```

3. **Install Microsoft SQL Server**:

   - Download and install Microsoft SQL Server from their [official website](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).

4. **Install Visual Studio Code**:

   - Download and install Visual Studio Code from their [official website](https://code.visualstudio.com/).

5. **Install Python**:

   - Ensure Python is installed on your system. Download it from the [official website](https://www.python.org/).

6. **Configure GitHub Profile**:
   - Set up your GitHub profile for CI/CD pipelining.

## Getting Started

1. **SQL Server Setup**:

   - Ensure you have SQL Server installed and running on your system.
   - Create a database within SQL Server to use with this project.

2. **Create a .env File**:

   - In the root directory of the project, create a `.env` file and add your SQL Server username and password:

   ```
   export USERNAME=your_username
   export PASSWORD=your_password
   ```

3. **Running The file**:

   - To run a file, simply execute the following command in the terminal:

   ```
    python <filename>.py || python3 <filename>.py `for mac and linux users`|| py <filename>.py

   ```

4. **Project Structure**:

   - The root directory contains several files and folders. Here's an explanation of each:

   - **Old_files**:

     - Contains all the old versions of files with their respective naming conventions.

   - **Testing**:

     - Contains all the test files for HR table calculations.

   - **Waste**:

     - Contains all the buggy code and code that is not currently useful.

   - **connect_sql.py**:

     - This file contains the connection code for SQL Server. Use this file to establish a connection to the SQL Server.

   - **Delete_ID.py**:

     - Contains the code to delete data from the HR table.

   - **insert_timer.py**:

     - Contains the code to insert data into the HR table with a timer. This file will run in a loop and rerun after an hour or so.

   - **paramID.py**:

     - Prints all the parameter IDs with PC, ID, etc., to the terminal.

   - **shift.py**:

     - Displays data from the HR table in the terminal or console.

   - **Param_Insert_Final.py**:
     - Contains the code to insert data into the HR table. This file is intended for production as it includes the logic implementation.

## Conclusion

This README provides the necessary steps and explanations to set up and understand the project. If you have any questions or need further assistance, feel free to reach out.
