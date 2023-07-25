import sys
import os
import sqlite3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import bcrypt

from multidbconnector import MultiDBConnector

def detect_column_type(series):
    if pd.api.types.is_string_dtype(series):
        return 'string'
    elif pd.api.types.is_numeric_dtype(series):
        return 'number'
    elif pd.api.types.is_datetime64_any_dtype(series):
        return 'datetime'
    else:
        return 'unknown'

def calculate_descriptive_stats(series):
    stats = {}

    if pd.api.types.is_numeric_dtype(series):
        stats['min'] = series.min()
        stats['max'] = series.max()
        # Add more numeric statistics as needed
    elif pd.api.types.is_string_dtype(series):
        stats['min_length'] = series.str.len().min()
        stats['max_length'] = series.str.len().max()
        # Add more string statistics as needed
    elif pd.api.types.is_datetime64_any_dtype(series):
        stats['min_date'] = series.min()
        stats['max_date'] = series.max()
        # Add more datetime statistics as needed
    return stats


def store_db_credentials(db_type, host, port, database, username, password):
    # Hash the password before storing it in the database
    password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    # Connect to the SQLite database
    conn = sqlite3.connect('database_credentials.db')
    cursor = conn.cursor()

    # Insert the credentials into the table
    cursor.execute('''INSERT INTO db_credentials 
                      (db_type, host, port, database, username, password_hash) 
                      VALUES (?, ?, ?, ?, ?, ?)''',
                   (db_type, host, port, database, username, password_hash))

    # Commit and close the database connection
    conn.commit()
    conn.close()


def read_db_credentials(db_type):
    # Connect to the SQLite database
    conn = sqlite3.connect('database_credentials.db')
    cursor = conn.cursor()

    # Retrieve the credentials for the given db_type
    cursor.execute('''SELECT host, port, database, username, password_hash FROM db_credentials WHERE db_type=?''',
                   (db_type,))
    row = cursor.fetchone()

    # Commit and close the database connection
    conn.commit()
    conn.close()

    if row is not None:
        host, port, database, username, password_hash = row
        return host, port, database, username, password_hash
    else:
        print(f"No credentials found for {db_type}.")
        sys.exit(1)


def create_db_connection():
    # Create a connection to the SQLite database (create one if it doesn't exist)
    conn = sqlite3.connect('database_credentials.db')
    cursor = conn.cursor()

    # Create a table to store database credentials
    cursor.execute('''CREATE TABLE IF NOT EXISTS db_credentials (
                        id INTEGER PRIMARY KEY,
                        db_type TEXT NOT NULL,
                        host TEXT NOT NULL,
                        port TEXT NOT NULL,
                        database TEXT NOT NULL,
                        username TEXT NOT NULL,
                        password_hash TEXT NOT NULL
                    )''')

    # Commit and close the database connection
    conn.commit()
    conn.close()


def profile_data_source(source_type, source_path_or_db_connector, table_name=None):
    if source_type == 'file':
        # Read the text file into a pandas DataFrame
        df = pd.read_csv(source_path_or_db_connector)

        # Log basic file attributes
        file_name = os.path.basename(source_path_or_db_connector)
        file_size = os.path.getsize(source_path_or_db_connector)
        record_count = len(df)
        create_date = datetime.fromtimestamp(os.path.getctime(source_path_or_db_connector))
    elif source_type == 'database':
        # Get the MultiDBConnector instance from the argument
        db_connector = source_path_or_db_connector

        # Execute a query to fetch data from the table
        query = f"SELECT * FROM {table_name};"
        data = db_connector.execute_query(query)

        # Convert the query result to a pandas DataFrame
        df = pd.DataFrame(data, columns=[col[0] for col in db_connector.cursor.description])

        # Log basic table attributes
        create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    else:
        raise ValueError("Invalid source_type. Use 'file' or 'database'.")

    # Connect to the SQLite database to log profiling results
    conn = sqlite3.connect('data_profiling.db')
    cursor = conn.cursor()

    # Iterate through DataFrame columns and perform data profiling
    for col in df.columns:
        column_type = detect_column_type(df[col])
        stats = calculate_descriptive_stats(df[col])
        descriptive_stats_str = str(stats)

        # Log the results into the database
        cursor.execute('''INSERT INTO profiling_results 
                          (filename, size, record_count, create_date, table_name, column_name, column_type, descriptive_stats) 
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                       (file_name, file_size, record_count, create_date, table_name, col, column_type, descriptive_stats_str))

    # Commit and close the database connection
    conn.commit()
    conn.close()


def read_db_env():
    load_dotenv()  # Load environment variables from the .env file
    db_type = os.getenv("DB_TYPE")

    if None in (db_type, host, port, database, username, password):
        print("Database environment variables not set properly in .env file.")
        sys.exit(1)

    return db_type, host, port, database, username, password

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python dataprofiler.py <source_type> <table_name_or_db_type>")
        sys.exit(1)

    source_type = sys.argv[1]
    source_path_or_db_type = sys.argv[2]

    if source_type == 'file':
        # Profile the text file
        profile_data_source('file', source_path_or_db_type)
    elif source_type == 'database':
        host, port, database, username, password_hash = read_db_credentials(source_path_or_db_type)

        # Prompt the user for the password and verify it against the hashed password
        password = input("Enter your password: ")
        if bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
            # Create a MultiDBConnector instance and connect to the database
            db_connector = MultiDBConnector(source_path_or_db_type, host, port, database, username, password)
            db_connector.connect()

            # Profile the table and log the results
            profile_data_source('database', db_connector, source_path_or_db_type)

            # Close the database connection
            db_connector.close()
        else:
            print("Invalid password.")
            sys.exit(1)
    else:
        print("Invalid source_type. Use 'file' or 'database'.")
        sys.exit(1)