# -*- coding: utf-8 -*-
"""formatted_zone

## Build relational model based on data from persistent zone
"""

# Required libraries
import os
import pandas as pd
import psycopg2
import shutil
import subprocess


# Constants
TEMPORAL_PATH = "/content/drive/MyDrive/ADSDB/landing/temporal"
PERSISTENT_PATH = "/content/drive/MyDrive/ADSDB/landing/persistent"
SQL_PATH = "/content/drive/MyDrive/ADSDB/formatted/sql"
INFLATION_FILENAME = "inflation_rate"
EMPLOYMENT_FILENAME = "employment_rate"
HPI_FILENAME = "house_price_index"
HPI_WEIGHTS_FILENAME = "house_price_index_weights"
FILENAMES = [INFLATION_FILENAME, EMPLOYMENT_FILENAME, HPI_FILENAME, HPI_WEIGHTS_FILENAME]

# Mount Google Drive to Colab
#from google.colab import drive
#drive.mount('/content/drive')

#Install PostgreSQL

import subprocess

# Replace !apt-get command
subprocess.run(["apt-get", "-y", "-qq", "install", "postgresql", "postgresql-contrib"], check=True)

# Replace !service command
subprocess.run(["service", "postgresql", "start"], check=True)

# Creating a superuser named adsdb
subprocess.run(["sudo", "-u", "postgres", "createuser", "--superuser", "adsdb"], check=True)

# Creating a database named adsdb
subprocess.run(["sudo", "-u", "postgres", "createdb", "adsdb"], check=True)

# Setting the password for the user adsdb to 'adsdb'
subprocess.run(["sudo", "-u", "postgres", "psql", "-c", "ALTER USER adsdb WITH PASSWORD 'adsdb';"], check=True)

# Database connection function
def connect_to_database():
    connection = psycopg2.connect(
      host="localhost",
      database="adsdb",
      user="adsdb",
      password="adsdb"
    )
    return connection.cursor(), connection

# Reading a CSV from persistent folder
def read_persistent_csv(year, filename):
    return pd.read_csv(f"{PERSISTENT_PATH}/{year}/{filename}_{year}.csv")

# Predefined structures to identify primary keys and any SERIAL columns
TABLE_STRUCTURES = {
    "inflation_rate": {
        "primary_key": "year",
    },
    "employment_rate": {
        "primary_key": "id",
        "serials": ["id"]
    },
    "house_price_index": {
        "primary_key": "id",
        "serials": ["id"]
    },
    "house_price_index_weights": {
        "primary_key": "id",
        "serials": ["id"]
    }
}

def generate_sql_create_table(df, table_name, year):
    dtype_mapping = {
        'int64': 'INT',
        'float64': 'DECIMAL(5,2)',
        'object': 'VARCHAR(100)'  # This sets VARCHAR as default, adjust if necessary
    }
    structures = TABLE_STRUCTURES.get(table_name, {})

    columns = []

    # If table is inflation_rate, set year as primary key
    if table_name == "inflation_rate":
        columns.append("year INT PRIMARY KEY")
    # Otherwise, add auto-incremented id as primary key
    else:
        columns.append("id SERIAL PRIMARY KEY")
        if 'year' in df.columns:
            columns.append("year INT")

    for col in df.columns:
        column_type = dtype_mapping[str(df[col].dtype)]

        # Avoid re-adding the column if it's already added as a primary key or year
        if col in ["id", "year"]:
            continue

        columns.append(f"{col} {column_type}")

    # If another primary key is defined (besides year or id), create a unique constraint for it
    if "primary_key" in structures and structures["primary_key"] not in ["id", "year"]:
        columns.append(f"UNIQUE ({structures['primary_key']})")

    final_table_name = f"{table_name}_{year}"
    sql_command = f"CREATE TABLE {final_table_name} ({', '.join(columns)});"

    return sql_command


# Save SQL Create Table command to a file
def save_sql_to_file(sql_command, filename):
    if not os.path.exists(SQL_PATH):
        os.makedirs(SQL_PATH)
    with open(f"{SQL_PATH}/{filename}.sql", 'w') as file:
        file.write(sql_command)

def store_data_in_postgres(df, table_name, year, cursor, connection):
    final_table_name = f"{table_name}_{year}"
    df_columns = list(df)
    columns = ",".join(df_columns)
    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

    updates = ",".join([f"{col}=EXCLUDED.{col}" for col in df_columns if col not in ["id", "year"]])
    insert_stmt = f"""
        INSERT INTO {final_table_name} ({columns}) {values}
        ON CONFLICT ({TABLE_STRUCTURES[table_name]['primary_key']})
        DO UPDATE SET {updates}
    """

    try:
        cursor.executemany(insert_stmt, df.values)
    except Exception as e:
        print(f"Error inserting into table {final_table_name}: {e}")
        connection.rollback()  # Rollback changes in case of error
    else:
        connection.commit()

def list_tables(cursor):
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';""")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"In total, we have {len(tables)} tables in the 'adsdb' database.")
    return tables

def table_stats(cursor, tablename):
    # Fetch the number of rows in the table
    cursor.execute(f"SELECT COUNT(*) FROM {tablename};")
    num_rows = cursor.fetchone()[0]

    # Fetch the number of columns in the table
    cursor.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{tablename}';")
    num_columns = cursor.fetchone()[0]

    # Fetch the "head" of the table (first 5 rows as an example)
    cursor.execute(f"SELECT * FROM {tablename} LIMIT 5;")
    head_rows = cursor.fetchall()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{tablename}';")
    columns = [col[0] for col in cursor.fetchall()]

    table_head = pd.DataFrame(head_rows, columns=columns)

    return {
        "table": tablename,
        "num_rows": num_rows,
        "num_columns": num_columns,
        "head": table_head
    }


def save_to_drive(dump_path, drive_path):
    try:
        # Try to copy the dump file to the specified drive path
        shutil.copy(dump_path, drive_path)
        # Remove the original dump file after copying
        os.remove(dump_path)
    except Exception as e:
        print(f"Error saving to Google Drive: {e}")

def main():
    cursor, connection = connect_to_database()

    for filename in FILENAMES:
        years = [folder for folder in os.listdir(PERSISTENT_PATH) if folder.isdigit()]
        for year in years:
            df = read_persistent_csv(year, filename)

            # Generate SQL table creation command with year-specific name
            sql_command = generate_sql_create_table(df, filename, year)

            # Save to file (only once for each data source, not for each year)
            if year == years[0]:
                save_sql_to_file(sql_command, filename)

            final_table_name = f"{filename}_{year}"

            # Create the table in PostgreSQL (only if the table doesn't exist)
            cursor.execute(f"SELECT to_regclass('{final_table_name}');")
            if cursor.fetchone()[0] is None:
                try:
                    cursor.execute(sql_command)
                except Exception as e:
                    print(f"Error creating table {final_table_name}: {e}")
                    continue  # Skip to the next iteration if there's an error

            store_data_in_postgres(df, filename, year, cursor, connection)

    # List tables
    tables = list_tables(cursor)

    # Collect and display statistics for each table
    for tablename in tables:
        stats = table_stats(cursor, tablename)
        print(f"\nStatistics for table {tablename}:")
        print(f"Number of rows: {stats['num_rows']}")
        print(f"Number of columns: {stats['num_columns']}")
        print("Head of the table:")
        print(stats['head'])
        print("-" * 50)  # Separator

    # Path for the dump file
    dump_path = "/content/dumpfile.sql"
    drive_path = "/content/drive/MyDrive/ADSDB/formatted/dumpfile.sql"

    # Create a temporary .pgpass file for authentication
    with open("/root/.pgpass", "w") as f:
        f.write("*:*:*:adsdb:adsdb")
    os.chmod("/root/.pgpass", 0o600)  # Set the required permissions

    # Use the pg_dump command to dump the data
    try:
        subprocess.run(['PGPASSFILE=/root/.pgpass', 'pg_dump', '-h', 'localhost', '-U', 'adsdb', '-d', 'adsdb', '-f', dump_path], shell=True)

        # If successful, save the dump to Google Drive
        save_to_drive(dump_path, drive_path)
    except Exception as e:
        print(f"Error during pg_dump: {e}")

    # Clean up the temporary password file
    os.remove("/root/.pgpass")


    connection.close()

if __name__ == "__main__":
    main()