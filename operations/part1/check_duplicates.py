# -*- coding: utf-8 -*-
"""check_duplicates.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1NKnExN-ExokxXQqTqA32o9TGzmEzBMzb
"""

# Import necessary libraries
import os
import shutil


# Mount Google Drive to Colab
#from google.colab import drive
#drive.mount('/content/drive')


# Install and Setup PostgreSQL
import subprocess

# Replace !apt-get command
subprocess.run(["apt-get", "-y", "-qq", "install", "postgresql", "postgresql-contrib"], check=True)

# Replace !service command
subprocess.run(["service", "postgresql", "start"], check=True)

#try:
#    subprocess.run(["sudo", "-u", "postgres", "createuser", "--superuser", "adsdb"], check=True)
#except subprocess.CalledProcessError:
#    print("The user 'adsdb' already exists.")

# Creating a database named adsdb
#subprocess.run(["sudo", "-u", "postgres", "createdb", "adsdb"], check=True)

# Setting the password for the user adsdb to 'adsdb'
#subprocess.run(["sudo", "-u", "postgres", "psql", "-c", "ALTER USER adsdb WITH PASSWORD 'adsdb';"], check=True)

drive_path = "/content/drive/MyDrive/ADSDB/trusted/dumpfile.sql"
dump_path= "/content/dumpfile.sql"
# Check if the dumpfile exists in the Google Drive
if not os.path.exists(drive_path):
  print("Dump file not found in Google Drive.")


# Copy the dumpfile from Google Drive to Colab's environment
shutil.copy(drive_path, dump_path)
# Create a temporary .pgpass file for authentication
with open("/root/.pgpass", "w") as f:
  f.write("*:*:*:adsdb:adsdb")
os.chmod("/root/.pgpass", 0o600)  # Set the required permissions

# Restore the database using psql command
try:
  subprocess.run(['PGPASSFILE=/root/.pgpass', 'psql', '-h', 'localhost', '-U', 'adsdb', '-d', 'adsdb', '-f', dump_path], shell=True)
  print("Restoration successful!")
except Exception as e:
  print(f"Error during restoration: {e}")

cmd = 'PGPASSFILE=/root/.pgpass psql -h localhost -U adsdb -d adsdb -c "SELECT table_name FROM information_schema.tables WHERE table_schema=\'public\';"'
subprocess.run(cmd, shell=True)

table_names = ['final_employment_rate', 'final_house_price_index_weights', 'final_inflation_rate', 'final_house_price_index']

def show_table(table_name):
  display_query = f"SELECT * FROM {table_name} LIMIT 20;"
  cmd = f'PGPASSFILE=/root/.pgpass psql -h localhost -U adsdb -d adsdb -c "{display_query}"'
  subprocess.run(cmd, shell=True)


def check_for_duplicates(table_name):
    if 'inflation' in table_name:
      duplicate_query = f"""
      SELECT year, count(*)
      FROM {table_name}
      GROUP BY year
      HAVING COUNT(*) > 1
      LIMIT 20;
      """
    elif 'employment' in table_name:
      duplicate_query = f"""
      SELECT year, quarter, provinces, rates, sex, count(*)
      FROM {table_name}
      GROUP BY year, quarter, provinces, rates, sex
      HAVING COUNT(*) > 1
      ORDER BY year, quarter
      LIMIT 20;
      """
    elif 'weights' in table_name:
      duplicate_query = f"""
      SELECT year, autonomous_communities_and_cities , index_type, count(*)
      FROM {table_name}
      GROUP BY year, autonomous_communities_and_cities , index_type
      HAVING COUNT(*) > 1
      LIMIT 20;
      """
    elif (table_name == 'final_house_price_index'):
      duplicate_query = f"""
      SELECT year, quarter, indices_and_rates, index_type, autonomous_communities_and_cities, national_total, count(*)
      FROM {table_name}
      GROUP BY year, quarter, indices_and_rates, index_type, autonomous_communities_and_cities, national_total
      HAVING COUNT(*) > 1
      LIMIT 20;
      """
    cmd = f'PGPASSFILE=/root/.pgpass psql -h localhost -U adsdb -d adsdb -c "{duplicate_query}"'
    subprocess.run(cmd, shell=True)


for table_name in table_names:
  #get_primary_keys(table_name)
  check_for_duplicates(table_name)

show_table('final_inflation_rate')
show_table('final_employment_rate')