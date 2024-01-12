# -*- coding: utf-8 -*-
"""AnalyticalSandbox.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1bZg7BNlQrWNkYhIzp4ZaB1XT7o4HTVT9
"""

#!pip install --quiet duckdb
#!pip install --quiet jupysql
#!pip install --quiet duckdb-engine
#!pip install --quiet pandas
#!pip install --quiet matplotlib

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import glob
import numpy as np

#from google.colab import drive
#drive.mount('/content/drive')

csv = '/content/drive/MyDrive/ADSDB/exploitation/data.csv'
df = pd.read_csv(csv)
df.head()

# Conncect to Duckdb
conn = duckdb.connect("/content/drive/MyDrive/ADSDB/colabs-part2/Analytical Sandbox/complete.db")

conn.execute("""
CREATE OR REPLACE TABLE house_price_indexes AS
SELECT Year, Quarter, Provinces, house_price_index_type, house_price_index
FROM df
WHERE Sex = 'Both sexes';
""")

conn.execute("DESCRIBE house_price_indexes").df()

cursor = conn.cursor()

# Execute the query to get a list of all tables
cursor.execute("SHOW TABLES")

# Fetch all the results
tables = cursor.fetchall()

# Print the list of tables
print("Tables in the database:")
for table in tables:
    print(table[0])

conn.execute("""SELECT * FROM house_price_indexes""").df()

conn.close()