# -*- coding: utf-8 -*-
"""ModelGeneration.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/19Y09hJ50gsxjq2D6BL8F5C4-Gp1T2Xle
"""

import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import duckdb
import csv
import os
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error

#from google.colab import drive
#drive.mount('/content/drive')

"""# Preparation"""

conn_input = duckdb.connect(database='/content/drive/MyDrive/ADSDB/colabs-part2/Train and Validation Sets Generation/prepared_data_splitted.db', read_only=True)
path = '/content/drive/MyDrive/ADSDB/colabs-part2/Model Generation/models.csv'        # Path to save models.

# Print tables in input database
result = conn_input.execute("SHOW TABLES;")

rows = result.fetchall()
for row in rows:
    print(row[0])

df_train = conn_input.execute("SELECT * FROM train").df()
df_train.head()

df_val = conn_input.execute("SELECT * FROM val").df()
df_val.head()

# Time series require the temporal dimension to be the index
if 'Quarter' in df_train.index.names:
    print("'Quarter' is already the index.")
else:
    df_train.set_index('Quarter', inplace=True)

if 'Quarter' in df_val.index.names:
    print("'Quarter' is already the index.")
else:
    df_val.set_index('Quarter', inplace=True)

total_set = pd.concat([df_train, df_val], ignore_index=False)
total_set = total_set.sort_index(ascending=True)
total_set.head()

"""# Save and Load functions"""

def save_model(p, d, q, mae, mape, rmse, train_range, predict_range, path, mode):
  model_name = 'Arima' + '-' + str(mode) + '(' + str(p) + ',' + str(d) + ',' + str(q) + ') ' + str(train_range) + str(predict_range)

  if not os.path.exists(path):
        with open(path, mode='w', newline='') as new_csv_file:
            fieldnames = ["model_name", "p", "d", "q", "mae", "mape", "rmse", "train_range", "predict_range", "mode"]
            writer = csv.DictWriter(new_csv_file, fieldnames=fieldnames)
            writer.writeheader()

  # Check if the entry already exists in the CSV file
  entry_exists = False
  with open(path, mode='r', newline='') as csv_file:
      reader = csv.DictReader(csv_file)
      for row in reader:
          if row["model_name"] == model_name:
              entry_exists = True
              break

   # If the entry doesn't exist, add it to the CSV file
  if not entry_exists:
    with open(path, mode='a', newline='') as csv_file:
      fieldnames = ["model_name", "p", "d", "q", "mae", "mape", "rmse", "train_range", "predict_range", "mode"]
      writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
      # Check if the file is empty, and write the header if it is
      if csv_file.tell() == 0:
        writer.writeheader()
        # Write model information to the CSV file
      writer.writerow({
          "model_name": model_name,
          #"seed": seed,
          "p": p,
          "d": d,
          "q": q,
          "mae": mae,
          "mape": mape,
          "rmse": rmse,
          "train_range": train_range,
          "predict_range": predict_range,
          "mode": mode
          })
    print(f"Model and information saved to {path}")
  else:
    print(f"Model information already exists in {path}, not adding a duplicate entry.")


def load_model(path, model_name):
  try:
      with open(path, mode='r') as csv_file:
          reader = csv.DictReader(csv_file)

          for row in reader:
              if row["model_name"] == model_name:
                  return {
                      "p": int(row["p"]),
                      "d": int(row["d"]),
                      "q": int(row["q"]),
                      #"seed": np.uint32(int(row["seed"])),
                      "mae": float(row["mae"]),
                      "mape": float(row["mape"]),
                      "rmse": float(row["rmse"]),
                      "train_range": str(row["train_range"]),
                      "predict_range": str(row["predict_range"]),
                      "mode": row["mode"]
                  }

      print(f"Model with name '{model_name}' not found in {path}")
      return None
  except FileNotFoundError:
        print(f"File {path} not found.")
        return None

"""# ARIMA Model autofitting"""

#!pip install pmdarima

import pmdarima as pm
auto_arima = pm.auto_arima(df_train, stepwise=False, seasonal=False, error_action="ignore")
auto_arima

# Extract p, d, and q parameters
p, d, q = auto_arima.order

# Print the parameters
print("ARIMA(p={}, d={}, q={})".format(p, d, q))

auto_arima.summary()

# Run auto model
model = ARIMA(df_train, order=(p, d, q))
model_fit = model.fit()
print(model_fit.summary())

forecast_test_auto = model_fit.forecast(len(df_val))
total_set['forecast_auto'] = [None]*len(df_train) + list(forecast_test_auto)

total_set.plot()

# Print Metrics

mae = mean_absolute_error(df_val, forecast_test_auto)
mape = mean_absolute_percentage_error(df_val, forecast_test_auto)
rmse = np.sqrt(mean_squared_error(df_val, forecast_test_auto))

print(f'mae - auto: {mae}')
print(f'mape - auto: {mape}')
print(f'rmse - auto: {rmse}')

# Save Model
train_range="2008Q1-2021Q4"
predict_range="2022Q1-2022Q4"
mode="auto"
save_model(p, d, q, mae, mape, rmse, train_range, predict_range, path, mode)

"""# ARIMA Model Manual-Fitting"""

from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

acf_or = plot_acf(df_train)
pacf_or = plot_pacf(df_train)

from statsmodels.tsa.stattools import adfuller
adf_test = adfuller(df_train)
print(f'p-value: {adf_test[1]}')

# Using difference
df_train_diff = df_train.diff().dropna()
df_train_diff.plot()
acf_diff = plot_acf(df_train_diff)
pacf_diff = plot_pacf(df_train_diff)

adf_test_diff = adfuller(df_train_diff)
print(f'p-value: {adf_test_diff[1]}')

# Using second difference
df_train_diff2 = df_train_diff.diff().dropna()
df_train_diff2.plot()
acf_diff2 = plot_acf(df_train_diff2)
pacf_diff2 = plot_pacf(df_train_diff2)

adf_test_diff2 = adfuller(df_train_diff2)
print(f'p-value: {adf_test_diff2[1]}')

# Choose p,d,q
p,d,q = 2,1,0

# Run model
model = ARIMA(df_train, order=(p, d, q))
model_fit = model.fit()
print(model_fit.summary())

forecast_test_manual = model_fit.forecast(len(df_val))
total_set['forecast_manual'] = [None]*len(df_train) + list(forecast_test_manual)

print(total_set)
total_set.plot()

# Print Metrics

mae = mean_absolute_error(df_val, forecast_test_manual)
mape = mean_absolute_percentage_error(df_val, forecast_test_manual)
rmse = np.sqrt(mean_squared_error(df_val, forecast_test_manual))

print(f'mae - manual: {mae}')
print(f'mape - manual: {mape}')
print(f'rmse - manual: {rmse}')

# Save Model
train_range="2008Q1-2021Q4"
predict_range="2022Q1-2022Q4"
mode="manual"
save_model(p, d, q, mae, mape, rmse, train_range, predict_range, path, mode)