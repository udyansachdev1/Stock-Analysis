# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook to get last 100 days of stock data and update the delta table accordingly
# MAGIC
# MAGIC _This will run every day at a specific time_

# COMMAND ----------

# Importing libraries
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql import functions as F
from pyspark.sql.functions import col


# COMMAND ----------

tickers

# COMMAND ----------

tickers = pd.read_csv("../data/Tickers.csv")
df_list = []
for i in tickers["Ticker"]:
    try:
        query = (
            "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol="
            + i
            + "&apikey=B434MFNHO276GM45&datatype=csv"
        )

        # Load the data from the API
        df = pd.read_csv(query)

        # Adding column for ticker
        df["Ticker"] = i

        # Adding the data to the list
        df_list.append(df)
    except:
        print(f"Error occoured for {i}")
        break
print("Data Loaded")

# COMMAND ----------

df = pd.concat(df_list)
# rearraning columns
df = df[
    [
        "Ticker",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
]
# converting timestamp to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])
# sort by ticker and timestamp
df.sort_values(by=["Ticker", "timestamp"], inplace=True)

# COMMAND ----------

# Creating a spark dataframe for delta table
spark_df = spark.createDataFrame(df)

# Assuming df is your DataFrame with new data
new_data = spark_df

# Load the Delta table
deltaTable = DeltaTable.forPath(spark, "/user/hive/warehouse/historical_data")

# Define the merge condition
condition = (
    "deltaTable.Ticker = new_data.Ticker AND deltaTable.timestamp = new_data.timestamp"
)

# Use merge
deltaTable.alias("deltaTable").merge(
    new_data.alias("new_data"), condition
).whenMatchedUpdate(
    set={
        "Ticker": "new_data.Ticker",
        "timestamp": "new_data.timestamp",
        "open": "new_data.open",
        "high": "new_data.high",
        "low": "new_data.low",
        "close": "new_data.close",
        "volume": "new_data.volume",
    }
).whenNotMatchedInsert(
    values={
        "Ticker": "new_data.Ticker",
        "timestamp": "new_data.timestamp",
        "open": "new_data.open",
        "high": "new_data.high",
        "low": "new_data.low",
        "close": "new_data.close",
        "volume": "new_data.volume",
    }
).execute()

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("Change Delta Table Schema").getOrCreate()

# Read the Delta table
df = spark.read.format("delta").load("/user/hive/warehouse/historical_data")

# Convert the timestamp column to Date data type
df = df.withColumn("timestamp", to_date(col("timestamp")))

# Write the DataFrame back to the Delta table
df.write.format("delta").mode("overwrite").save(
    "/user/hive/warehouse/historical_data_stocks"
)

# COMMAND ----------

# Load the Delta table
deltaTable = DeltaTable.forPath(spark, "/user/hive/warehouse/historical_data")

# Convert DeltaTable to DataFrame for aggregation
df = deltaTable.toDF()

# Get min and max timestamps
min_timestamp = df.agg(F.min("timestamp")).collect()[0][0]
max_timestamp = df.agg(F.max("timestamp")).collect()[0][0]

print("Min timestamp: ", min_timestamp)
print("Max timestamp: ", max_timestamp)

# COMMAND ----------

# Convert DeltaTable to DataFrame for sorting and limiting
df = deltaTable.toDF()

# Get the last 30 distinct timestamps
last_30_distinct_timestamps = (
    df.select("timestamp").distinct().orderBy(col("timestamp").desc()).limit(30)
)

# Show the result
last_30_distinct_timestamps.show()

# COMMAND ----------

from pyspark.sql.functions import col, count

# Group by 'timestamp' and 'Ticker' and count the number of records in each group
grouped_df = df.groupBy("timestamp", "Ticker").agg(count("*").alias("count"))

# Check if any group has more than 1 record
non_unique_records = grouped_df.filter(col("count") > 1)

# If non_unique_records DataFrame is empty, then the table is unique at the 'timestamp' + 'Ticker' level
if non_unique_records.count() == 0:
    print("The table is unique at the 'timestamp' + 'Ticker' level.")
else:
    print("The table is not unique at the 'timestamp' + 'Ticker' level.")

# ruff : noqa
