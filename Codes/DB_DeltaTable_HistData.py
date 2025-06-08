# Databricks notebook source
# Importing required libraries
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_date
import pandas as pd

# COMMAND ----------

# Reading the historical data for 20 years from the parquet file
# Initialize Spark Session
spark = SparkSession.builder.appName("Parquet to Delta").getOrCreate()

# Read the parquet file
df = pd.read_parquet("../data/processed_20.parquet")

# Creating a spark dataframe for delta table
spark_df = spark.createDataFrame(df)

# Write the spark DataFrame to a Delta table
spark_df.write.format("delta").mode("overwrite").saveAsTable("historical_data")


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


# ruff : noqa
