# Databricks notebook source
# Importing required libraries
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, to_date
import pandas as pd

# COMMAND ----------

# Reading the ticker symbols from the csv file
# Initialize Spark Session
spark = SparkSession.builder.appName("CSV to Delta").getOrCreate()

from pyspark.sql.functions import col

# Drop the current table
spark.sql("DROP TABLE IF EXISTS ticker_symbols")

# Read the csv file from the repository
df = pd.read_csv("../data/Tickers.csv")

# Creating a spark dataframe for delta table
spark_df = spark.createDataFrame(df)

# Select distinct 'Ticker' values
spark_df = spark_df.select("Ticker").distinct()

# set the mergeSchema option to true
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Write the spark DataFrame to a Delta table
spark_df.write.format("delta").mode("overwrite").saveAsTable("ticker_symbols")


# ruff : noqa
