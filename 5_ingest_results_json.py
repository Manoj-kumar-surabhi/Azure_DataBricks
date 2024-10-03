# Databricks notebook source
results_schema = "constructorId INT, driverId INT, fastestLap INT, fastestLapSpeed FLOAT, fastestLapTime STRING, grid INT, laps INT, milliseconds INT, number INT, points FLOAT, position INT, positionOrder INT, positionOrder INT, positionText INT, rank INT, raceId INT, resultId INT, statusId STRING, time STRING"

# COMMAND ----------

results_df = spark.read.json("/mnt/formula1/results.json")

# Drop the duplicate column
results_df = results_df.drop("positionorder").drop("statusId")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_final_df = results_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_final_df.write.mode("overwrite").parquet("/mnt/formula1/results_final")
