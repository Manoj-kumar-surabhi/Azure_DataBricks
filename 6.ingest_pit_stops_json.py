# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline", "true").json("/mnt/formula1/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pit_stops_renamed_df.write.mode("overwrite").parquet("/mnt/formula1/processed/pit_stops")
