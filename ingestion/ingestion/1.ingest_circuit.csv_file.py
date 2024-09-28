# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuit.csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC ###step-1 Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("country", StringType(),True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read .option("header", "true").schema(circuits_schema).csv("dbfs:/mnt/formula1/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1

# COMMAND ----------

circuits_df.printSchema()
