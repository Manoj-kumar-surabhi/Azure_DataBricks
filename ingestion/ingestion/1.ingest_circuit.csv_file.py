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

# MAGIC %md 
# MAGIC ###Selecting the columns we need
# MAGIC # this type below have the ability to use functions on the table

# COMMAND ----------

from pyspark.sql.functions import col 

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 3 Rename the Columns  

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id").withColumnRenamed("circuitRef", "circuit_ref").withColumnRenamed("lat", "latitude").withColumnRenamed("lng", "longitude").withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC #step 4 Add a column
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # write to data lake storage

# COMMAND ----------

circuits_final_df.write.parquet("/mnt/formula1/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1/processed/circuits"))
