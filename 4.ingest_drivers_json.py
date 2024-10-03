# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 import drivers.json file and create a schema 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType([StructField("forename", StringType()), StructField("surname", StringType())])

# COMMAND ----------

drivers_schema = StructType([StructField("driverId", IntegerType(), False), StructField("driverRef", IntegerType(), True), StructField("code", StringType(), True), StructField("dob", DateType(), True), StructField("name", name_schema), StructField("nationality", StringType(), True)])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json("/mnt/formula1/drivers.json")



# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 Remove the url column

# COMMAND ----------

drivers_dropped_df = drivers_df.drop("url")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

# COMMAND ----------

drivers_renamed_df = drivers_dropped_df.withColumnRenamed("driverId", "driver_id") \
                                       .withColumnRenamed("driverRef", "driver_ref") \
                                       .withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname"))) \
                                       .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

drivers_renamed_df.write.mode("overwrite").parquet("/mnt/formula1/processed/drivers")
