# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 
# MAGIC Import the constrcutors.json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json("/mnt/formula1/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC step 2 Drop the url column

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC Step-3 Rename and create a new column called ingestion date from current_timestamp function

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed(
    "constructorId", "constructor_id"
).withColumnRenamed(
    "constructorRef", "constrcutor_ref"
).withColumn(
    "ingestion_date", current_timestamp()
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 4 write data to data lake
# MAGIC

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet("/mnt/formula1/processed/constructors")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1/processed/constructors"))
