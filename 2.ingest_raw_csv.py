# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 Access data 

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DateType, TimestampType

# COMMAND ----------

races_schema = StructType(fields=[
  StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True ),
  StructField ("time", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header", "true").schema(races_schema).csv("/mnt/formula1/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 : Selecting the columnns we need 

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_column = races_df.select(col("raceId"), col("year"), col("round"),col("circuitId"),col("name"),col("date"), col("time") )

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 Rename the columns

# COMMAND ----------

races_renamed_columns = races_selected_column.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC Step- 4 Concat the date and time to race_timestamp
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_newcolumn_df = races_renamed_columns.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit

# COMMAND ----------

races_concated_df = races_newcolumn_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_without_datetime_df = races_concated_df.select(
    col("race_id"), 
    col("race_year"), 
    col("round"), 
    col("circuit_id"), 
    col("name"), 
    col("race_timestamp"), 
    col('ingestion_date')
)

# COMMAND ----------

# MAGIC %md
# MAGIC step 5 write the output to data lake storage
# MAGIC

# COMMAND ----------

races_without_datetime_df.write.parquet('/mnt/formula1/processed/races')
