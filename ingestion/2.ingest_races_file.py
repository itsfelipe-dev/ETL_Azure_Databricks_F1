# Databricks notebook source
# MAGIC %md 
# MAGIC #### Define schema csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)


# COMMAND ----------

races_df = (
    spark.read.option("header", True)
    .schema(races_schema)
    .csv(f"{raw_folder_path}/races.csv")
)


# COMMAND ----------

# Select columns 
races_df_selected_df = races_df.select("raceId","year","round","circuitId","name","date","time")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Rename columns 
# MAGIC

# COMMAND ----------

races_renamed_df = (
    races_df_selected_df.withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("year", "race_year")
    .withColumnRenamed("circuitId", "circuit_id")
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Add new Columns
# MAGIC With timestamp current ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col
from pyspark.sql.functions import lit

# COMMAND ----------

races_add_col_df = races_renamed_df.withColumn(
    "race_timestamp",
    to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"),
)


# COMMAND ----------

races_final_df = add_ingestion_date(races_add_col_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake in parquet file

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet(
    f"{processed_folder_path}/races"
)
