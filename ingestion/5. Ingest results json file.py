# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit
from pyspark.sql.types import StructField, StructType,IntegerType,StringType,FloatType

# COMMAND ----------

results_schema = StructType(
    fields=[
        StructField("constructorId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("fastestLapSpeed", FloatType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("grid", IntegerType(), True),
        StructField("laps", IntegerType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("points", FloatType(), True),
        StructField("position", IntegerType(), True),
        StructField("positionOrder", IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("raceId", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("resultId", IntegerType(), False),
        StructField("statusId",  StringType(),True),
        StructField("time", IntegerType(), True),
    ]
)


# COMMAND ----------

results_df = ( spark.read.schema(results_schema)
              .json(f'{raw_folder_path}/results.json')
              )

# COMMAND ----------

# MAGIC %md
# MAGIC Rename cols

# COMMAND ----------

result_rename_cols = (
    results_df.withColumnRenamed("resultId", "result_id")
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionText", "position_text")
    .withColumnRenamed("positionOrder", "position_order")
    .withColumnRenamed("fastestLap", "fastest_lap")
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
)


# COMMAND ----------

result_rename_cols_df = add_ingestion_date(result_rename_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC Drop columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

result_final_df = result_rename_cols_df.drop(col("statusId"))

# COMMAND ----------

result_final_df.write.mode("overwrite").partitionBy("race_id").parquet(
    f"{processed_folder_path}/results"
)


# COMMAND ----------

# MAGIC %md
# MAGIC