# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit
from pyspark.sql.types import StructField, StructType,IntegerType,StringType,FloatType, DateType

# COMMAND ----------

lap_times_schema = StructType(fields=[
	StructField("raceId", IntegerType(), True),
	StructField("driverId", IntegerType(), True),
	StructField("position", IntegerType(), True),
	StructField("time", StringType(), True),
	StructField("miliseconds", IntegerType(), True),
	])

# COMMAND ----------

lap_times_df = (
    spark.read.schema(lap_times_schema)
                .csv(f'{raw_folder_path}/lap_times')
                )

# COMMAND ----------

lap_renamed_final_df = (
    lap_times_df.withColumnRenamed("reaceId", "race_id")
    .withColumnRenamed("driverID", "driver_id")
)


# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_renamed_final_df)

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet(f'{processed_folder_path}lap_times')