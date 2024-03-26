# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit
from pyspark.sql.types import StructField, StructType,IntegerType,StringType,FloatType, DateType

# COMMAND ----------

pipstops_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), False),
        StructField("driverId", IntegerType(), True),
        StructField("stop", StringType(), True),
        StructField("lap", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("duration", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
    ]
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Set option("multiline",True) To be able to read multilineJson

# COMMAND ----------

pitstops_df = (spark.read.schema(pipstops_schema)
                .option("multiLine",True)
               .json(f'{raw_folder_path}/pit_stops.json')
             )

# COMMAND ----------

pitstops_renamed_df = (
    pitstops_df.withColumnRenamed("reaceId", "race_id")
    .withColumnRenamed("driverID", "driver_id")
)


# COMMAND ----------

pitstops_final_df =add_ingestion_date(pitstops_renamed_df)

# COMMAND ----------

pitstops_final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/pit_stops')