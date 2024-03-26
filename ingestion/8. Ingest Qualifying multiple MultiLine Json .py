# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit
from pyspark.sql.types import StructField, StructType,IntegerType,StringType,FloatType, DateType

# COMMAND ----------

qualifying_schema = StructType(
    fields=[
        StructField("qualifyId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("position", StringType(), True),
        StructField("q1", StringType(), True),
        StructField("q2", StringType(), True),
        StructField("q3", StringType(), True),
    ]
)


# COMMAND ----------

qualifying_df = (
    spark.read
            # .schema(qualifying_schema)
            .option("multiLine",True)
            .json(f'{raw_folder_path}/qualifying')
             )

# COMMAND ----------

qualifying_renamed_df = ( 
                         qualifying_df
                            .withColumnRenamed("qualifyingId","qualifying_id")
                            .withColumnRenamed("raceId","race_id")
                            .withColumnRenamed("driverId","driverId")
                            .withColumnRenamed("constructorId","constructor_id")
                          )

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")