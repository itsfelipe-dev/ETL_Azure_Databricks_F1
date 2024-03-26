# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 read the json file 
# MAGIC

# COMMAND ----------

drivers_df = spark.read \
     .json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Rename columns and add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

# COMMAND ----------

drivers_renamed_df = (
    drivers_df.withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("driverRef", "driver_ref")
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))
)


# COMMAND ----------

drivers_ingestion_date_df = add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Drop unwanted columns from the dataframe

# COMMAND ----------

drivers_final_df = drivers_ingestion_date_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")