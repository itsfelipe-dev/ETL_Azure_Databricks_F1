# Databricks notebook source
# MAGIC %md
# MAGIC ### Load Json file with schema

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema).json(
    f"{raw_folder_path}/constructors.json"
)


# COMMAND ----------

# MAGIC %md 
# MAGIC #### Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_drop_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Rename columns and add ingestion date 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_renamed_df = constructor_drop_df.withColumnRenamed(
    "constructorId", "constructor_id"
).withColumnRenamed("constructorRef", "constructor_ref")


# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_renamed_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(
    f"{processed_folder_path}/constructor"
)
