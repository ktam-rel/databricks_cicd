# Databricks notebook source
# MAGIC %md ## Process ADLS blobs in container and put data into a DeltaLake table

# COMMAND ----------

from relpipeline.util import *

db_utils = get_dbutils(spark)
mount_str = "/mnt/ktam"
mount(db_utils, "ktam-test", "analyticsresearch", "analyticsresearch-storageacctscope", "storage-account-key", mount_str)


# COMMAND ----------

from pyspark.sql.functions import col, substring
from pyspark.sql.types import IntegerType

df = transform_csv_data(spark, mount_str + "/data")
write_df_to_delta_table(df, mount_str)

# COMMAND ----------

spark.read.format("delta").load("/mnt/ktam/delta/output_delta").createOrReplaceTempView("covid_delta")
spark.sql("SELECT count(*) FROM covid_delta").show()
spark.sql("SELECT * FROM covid_delta LIMIT 5").show()
spark.sql("SELECT * FROM covid_delta WHERE Combined_Key = 'Albania'").show()
