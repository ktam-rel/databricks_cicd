# Databricks notebook source
# MAGIC %md ## Train a model from DeltaLake table
# MAGIC 
# MAGIC Code derived from this article about timeseries forecasting using pandas UDF in spark: 
# MAGIC 
# MAGIC https://medium.com/walmartglobaltech/multi-time-series-forecasting-in-spark-cc42be812393
# MAGIC 
# MAGIC https://github.com/maria-alphonsa-thomas/Multi-Time-Series-Pyspark-Pandas-UDF

# COMMAND ----------

from relpipeline.util import *

db_utils = get_dbutils(spark)
mount_str = "/mnt/ktam"
mount(db_utils, "ktam-test", "analyticsresearch", "analyticsresearch-storageacctscope", "storage-account-key", mount_str)

# COMMAND ----------

data = load_from_delta(spark, "/mnt/ktam/delta/output_delta")
data_selected_groups = filter_groups_less_than(data, 100, 'Combined_Key')

forecasted_spark_df = data_selected_groups.groupby('Combined_Key').apply(holt_winters_time_series_udf)
forecasted_spark_df.display()


# COMMAND ----------


