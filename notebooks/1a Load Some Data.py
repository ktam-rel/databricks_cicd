# Databricks notebook source
# MAGIC %md ## Copy data from a URL into ADLS container
# MAGIC 
# MAGIC Downloads COVID csv data from here
# MAGIC - https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports

# COMMAND ----------

from relpipeline.util import *

db_utils = get_dbutils(spark)
mount_str = "/mnt/ktam"
mount(db_utils, "ktam-test", "analyticsresearch", "analyticsresearch-storageacctscope", "storage-account-key", mount_str)


# COMMAND ----------

covid_data_dir_URL = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports?ref=master"

data_json = get_json_from_url(covid_data_dir_URL)
files = get_fileinfo_dataframe_from_json(spark, data_json)
download_all(db_utils, "/dbfs" + mount_str + "/data/", files)

# COMMAND ----------

# MAGIC %md ### Notes
# MAGIC - Move any testable logic into library functions
# MAGIC - Any code directly in the job/notebook top level code will not be covered with unit tests, so make it as simple as possible
# MAGIC - Dynamic typing in Python allows for easier unit testing since functions can be freely monkey patched to mock out behavior. 
# MAGIC - Libraries are a bit involved to iterate on. To replace a library in a cluster after making modifications, here are the steps. 
# MAGIC   1. Repackage wheel file ```python.exe .\setup.py bdist_wheel```
# MAGIC   1. Copy to dbfs ```databricks fs cp .\dist\relpipeline-1.0.0-py3-none-any.whl dbfs:/FileStore/relpipeline-1.0.0-py3-none-any.whl --overwrite```
# MAGIC   1. Uninstall existing library ```databricks libraries uninstall --cluster-id 0913-144225-fifes758 --whl dbfs:/FileStore/relpipeline-1.0.0-py3-none-any.whl```
# MAGIC   1. Restart cluster ```databricks clusters restart --cluster-id 0913-144225-fifes758```
# MAGIC   1. Reinstall library from wheel ```databricks libraries install --cluster-id 0913-144225-fifes758 --whl dbfs:/FileStore/relpipeline-1.0.0-py3-none-any.whl```
# MAGIC 
# MAGIC ### Obstacles to local E2E testing
# MAGIC - dbutils and any interaction with dbfs
# MAGIC - Library mechanism is specific to Dtabricks and would have to figure out an alternative way to install python dependencies in a local Spark environment
# MAGIC - Need to figure out how to also install any other dependencies that are installed to a Databricks cluster by default 
