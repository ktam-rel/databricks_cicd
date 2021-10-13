# Databricks notebook source
# MAGIC %md ## Copy data from a URL into ADLS container
# MAGIC 
# MAGIC Right now this just downloads one CSV file. Ideally it should grab all CSV files in this directory
# MAGIC - https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports
# MAGIC 
# MAGIC Use this URL to get a JSON object listing directory contents:
# MAGIC - https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports?ref=master

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
