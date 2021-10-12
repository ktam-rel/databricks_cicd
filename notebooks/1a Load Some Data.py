# Databricks notebook source
# MAGIC %md ## Copy data from a URL into ADLS container
# MAGIC 
# MAGIC Right now this just downloads one CSV file. Ideally it should grab all CSV files in this directory
# MAGIC - https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports
# MAGIC 
# MAGIC Use this URL to get a JSON object listing directory contents:
# MAGIC - https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports?ref=master

# COMMAND ----------

from relpipeline.data import mount, get_dbutils

db_utils = get_dbutils(spark)
mount_str = "/mnt/ktam"
mount(db_utils, "ktam-test", "analyticsresearch", "analyticsresearch-storageacctscope", "storage-account-key", mount_str)

# COMMAND ----------

db_utils.fs.ls(mount_str)

# COMMAND ----------

import urllib.request  # the lib that handles the url stuff
import json

covid_data_dir_URL = urllib.request.urlopen(
    "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports?ref=master")
response = covid_data_dir_URL.read()
encoding = covid_data_dir_URL.info().get_content_charset('utf-8')
files = json.loads(response.decode(encoding))


from relpipeline.data import get_json_from_url

data_url = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports?ref=master")

files_info = get_json_from_url(data_url)

# function to create dataframe from from json

paths = list(map(lambda f: (f["path"], f["download_url"]), files))
pathsSchema = ['path', 'download_url']

pathsDF = spark.createDataFrame(paths, pathsSchema)


# function to download file from info
def download_covid_data(df):
    covid_data_file_URL = urllib.request.urlopen(df.download_url)
    data = covid_data_file_URL.read().decode(covid_data_file_URL.headers.get_content_charset())
    writepath = '/dbfs/mnt/ktam/' + df.path
    with open(writepath, 'w') as f:
        f.write(data)

pathsDF.filter(pathsDF.path.endswith('.csv')).foreach(download_covid_data)
