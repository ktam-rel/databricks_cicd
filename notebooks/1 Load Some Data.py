# Databricks notebook source
# MAGIC %md ## Copy data from a URL into ADLS container
# MAGIC 
# MAGIC Right now this just downloads one CSV file. Ideally it should grab all CSV files in this directory
# MAGIC - https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports
# MAGIC 
# MAGIC Use this URL to get a JSON object listing directory contents:
# MAGIC - https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports?ref=master

# COMMAND ----------

def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils
 
dbutils = get_dbutils(spark)

mount_str = "/mnt/ktam"

if any(mount.mountPoint == mount_str for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_str)
dbutils.fs.mount(
    source = "wasbs://ktam-test@analyticsresearch.blob.core.windows.net",
    mount_point = mount_str,
    extra_configs = {"fs.azure.account.key.analyticsresearch.blob.core.windows.net":dbutils.secrets.get(scope = "analyticsresearch-storageacctscope", key = "storage-account-key")})


# COMMAND ----------

dbutils.fs.ls(mount_str)

# COMMAND ----------

import urllib.request  # the lib that handles the url stuff
import json

covid_data_dir_URL = urllib.request.urlopen(
    "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports?ref=master")
response = covid_data_dir_URL.read()
encoding = covid_data_dir_URL.info().get_content_charset('utf-8')
files = json.loads(response.decode(encoding))

for f in files:
    path = f["path"]

    if path.endswith(".csv"):
        covid_data_file_URL = urllib.request.urlopen(f["download_url"])
        data = covid_data_file_URL.read().decode(covid_data_file_URL.headers.get_content_charset())

        writepath = "/mnt/ktam/" + path
        dbutils.fs.put(writepath, data, overwrite=True)

        print("Wrote file to " + writepath)
