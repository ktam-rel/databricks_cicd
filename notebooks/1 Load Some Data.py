# Databricks notebook source
# MAGIC %md ## Copy data from a URL into ADLS container

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

data = urllib.request.urlopen("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/archived_data/archived_daily_case_updates/01-28-2020_1300.csv").read()
print(data)

# COMMAND ----------

file1 = open("/dbfs/mnt/ktam/test123.csv", "wb")
file1.write(data)
file1.close()
