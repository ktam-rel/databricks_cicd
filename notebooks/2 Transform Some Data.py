# Databricks notebook source
# MAGIC %md ## Process ADLS blobs in container and put data into a DeltaLake table

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

df = spark.read.option("header", "true").csv("/mnt/ktam/*20*.csv")
df = df.withColumn("Last_Update", substring(col("Last_Update"),0,10))
df = df.withColumn("Deaths", data["Deaths"].cast(IntegerType()))
df = df.withColumnRenamed("Last_Update", "LastUpdate")
df.display()


# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/mnt/ktam/delta/output_delta")

# COMMAND ----------

spark.read.format("delta").load("/mnt/ktam/delta/output_delta").createOrReplaceTempView("covid_delta")


# COMMAND ----------

spark.sql("SELECT count(*) FROM covid_delta").show()
spark.sql("SELECT * FROM covid_delta LIMIT 5").show()
spark.sql("SELECT * FROM covid_delta WHERE Combined_Key = 'Albania'").show()

# COMMAND ----------


