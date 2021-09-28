# Databricks notebook source
# MAGIC %md ## Train a model from DeltaLake table

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

import urllib.request  # the lib that handles the url stuff

data = urllib.request.urlopen("https://raw.githubusercontent.com/maria-alphonsa-thomas/Multi-Time-Series-Pyspark-Pandas-UDF/master/kaggle/train.csv").read()

file1 = open("/dbfs/mnt/ktam/timeseries.csv", "wb")
file1.write(data)
file1.close()

# COMMAND ----------


# import the necessary pyspark and pandas libraries

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType,FloatType

import statsmodels.tsa.api as sm
import numpy as np
import pandas as pd


# read the entire data as spark dataframe
data = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/ktam/timeseries.csv')\
.select('Store','Dept','Date','Weekly_Sales')

## basic data cleaning before implementing the pandas udf
##removing Store - Dept combination with less than 2 years (52 weeks ) of data

selected_com = data.groupBy(['Store','Dept']).count().filter("count > 104").select("Store","Dept")
data_selected_store_departments = data.join(selected_com,['Store','Dept'],'inner')


##pandas udf
schema = StructType([StructField('Store', StringType(), True),
                     StructField('Dept', StringType(), True),
                     StructField('weekly_forecast_1', DoubleType(), True),
                     StructField('weekly_forecast_2', DoubleType(), True)])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)

def holt_winters_time_series_udf(data):
  
    data.set_index('Date',inplace = True)
    time_series_data = data['Weekly_Sales']
    

    ##the model
    model_monthly = sm.ExponentialSmoothing(np.asarray(time_series_data),trend='add').fit()

    ##forecast values
    forecast_values = pd.Series(model_monthly.forecast(2),name = 'fitted_values')
   
    
     
    
    return pd.DataFrame({'Store': [str(data.Store.iloc[0])],'Dept': [str(data.Dept.iloc[0])],'weekly_forecast_1': [forecast_values[0]], 'weekly_forecast_2':[forecast_values[1]]})


##aggregating the forecasted results in the form of a spark dataframe
forecasted_spark_df = data_selected_store_departments.groupby(['Store','Dept']).apply(holt_winters_time_series_udf)


## to see the forecasted results
forecasted_spark_df.show(100)


# COMMAND ----------


