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


# import the necessary pyspark and pandas libraries

from pyspark.sql.functions import pandas_udf, PandasUDFType, unix_timestamp, col, substring
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType


import statsmodels.tsa.api as sm
import numpy as np
import pandas as pd


# read the entire data as spark dataframe
data = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/ktam/*20*.csv').select('Combined_Key', 'Last_Update', 'Deaths')
data = data.withColumn("Updated2", unix_timestamp(col("Last_Update"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
data = data.withColumn("Updated3", substring(col("Last_Update"),0,10))
data = data.withColumn("Deaths", data["Deaths"].cast(IntegerType()))

data.display()
data.printSchema()



# COMMAND ----------

#data.filter(col("Combined_Key") == "Argentina").coalesce(1).write.option("header",True).csv("/mnt/ktam/argentina.csv")
data2 = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/ktam/argentina_clean.csv')
data = data.filter((data.Combined_Key == "Argentina")).orderBy(col("Updated3"))
data.display()
data2.display()
data.printSchema()
data2.printSchema()


# COMMAND ----------

selected_com = data.groupBy(['Combined_Key']).count().filter("count > 104").select("Combined_Key")
data_selected_groups = data.join(selected_com,['Combined_Key'],'inner')


##pandas udf
schema = StructType([StructField('Combined_Key', StringType(), True),
                     StructField('daily_forecast_1', LongType(), True),
                     StructField('daily_forecast_2', LongType(), True)])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def holt_winters_time_series_udf(data):
  
    data.set_index('Updated3',inplace = True)
    time_series_data = data['Deaths']
    

    ##the model
    model = sm.ExponentialSmoothing(np.asarray(time_series_data),trend='add').fit()

    ##forecast values
    forecast_values = pd.Series(model.forecast(2),name = 'fitted_values')
    
    
    return pd.DataFrame({'Combined_Key': [str(data.Combined_Key.iloc[0])], 'daily_forecast_1': [forecast_values[0]], 'daily_forecast_2':[forecast_values[1]]})

forecasted_spark_df = data_selected_groups.groupby('Combined_Key').apply(holt_winters_time_series_udf)
forecasted_spark_df.display()

# COMMAND ----------

# MAGIC %md Sample code taken from this article about timeseries forecasting using pandas UDF in spark: 
# MAGIC 
# MAGIC https://medium.com/walmartglobaltech/multi-time-series-forecasting-in-spark-cc42be812393
# MAGIC 
# MAGIC https://github.com/maria-alphonsa-thomas/Multi-Time-Series-Pyspark-Pandas-UDF

# COMMAND ----------


from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType,FloatType

import statsmodels.tsa.api as sm
import numpy as np
import pandas as pd

import urllib.request  # the lib that handles the url stuff

data = urllib.request.urlopen("https://raw.githubusercontent.com/maria-alphonsa-thomas/Multi-Time-Series-Pyspark-Pandas-UDF/master/kaggle/train.csv").read()

file1 = open("/dbfs/mnt/ktam/timeseries.csv", "wb")
file1.write(data)
file1.close()

# read the entire data as spark dataframe
data = spark.read.format('csv').options(header='true', inferSchema='true').load('/mnt/ktam/timeseries.csv')\
.select('Store','Dept','Date','Weekly_Sales')

data.printSchema()

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
forecasted_spark_df = data.groupby(['Store','Dept']).apply(holt_winters_time_series_udf)


## to see the forecasted results
forecasted_spark_df.show(100)

# COMMAND ----------


