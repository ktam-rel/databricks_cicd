# Databricks notebook source
# MAGIC %md ## Train a model from DeltaLake table
# MAGIC 
# MAGIC Code derived from this article about timeseries forecasting using pandas UDF in spark: 
# MAGIC 
# MAGIC https://medium.com/walmartglobaltech/multi-time-series-forecasting-in-spark-cc42be812393
# MAGIC 
# MAGIC https://github.com/maria-alphonsa-thomas/Multi-Time-Series-Pyspark-Pandas-UDF

# COMMAND ----------


# import the necessary pyspark and pandas libraries

from pyspark.sql.functions import pandas_udf, PandasUDFType, unix_timestamp, col, substring
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,FloatType


import statsmodels.tsa.api as sm
import numpy as np
import pandas as pd

data = spark.read.format("delta").load("/mnt/ktam/delta/output_delta").select('Combined_Key', 'LastUpdate', 'Deaths')
selected_com = data.groupBy(['Combined_Key']).count().filter("count > 100").select("Combined_Key")
data_selected_groups = data.join(selected_com,['Combined_Key'],'inner')


##pandas udf
schema = StructType([StructField('Combined_Key', StringType(), True),
                     StructField('daily_forecast_1', IntegerType(), True),
                     StructField('daily_forecast_2', IntegerType(), True)])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def holt_winters_time_series_udf(data):
  
    time_series_data = data['Deaths']
    

    ##the model
    model = sm.ExponentialSmoothing(np.asarray(time_series_data),trend='add').fit()

    ##forecast values
    forecast_values = pd.Series(model.forecast(2),name = 'fitted_values')
    
    
    return pd.DataFrame({'Combined_Key': [str(data.Combined_Key.iloc[0])], 'daily_forecast_1': [forecast_values[0]], 'daily_forecast_2':[forecast_values[1]]})

forecasted_spark_df = data_selected_groups.groupby('Combined_Key').apply(holt_winters_time_series_udf)
forecasted_spark_df.display()

# COMMAND ----------


