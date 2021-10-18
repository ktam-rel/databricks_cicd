import urllib.request  # the lib that handles the url stuff
import json
from pyspark.sql.functions import col, substring
from pyspark.sql.types import IntegerType


def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils

def mount(dbutils, container, storage_account, scope, key, mount_as):
    if any(mount.mountPoint == mount_as for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(mount_as)
    dbutils.fs.mount(
        source = f"wasbs://{container}@{storage_account}.blob.core.windows.net",
        mount_point = mount_as,
        extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net":dbutils.secrets.get(scope = scope, key = key)})

def get_json_from_url(url):
    context = urllib.request.urlopen(url)
    response = context.read()
    encoding = context.info().get_content_charset('utf-8')
    return json.loads(response.decode(encoding))

def get_fileinfo_dataframe_from_json(spark, json_obj):
    paths = list(map(lambda f: (f["name"], f["download_url"]), json_obj))
    pathsSchema = ['name', 'download_url']
    return spark.createDataFrame(paths, pathsSchema)

def download_file_from_dataframe(file_row, base_path):
    url = urllib.request.urlopen(file_row.download_url)
    data = url.read().decode(url.headers.get_content_charset())
    writepath = base_path + file_row.name
    with open(writepath, 'w') as f:
        f.write(data)

def filter_files_dataframe(files):
    return files.filter(files.name.endswith('.csv'))

def download_all(dbutils, dir_path, files):
    dbutils.fs.rm(dir_path, True)
    dbutils.fs.mkdirs(dir_path)
    filter_files_dataframe(files).foreach(lambda f: download_file_from_dataframe(f, dir_path))

def transform_csv_data(spark, csv_path):
    df = spark.read.option("header", "true").csv(csv_path + "/*.csv")
    df = df.withColumn("Last_Update", substring(col("Last_Update"),0,10))
    df = df.withColumn("Deaths", df["Deaths"].cast(IntegerType()))
    df = df.withColumnRenamed("Last_Update", "LastUpdate")
    return df

def write_df_to_delta_table(df, base_path):
    df.write.format("delta").mode("overwrite").save(base_path  + "/delta/output_delta")



