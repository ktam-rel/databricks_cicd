import urllib.request  # the lib that handles the url stuff
import json

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

def download_file_from_dataframe(df, base_path):
    url = urllib.request.urlopen(df.download_url)
    data = url.read().decode(url.headers.get_content_charset())
    writepath = base_path + df.name
    with open(writepath, 'w') as f:
        f.write(data)

def download_all(dbutils, dir_path, files):
    dbutils.fs.rm(dir_path, True)
    dbutils.fs.mkdirs(dir_path)
    files.filter(files.name.endswith('.csv')).foreach(lambda f: download_file_from_dataframe(f, dir_path))

