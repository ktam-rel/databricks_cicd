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


import urllib.request  # the lib that handles the url stuff
import json

def get_json_from_url(url):
    context = urllib.request.urlopen(url)
    response = context.read()
    encoding = context.info().get_content_charset('utf-8')
    return json.loads(response.decode(encoding))

def get_fileinfo_dataframe_from_json(json_obj):
    paths = list(map(lambda f: (f["path"], f["download_url"]), json_obj))
    pathsSchema = ['path', 'download_url']
    return spark.createDataFrame(paths, pathsSchema)
