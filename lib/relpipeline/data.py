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




import time

def compute(x):
    response = expensive_api_call()
    return response + x

def expensive_api_call():
    time.sleep(10) # takes 1,000 seconds
    return 123


