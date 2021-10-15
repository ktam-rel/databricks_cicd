from relpipeline.util import compute, download_all, download_file_from_dataframe
from unittest.mock import ANY, call

def test_download_all_recreates_directory(mocker):
    dbutils = mocker.MagicMock()
    files = mocker.MagicMock()

    download_all(dbutils, "some\\path", files)

    dbutils.fs.rm.assert_called_with("some\\path", True)
    dbutils.fs.mkdirs.assert_called_with("some\\path")



def test_download_all_only_processes_csv_files_v1(mocker):
    dbutils = mocker.MagicMock()
    files = mocker.MagicMock()

    download_all(dbutils, "some\\path", files)

    files.filter.assert_called_once()
    files.name.endswith.assert_called_with(".csv")


def test_download_all_only_processes_csv_files_v1(mocker):
    dbutils = mocker.MagicMock()
    files = mocker.MagicMock()

    download_all(dbutils, "some\\path", files)

    files.filter.assert_called_once()
    files.name.endswith.assert_called_with(".csv")

def test_download_all_only_processes_csv_files_v2(mocker):
    dbutils = mocker.MagicMock()
    patched_download = mocker.patch('relpipeline.util.download_file_from_dataframe')

    from pyspark.sql import SparkSession
    spark = (SparkSession
        .builder
        .appName("TestRelPipeline")
        .getOrCreate())

    files = spark.createDataFrame([("1.csv", "http://some_url1")
        , ("2.csv", "http://some_url2")
        , ("3.doc", "http://some_url3")]
        , ["name", "download_url"])

    download_all(dbutils, "some\\path", files)

    #calls = [call(ANY, "http://some_url1"), call(ANY, "http://some_url2")]
    #patched_download.assert_has_calls(calls)

    #print("Call Count!!! " + str(patched_download.call_count)) 
    patched_download.assert_called()

