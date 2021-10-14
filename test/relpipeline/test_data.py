from relpipeline.util import compute, download_all


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


    # In Python
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg
    # Create a DataFrame using SparkSession
    spark = (SparkSession
        .builder
        .appName("AuthorsAges")
        .getOrCreate())
    # Create a DataFrame
    data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),
        ("TD", 35), ("Brooke", 25)], ["name", "age"])
    # Group the same names together, aggregate their ages, and compute an average
    avg_df = data_df.groupBy("name").agg(avg("age"))
    # Show the results of the final execution
    avg_df.show()