import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

def test_data_cleaning(spark):
    # Create a sample DataFrame with some inconsistent records
    data = [
        (1, 0.0, 10.0), # Must be removed (zero distance)
        (2, 5.5, 20.0), # Must be kept
        (3, 3.0, -5.0)  # Must be removed (negative amount)
    ]
    columns = ["VendorID", "trip_distance", "total_amount"]
    df = spark.createDataFrame(data, columns)
    
    # Apply the cleaning logic (this should be the same as in transform_gold.py)
    df_cleaned = df.filter("trip_distance > 0 AND total_amount > 0")
    
    assert df_cleaned.count() == 1
    assert df_cleaned.collect()[0]["VendorID"] == 2