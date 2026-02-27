import pytest
from pyspark.sql import SparkSession
from src.transform_gold import clean_trip_data, convert_store_flag, calculate_travel_time
from datetime import datetime

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

def test_clean_trip_data_filter_function(spark):
    data = [
        (1, 0.0, 10.0), # Must be removed (zero distance)
        (2, 5.5, 20.0), # Must be kept
        (3, 3.0, -5.0)  # Must be removed (negative amount)
    ]
    df = spark.createDataFrame(data, ["VendorID", "trip_distance", "total_amount"])
    
    df_result = clean_trip_data(df)
    
    assert df_result.count() == 1
    assert df_result.collect()[0]["VendorID"] == 2

def test_convert_store_flag_boolean_function(spark):
    data = [("Y",), ("N",), ("garbage",), (None,)]
    df = spark.createDataFrame(data, ["store_and_fwd_flag"])
    
    df_result = convert_store_flag(df)
    
    results = df_result.collect()
    assert results[0]["store_and_fwd_flag"] is True
    assert results[1]["store_and_fwd_flag"] is False
    assert results[2]["store_and_fwd_flag"] is None

def test_calculate_travel_time_real_function(spark):
    data = [(datetime(2023, 1, 1, 12, 0, 0), datetime(2023, 1, 1, 12, 30, 0))]
    df = spark.createDataFrame(data, ["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    
    df_result = calculate_travel_time(df)
    
    assert df_result.collect()[0]["total_time_travel"] == "00:30:00"