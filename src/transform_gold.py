from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, TimestampNTZType, LongType

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_to_gold():
    spark_gold = SparkSession.builder \
        .appName("NYC Taxi Gold Transformation - 2") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.caseSensitive", "false") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .getOrCreate()
    
    spark_gold.catalog.clearCache()
    
    #.config("spark.sql.parquet.mergeSchema", "true") \
    #.config("spark.sql.parquet.validateSchema", "false") \

    # After explore the dataframe created the schema for fast processing
    schema_fields = StructType([
        StructField("VendorID",                 LongType(),         True),
        StructField("tpep_pickup_datetime",     TimestampNTZType(), True),
        StructField("tpep_dropoff_datetime",    TimestampNTZType(), True),
        StructField("passenger_count",          DoubleType(),       True),
        StructField("trip_distance",            DoubleType(),       True),
        StructField("RatecodeID",               DoubleType(),       True),
        StructField("store_and_fwd_flag",       StringType(),       True),
        StructField("PULocationID",             IntegerType(),      True),
        StructField("DOLocationID",             IntegerType(),      True),
        StructField("payment_type",             IntegerType(),      True),
        StructField("fare_amount",              DoubleType(),       True),
        StructField("extra",                    DoubleType(),       True),
        StructField("mta_tax",                  DoubleType(),       True),
        StructField("tip_amount",               DoubleType(),       True),
        StructField("tolls_amount",             DoubleType(),       True),
        StructField("improvement_surcharge",    DoubleType(),       True),
        StructField("total_amount",             DoubleType(),       True),
        StructField("congestion_surcharge",     DoubleType(),       True),
        StructField("airport_fee",              DoubleType(),       True),
        StructField("ingestion_timestamp",      TimestampType(),    True),
        StructField("source_file",              StringType(),       True)
    ])

    # Read data
    df_bronze = spark_gold.read.parquet("data/bronze/nyc_taxi/trip_data/", schema=schema_fields)

    df_zones = spark_gold.read.option("header", "true").csv("data/bronze/nyc_taxi/taxi_zone/taxi_zone_lookup.csv")

    # Cleaning and normalization
    # Filter for inconsistent records (ex: negative distances or amounts)
    df_cleaned = df_bronze.filter("trip_distance > 0 AND total_amount > 0")

    # Create Dimension Table for Locations
    dim_location = df_zones.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone")
    )

    # Clean and transform the store_and_fwd_flag to boolean
    df_cleaned = df_cleaned.withColumn("store_and_fwd_flag",
        F.when(F.col("store_and_fwd_flag") == "Y", True)
        .when(F.col("store_and_fwd_flag") == "N", False)
        .otherwise(None) # For any other value, set as null
    )

    fact_trips = df_cleaned.select(
        F.monotonically_increasing_id().alias("trip_id"), # Surrogate key
        F.col("VendorID").cast("long").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_time"),
        F.col("tpep_dropoff_datetime").alias("dropoff_time"),
        F.regexp_extract((F.col("tpep_dropoff_datetime") - F.col("tpep_pickup_datetime")).cast("string"), r"(\d{2}:\d{2}:\d{2})", 1).alias("total_time_travel"), # New column for total travel time
        F.col("passenger_count").cast("double").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double"),
        F.col("RatecodeID").cast("double").cast("int").alias("rate_code_id"),
        F.col("store_and_fwd_flag").alias("store_fwd_bool"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("payment_type").cast("int"),
        F.col("fare_amount").cast("double"),
        F.col("extra").cast("double"),
        F.col("mta_tax").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("tolls_amount").cast("double"),
        F.col("improvement_surcharge").cast("double"),
        F.col("total_amount").cast("double"),
        F.col("congestion_surcharge").cast("double"),
        F.col("airport_fee").cast("double"),
        F.col("year"),
        F.col("month")
    )

    # Write to Gold layer
    # Partition by year and month for better query performance
    output_path = f"data/gold/nyc_taxi/fact_trip/"
    fact_trips.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)
    
    logging.info("Fact table created successfully in Gold layer.")

    dim_location.write.mode("overwrite") \
        .parquet("data/gold/nyc_taxi/dim_location/")
    
    logging.info("Dimension model created successfully in Gold layer.")

if __name__ == "__main__":
    transform_to_gold()