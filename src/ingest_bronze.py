from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests
import sys
import argparse
import os

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_data(years_months: list):
    spark = SparkSession.builder \
        .appName("NYC Taxi Bronze Ingestion") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    # URL Base
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"

    os.makedirs("data/landing/nyc_taxi/", exist_ok=True)
    os.makedirs("data/bronze/nyc_taxi/trip_data", exist_ok=True)
    os.makedirs("data/gold/nyc_taxi/dim_location/", exist_ok=True)
    os.makedirs("data/gold/nyc_taxi/fact_trip/", exist_ok=True)
    
    for year, month in years_months:
        # Format month (ex: 01)
        month_str = f"{month:02d}"
        file_url = f"{base_url}_{year}-{month_str}.parquet"
        local_path = f"data/landing/nyc_taxi/yellow_tripdata_{year}-{month_str}.parquet"
        
        logging.info(f"Reading data from: {year}-{month_str}")
        
        try:
            # Download file to local
            response = requests.get(file_url)
            if response.status_code != 200:
                logging.error(f"Failed to download {file_url}: Status code {response.status_code}")
            elif len(response.content) == 0:
                logging.error(f"Downloaded file {file_url} is empty.")
            elif response.status_code == 200 and len(response.content) > 0:
                with open(local_path, "wb") as f:
                    f.write(response.content)
            
            df = spark.read.parquet(local_path)
            
            # Adding ingestion metadata
            df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
                   .withColumn("source_file", F.lit(file_url)) \
                   .withColumn("year", F.lit(year)) \
                   .withColumn("month", F.lit(month_str))

            # Write on GCS/Local partitioneted by year and month
            # Overwrite for idempotency
            output_path = f"data/bronze/nyc_taxi/trip_data/"
            df.write.mode("overwrite") \
                .partitionBy("year", "month") \
                .parquet(output_path)
                
            logging.info(f"Success: {year}-{month_str}")
        except Exception as e:
            logging.error(f"Error processing {year}-{month_str}: {e}")

if __name__ == "__main__":
    # Parser configs
    parser = argparse.ArgumentParser(description="Data ingestion NYC Taxi on bronze layer")
    parser.add_argument(
        "--months", 
        type=str, 
        required=True, 
        help="Month list using format YYYY-MM separated by comma (ex: 2023-01,2023-02)"
    )
    
    args = parser.parse_args()
    
    # Convert input string to list of tuples (year, month)
    try:
        raw_months = args.months.split(",")
        target_period = []
        for item in raw_months:
            year, month = item.strip().split("-")
            target_period.append((int(year), int(month)))
        
        ingest_data(target_period)
    except ValueError:
        logging.error("Date format invalid. Use YYYY-MM.")
    except Exception as e:
        logging.error(f"Error parsing input months: {e}. Correct format is YYYY-MM, separated by commas (ex: 2023-01,2023-02)")