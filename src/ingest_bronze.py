from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests
import sys

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_data(years_months: list):
    spark = SparkSession.builder \
        .appName("NYC Taxi Bronze Ingestion") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    # URL Base
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"
    
    for year, month in years_months:
        # Format month (ex: 01)
        month_str = f"{month:02d}"
        file_url = f"{base_url}_{year}-{month_str}.parquet"
        local_path = f"yellow_tripdata_{year}-{month_str}.parquet"
        
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
    # Example input: list of tuples (year, month)
    # In production, this would come from command line or orchestrator arguments.
    target_period = [(2023, 4), (2023, 5), (2023, 6)] # Example: Ingest data for April, May, June 2023
    ingest_data(target_period)