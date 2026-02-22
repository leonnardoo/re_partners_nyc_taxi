from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv("config/.env")

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_gold_to_postgres():
    spark = SparkSession.builder \
        .appName("Load Gold to DB") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()

    # Database connection properties
    database = os.getenv("DATABASE")
    user = os.getenv("USER")
    password = os.getenv("PASSWORD")

    db_url = f"jdbc:postgresql://localhost:5432/{database}"

    db_properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    # Load dimension
    dim_df = spark.read.parquet("data/gold/nyc_taxi/dim_location/")

    dim_df.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "dim_location") \
    .option("user", "test") \
    .option("password", "test_pass") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    

    # Load fact table
    fact_df = spark.read.parquet("data/gold/nyc_taxi/fact_trip/")

    fact_df.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "fact_trip") \
    .option("user", "test") \
    .option("password", "test_pass") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    logging.info("Loading to PostgreSQL completed successfully.")

if __name__ == "__main__":
    load_gold_to_postgres()