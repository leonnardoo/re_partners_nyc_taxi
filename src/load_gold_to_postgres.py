from pyspark.sql import SparkSession
from dotenv import load_dotenv
import argparse
import os

# For load gold to BigQuery (if needed)
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

load_dotenv("config/.env")

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_gold_to_postgres(user_pg: str = None, password_pg: str = None, database_pg: str = None):
    spark = SparkSession.builder \
        .appName("Load Gold to DB") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()

    # Database connection properties
    database_pg = os.getenv("DATABASE_PG") if not database_pg else database_pg
    user_pg = os.getenv("USER_PG") if not user_pg else user_pg
    password_pg = os.getenv("PASSWORD_PG") if not password_pg else password_pg

    db_url = f"jdbc:postgresql://localhost:5432/{database_pg}"

    logging.info(f"Connecting to PostgreSQL database: {database_pg} with user: {user_pg} and password: {password_pg}")

    logging.info("Starting to load data from Gold to PostgreSQL...")

    # Load dimension
    dim_df = spark.read.parquet("data/gold/nyc_taxi/dim_location/")

    dim_df.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "dim_location") \
    .option("user", user_pg) \
    .option("password", password_pg) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    

    # Load fact table
    fact_df = spark.read.parquet("data/gold/nyc_taxi/fact_trip/")

    fact_df.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "fact_trip") \
    .option("user", user_pg) \
    .option("password", password_pg) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

    logging.info("Loading to PostgreSQL completed successfully.")


def load_gold_to_bigquery(project_id, dataset_id, table_id, file_path, location="US"):
    client = bigquery.Client(project=project_id)
    
    # Check if dataset exists, if not create it
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        logging.info(f"Dataset {dataset_id} not found. Creating...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        logging.info(f"Dataset {dataset_id} created successfully.")

    # Configure the load job
    table_full_id = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        # 'WRITE_TRUNCATE' replace table, 'WRITE_APPEND' append to table, 'WRITE_EMPTY' fails if table is not empty
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
    )

    # Execute the load job
    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(
            source_file, table_full_id, job_config=job_config
        )
    
    logging.info(f"Starting upload of file {file_path}...")
    load_job.result()  # Waiting job to complete
    logging.info(f"File {file_path} uploaded successfully to {table_full_id}.")

    # Verify the load
    destination_table = client.get_table(table_full_id)
    logging.info(f"Success! The table now has {destination_table.num_rows} rows.")



if __name__ == "__main__":
    # For load on BigQuery, you would first create the schema (Terraform) and then load the data:
    # load_gold_to_bigquery("project-id", "gold_nyc_taxi", "dim_location", "data/gold/nyc_taxi/dim_location/")
    # load_gold_to_bigquery("project-id", "gold_nyc_taxi", "fact_trip", "data/gold/nyc_taxi/fact_trip/"))

    # Parser configs
    parser = argparse.ArgumentParser(description="Variable input for loading Gold data to PostgreSQL")
    parser.add_argument("--user_pg", type=str, required=False, help="PostgreSQL user name")
    parser.add_argument("--password_pg", type=str, required=False, help="PostgreSQL password")
    parser.add_argument( "--database_pg", type=str, required=False, help="PostgreSQL database name")

    args = parser.parse_args()
    
    try:
        load_gold_to_postgres(args.user_pg, args.password_pg, args.database_pg)
    except Exception as e:
        logging.error(f"Error loading gold data to PostgreSQL: {e}")