# NYC Taxi Mini Pipeline

Data pipeline using Apache Spark to process NYC Taxi data, load it into a database dimensional model, and provision core infrastructure using Terraform on GCP.

## Project Architecture

The project implements a simplified **Medallion Architecture** to process [NYC TLC](https://www.nyc.gov) data, structured as follows:

*   **Landing Layer**: Raw data stored in **Parquet**
*   **Bronze Layer**: Raw data with minimal increment stored in **Parquet** and **CSV** format.
*   **Gold Layer**: Dimensional model and Fact table (**Star Schema**) optimized for high-performance analytical queries.
*   **Database**: [PostgreSQL](https://www.postgresql.org) serves as the final consumption layer for SQL-based analysis and BI tools.

# Standart structure of bucket/folder:
Partitioned the tables by year and month because it's easier to delete and re-enter the data if necessary (backfill and independence).

< layer > / < subject > / < table > / < year > / < month >

### Landing Layer
* nyc_taxi/yellow_tripdata_YYYY-MM.parquet 

### Bronze Layer
- nyc_taxi/taxi_zone/taxi_zone_lookup.csv
- nyc_taxi/trip_data/YEAR=YYYY/MONTH=MM/*.parquet

### Gold Layer
- nyc_taxi/dim_location/*.parquet
- nyc_taxi/fact_trip/YEAR=YYYY/MONTH=MM/*.parquet

### Database (tables)
- public.fact_trip
- public.dim_location

## Project Structure
Chose UV because it's a Python package manager that's simple and easy to understand.

```
config/                                 # Config variables folder
data/
├── landing/
    └── nyc_taxi/                       # Landing zone files
├── bronze/
    ├── trip_data/                      # Parquet file from landing
    └── taxi_zone/                      # CSV file from taxi_zone_lookup
└── gold/
    ├── dim_location/                   # Dimension table from location
    └── fact_trip/                      # Fact table from taxi trips
init-db/
└── init.sql                            # SQL file for creation dim and fact tables
notebook/
└── eda_nyc_taxy_trips.ipynb            # Notebook for exploration data (EDA)
src/
├── ingest_bronze.py                    # Spark job(1) load landing and ingest on bronze layer
├── transform_gold.py                   # Spark job(2) load from bronze to gold layer
└── load_gold_to_postgres.py            # Spark jbo(3) load from gold to Postgres(Docker)
terraform/                              # Terraform folder
(Files from terraform)
tests/                                  # Tests folder
(Python file for tests)
docker-compose.yml                      # Docker file to up Postgres
pyproject.toml                          # Project dependencies
README.md
```

# Requirements

- Python 3.10+
- UV (recommended)
- Terraform
- Docker

# Installation

1. Clone the repository.



2. Create virtual environment:
```bash
uv venv
```

3. Install dependencies:
```bash
uv sync
```

# Usage

Docker up
```bash
docker-compose up -d
```

Run bronze ingest
```bash
uv run src/ingest_bronze.py --month 2023-06
```

Run gold ingest
```bash
uv run src/transform_gold.py --month 2023-06
```

Run load to database
```bash
uv run src/load_gold_to_postgres.py --user_pg test --password_pg test_pass --database_pg nyc_taxi_warehouse
```

Secrets from user, password and database are examples used on this pipeline. In production this will be stored on Secret Manager (GCP) or github secrets.

Checking rows on database (PostgreSQL)
```bash
docker run --rm \
            --network host \
            -e PGPASSWORD=test_pass \
            postgres:15 \
            psql -h 127.0.0.1 -U test -d nyc_taxi_warehouse -c "
              SELECT 'dim_location' as table_name, COUNT(*) FROM dim_location
              UNION ALL
              SELECT 'fact_trip' as table_name, COUNT(*) FROM fact_trip;
            "
```

Dont forget to compsoer down to turned off Docker
```bash
docker-compose down -v
```

# 1. PySpark Job 1: Bronze Ingestion

In this step I added four columns to track and partitioning the data.

- ingestion_timestamp - Track the tingestion time
- source_file         - Track the file used for ingestion
- year                - Track wich year is uploaded (partition column)
- month               - Track wich month is uploaded (partition column)

# 2. PySpark Job 2: Dimensional Model Transformation

In this process, I perform an Exploratory Data Analysis (EDA) to understand the data before moving on to data modeling. Check the notebook on **notebook/eda_nyc_taxy_trips.ipynb**.
After understanding, I decided the schema_fields, data_cleaning and transformations.

- Data cleaning
    - The data was filtered considering distance and value greater than zero. Business logic. (trip_distance > 0 AND total_amount > 0)

- Data transformation
    - Change the column store_and_fwd_flag from string to boolean to bring more performance and integrity.
    - Create the fact table (fact_trip) on trip grain and dimension table (dim_location) for location data.
    - Change the column names (snake_case) and types for maintainability.

- Partition strategy
    - Added the column of year and month to partionated parquet files.

# 3. Build a Database Dimensional Model

Using PostgresSQL to load data from gold to SQL database.
Check the notebook to see the access to data on Postgres.

**A function was created for loading data into BigQuery in the file load_gold_to_postgress.sql to do that.**

# 4. Terraform GCP Infrastructure Definition
In the Terraform files, three buckets (landing, bronze and gold) were provisioned in the folder formats mentioned previously, with a **five year data lifecycle** before delete.

Three service accounts and their respective bucket access permissions were also provisioned.

One dataset was provided for gold layer.

# 5. Historical Data Logic
For historical data loading, the **ingest_data** function was created in the **ingest_bronze.py** file, which accepts tuples with **year** and **month** for processing. In the pyspark section, we have the **partitionOverwriteMode dynamic** option to perform the overwrite if the processed year/month already exists. With these options, we guarantee:
- Multiple historical months
- Re-run or backfill a specific month
- Avoids duplicates or data corruption

# 6. CI/CD
For CI/CD was created a GitHub Actions workflow to do:
- Linting and/or formatting
- Running Spark job tests (unit tests)
- Validating Terraform (terraform fmt + terraform validate)

## Limitations and what you would improve next
This specific project is builded and tested to run locally.
- Upgraded and tested to run on GCP
- Create a orchestrator to schedule the tasks (Composer - Airflow)
- Implement other subjects with this structure to check if has some failure on project structure
- Implement more tests
- Check if has other dimention tables for columns like payment_type, rate_code_id