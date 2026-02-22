-- Location Dimension
CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    borough TEXT,
    zone TEXT,
    service_zone TEXT
);

-- Fact Table for Trips
CREATE TABLE fact_trip (
    trip_id BIGINT PRIMARY KEY,
    vendor_id INT,
    pickup_time TIMESTAMP,
    dropoff_time TIMESTAMP,
    total_time_travel VARCHAR(20),
    passenger_count INT,
    trip_distance FLOAT,
    rate_code_id INT,
    store_fwd_bool BOOLEAN,

    pickup_location_id INT REFERENCES dim_location(location_id),
    dropoff_location_id INT REFERENCES dim_location(location_id),
    
    payment_type FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    year INT,
    month INT
);