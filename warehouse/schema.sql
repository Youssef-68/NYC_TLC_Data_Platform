DROP TABLE IF EXISTS fact_trips;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_vendor;

-- Staging
CREATE TABLE IF NOT EXISTS stg_trips (
    vendor_id INT,
    pickup_ts TIMESTAMP,
    dropoff_ts TIMESTAMP,
    pickup_location_id INT,
    dropoff_location_id INT,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    year INT,
    month INT
);

-- Vendor dim
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key SERIAL PRIMARY KEY,
    vendor_id INT UNIQUE
);

CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

-- Fact
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_key SERIAL PRIMARY KEY,
    vendor_key INT,
    date_key INT,
    pickup_location_id INT,
    dropoff_location_id INT,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION
);