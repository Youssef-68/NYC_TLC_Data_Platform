-- dim_vendor
INSERT INTO dim_vendor (vendor_id)
SELECT DISTINCT vendor_id
FROM stg_trips
WHERE vendor_id IS NOT NULL
ON CONFLICT DO NOTHING;

-- dim_date
INSERT INTO dim_date (full_date, year, month, day)
SELECT
    d::date,
    EXTRACT(YEAR FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d)
FROM generate_series(
    (SELECT MIN(DATE(pickup_ts)) FROM stg_trips),
    (SELECT MAX(DATE(pickup_ts)) FROM stg_trips),
    INTERVAL '1 day'
) d
ON CONFLICT (full_date) DO NOTHING;


-- fact
INSERT INTO fact_trips (
    vendor_key,
    date_key,
    pickup_location_id,
    dropoff_location_id,
    trip_distance,
    fare_amount,
    total_amount
)
SELECT
    dv.vendor_key,
    dd.date_key,
    s.pickup_location_id,
    s.dropoff_location_id,
    s.trip_distance,
    s.fare_amount,
    s.total_amount
FROM stg_trips s
LEFT JOIN dim_vendor dv ON s.vendor_id = dv.vendor_id
LEFT JOIN dim_date dd ON DATE(s.pickup_ts) = dd.full_date;