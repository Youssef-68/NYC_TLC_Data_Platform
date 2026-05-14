-- Row counts check
SELECT 'fact_trips' AS table_name, COUNT(*) FROM fact_trips
UNION ALL
SELECT 'dim_vendor', COUNT(*) FROM dim_vendor
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dim_date;

-- Check the date
SELECT MIN(full_date), MAX(full_date), COUNT(*)
FROM dim_date;

-- Check Nulls
SELECT COUNT(*) 
FROM fact_trips
WHERE vendor_key IS NULL OR date_key IS NULL;