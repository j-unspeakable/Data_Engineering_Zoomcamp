-- Query 1
SELECT COUNT(*) 
FROM green_taxi_data AS g 
WHERE EXTRACT(MONTH FROM g.lpep_pickup_datetime) = 1 and EXTRACT(DAY FROM g.lpep_pickup_datetime) = 15; 

-- Query 2
SELECT DATE(g.lpep_pickup_datetime) 
FROM green_taxi_data AS g 
WHERE trip_distance = (SELECT MAX(trip_distance) FROM green_taxi_data);

-- Query 3
SELECT COUNT(*) 
FROM green_taxi_data AS g 
WHERE DATE(g.lpep_pickup_datetime) = '2019-01-01'
AND g.passenger_count = 2;

-- Query 4
SELECT COUNT(*) 
FROM green_taxi_data AS g 
WHERE DATE(g.lpep_pickup_datetime) = '2019-01-01'
AND g.passenger_count = 3;

-- Query 5
SELECT z."Zone", g.tip_amount 
FROM green_taxi_data AS g 
JOIN taxi_zones AS z 
ON g."DOLocationID" = z."LocationID"
WHERE g."PULocationID" = 7
ORDER BY g.tip_amount DESC
Limit 1;