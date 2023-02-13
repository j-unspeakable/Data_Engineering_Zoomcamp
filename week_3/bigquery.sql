-- Create an external table using the fhv 2019 data.
CREATE OR REPLACE EXTERNAL TABLE `famous-strategy-377009.dezoomcamp.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://aoj_bucket_1/data/2019/fhv_tripdata_2019-*.csv.gz']
);

-- Create a non partitioned nor clustered table from the fhv trip dat of 2019.
CREATE OR REPLACE TABLE `famous-strategy-377009.dezoomcamp.fhv_tripdata` AS 
SELECT * FROM `famous-strategy-377009.dezoomcamp.external_fhv_tripdata`;

-- Count the distinct number of affiliated_base_number for the external_fhv_tripdata.
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `famous-strategy-377009.dezoomcamp.external_fhv_tripdata`;

-- Count the distinct number of affiliated_base_number for the fhv_tripdata.
SELECT COUNT(DISTINCT(Affiliated_base_number)) FROM `famous-strategy-377009.dezoomcamp.fhv_tripdata`;

-- Records with a blank (null) PUlocationID and DOlocationID in the entire datase.
SELECT COUNT(*) FROM `famous-strategy-377009.dezoomcamp.fhv_tripdata` AS fhv
WHERE fhv.PUlocationID is Null AND fhv.DOlocationID is Null;

-- Create a table partitioned by pickup_datetime and clustered by Affiliated_base_number.
CREATE OR REPLACE TABLE `famous-strategy-377009.dezoomcamp.fhv_tripdata_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS (
  SELECT * FROM `famous-strategy-377009.dezoomcamp.fhv_tripdata`
);

-- Retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive). Non-partitioned and Non-clustered table.
SELECT DISTINCT(Affiliated_base_number) FROM  `famous-strategy-377009.dezoomcamp.fhv_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

-- Retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive). Partitioned and Clustered table.
SELECT DISTINCT(Affiliated_base_number) FROM  `famous-strategy-377009.dezoomcamp.fhv_tripdata_partitioned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';