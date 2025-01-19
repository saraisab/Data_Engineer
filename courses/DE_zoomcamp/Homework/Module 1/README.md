## Question 2 
* First of all, I have created the Dockerfile and docker-compose.yaml. After that, I have started the two containers:

` docker-compose up `

* Secondly, I have download the csv files to a new directory called csv_files and descompress the gz file to a csv.

`cd csv_files`
```
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

gunzip green_tripdata_2019-10.csv.gz
```
* Finaly, I have executed all the code from the jupyter notebook file check_data.ypynb in order to analize the data and create and fill the tables in the db.

## Question 3
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

* Up to 1 mile

   ```
    SELECT 
	    COUNT(*) AS trips_up_1_mil
    FROM 
	    green_taxi_data
    WHERE 
	    trip_distance <= 1
  	    AND lpep_dropoff_datetime >= '2019-10-01'
  	    AND lpep_dropoff_datetime < '2019-11-01';
   ```
   Result: 104802
 * In between 1 (exclusive) and 3 miles (inclusive),
   ```
    SELECT 
        COUNT(*) AS trips_1_3
    FROM 
        green_taxi_data
    WHERE 
	    trip_distance > 1
	    AND trip_distance <= 3
        AND lpep_dropoff_datetime >= '2019-10-01'
        AND lpep_dropoff_datetime < '2019-11-01';
   ```
   Result: 198924

 * In between 3 (exclusive) and 7 miles (inclusive),
```
SELECT 
	COUNT(*) AS trips_1_7_mil
FROM 
	green_taxi_data
WHERE 
	trip_distance > 3
	AND trip_distance <= 7
  	AND lpep_dropoff_datetime >= '2019-10-01'
  	AND lpep_dropoff_datetime < '2019-11-01';
   
   ```
   Result: 109603
 * In between 7 (exclusive) and 10 miles (inclusive),
```
   SELECT 
	COUNT(*) AS trips_7_10_mil
FROM 
	green_taxi_data
WHERE 
	trip_distance > 7
	AND trip_distance <= 10
  	AND lpep_dropoff_datetime >= '2019-10-01'
  	AND lpep_dropoff_datetime < '2019-11-01';
   ```
   Result 27678
 * Over 10 miles
```
SELECT 
	COUNT(*) AS trips_over_10_mil
FROM 
	green_taxi_data
WHERE 
	trip_distance > 10
  	AND lpep_dropoff_datetime >= '2019-10-01'
  	AND lpep_dropoff_datetime < '2019-11-01';
   
   ```

Result: 35189
## Question 4
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

    SELECT DISTINCT 
	    MAX(trip_distance) AS MaxTripDistance,
	    lpep_pickup_datetime
    FROM 
	    green_taxi_data
    GROUP BY
        lpep_pickup_datetime
    ORDER BY 
        MaxTripDistance DESC
    LIMIT 1;

        
Result:  2019-10-31

## Question 5
```
SELECT 
	SUM(GTD.total_amount) AS TotAmount, 
	ZPU."Zone"
FROM 
	green_taxi_data AS GTD
	INNER JOIN zone ZPU ON GTD."PULocationID" = ZPU."LocationID"
WHERE
	DATE(GTD.lpep_pickup_datetime) = '2019-10-18'
GROUP BY 
	ZPU."Zone"
ORDER BY TotAmount DESC
LIMIT 3;
````

East Harlem North, East Harlem South, Morningside Heights

## Question 6
For the passengers picked up in October 2019 in the zone name "East Harlem North" which was the drop off zone that had the largest tip?

Note: it's tip , not trip

We need the name of the zone, not the ID.


```
SELECT 
	ZPU."Zone",
	MAX(GT."tip_amount") AS MaxTip,
	(SELECT 
		ZON."Zone" 
	FROM
		zone ZON
	WHERE
		ZON."LocationID" = GT."DOLocationID") AS ZoneDOLargestTip
	
FROM 
	green_taxi_data GT
	INNER JOIN zone ZPU ON ZPU."LocationID" = GT."PULocationID"
WHERE 		
	TO_CHAR(GT."lpep_pickup_datetime", 'YYYY-MM') = '2019-10'
	AND ZPU."Zone" = 'East Harlem North'
GROUP BY 
	ZPU."Zone",
	GT."DOLocationID"
ORDER BY 
	MaxTip DESC
LIMIT 1;
```
Result: JFK Airport

## Question 7
Which of the following sequences, respectively, describes the workflow for:

* Downloading the provider plugins and setting up backend,
* Generating proposed changes and auto-executing the plan
* Remove all resources managed by terraform`


Result: terraform init, terraform apply -auto-approve, terraform destroy


---

:blush: