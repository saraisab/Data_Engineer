# Module 5 Homework

## Question 1: Install Spark and PySpark
```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

spark.version
```
* Solution: '3.3.2'
---

## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

* Solution: 25MB
```python
df_yellow = spark.read.parquet('yellow_tripdata_2024-10.parquet')

output_path = 'data/pq/yellow/2024/10/'

df_yellow \
    .repartition(4) \
    .write.parquet(output_path)
```

## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

* Solution: 125,567
```python
from pyspark.sql import functions as F

df_yellow.createOrReplaceTempView('trips_data')
df_result = spark.sql("""
    SELECT 
        COUNT(*) AS number_trips
    FROM 
        trips_data
    WHERE
        tpep_pickup_datetime BETWEEN '2024-10-15 00:00:00' AND '2024-10-15 23:59:59'
    """).show()
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

* Solution: 162
```python
df_diff = spark.sql("""
    SELECT 
        (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 3600 AS hours_dif
    FROM 
        trips_data
    ORDER BY 
        hours_dif DESC
    LIMIT 1;
""").show()
```

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080


* Solution: 4040


## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

* Solution: Governor's Island/Ellis Island/Liberty Island

```python
df_zone = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

# Changing the LocationId type
from pyspark.sql.types import IntegerType, TimestampType
df_zone = df_zone.withColumn("LocationID", df_zone["LocationID"].cast(IntegerType()))

df_zone.createOrReplaceTempView('zones')
df_last_zone = spark.sql("""
    SELECT 
        COUNT(trips_data.tpep_pickup_datetime) as number_picksups
        ,z_pu.Zone as pickup_location_zone
    FROM
        trips_data
        INNER JOIN zones as z_pu ON z_pu.LocationID = trips_data.PULocationID
    GROUP BY
        z_pu.Zone
    ORDER BY
        number_picksups ASC
    LIMIT 1;
""").show()
```