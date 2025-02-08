# Module 3 - Data Warehouse Homework

* First of all, I had to add the google-cloud-storage module to my Python interpreter in Pycharm
* Secondly, I add my credentials.json to the new directory called 'credentials'
* Finally, this is the script that I have executed:

```python
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
import time

# Change this to your bucket name
BUCKET_NAME = "my_bucket_name"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "credentials/credentials.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "files"

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  

    print("All files processed and verified.")
```

## SQL QUERIES

* Create external table
```sql
CREATE OR REPLACE EXTERNAL TABLE `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024_external` 
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://celtic-surface-447817-d0-terra-bucket/yellow_tripdata_2024-*.parquet']
);
```
*Create table
```sql
CREATE OR REPLACE TABLE 
  `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024` 
AS
SELECT 
  * 
FROM 
  `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024_external`;
```

### Question 1:

Question 1: What is count of records for the 2024 Yellow Taxi Data?

    65,623
    840,402
    20,332,093
    85,431,289

Solution: 20,332,093
```
SELECT 
  COUNT(1) 
FROM
   `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024`;
```
### Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

    18.82 MB for the External Table and 47.60 MB for the Materialized Table
    0 MB for the External Table and 155.12 MB for the Materialized Table
    2.14 GB for the External Table and 0MB for the Materialized Table
    0 MB for the External Table and 0MB for the Materialized Table

Solution: 0 MB for the External Table and 155.12 MB for the Materialized Table
```SQL
SELECT 
  DISTINCT PULocationID
FROM
   `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024`;
  

SELECT 
  DISTINCT PULocationID
FROM
   `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024_external`;
```

### Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

* BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
* BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
* BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
* When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed
```SQL 
SELECT 
  PULocationID
FROM
   `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024`;

SELECT 
  PULocationID,
  DOLocationID
FROM
   `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024`;
```
Solution: 

BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

### Question 4:

How many records have a fare_amount of 0?

    128,210
    546,578
    20,188,016
    8,333
```
SELECT 
  DISTINCT PULocationID
FROM
   `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024_external`;
```
Solution: 8333

### Question 5:

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

Partition by tpep_dropoff_datetime and Cluster on VendorID
Cluster on by tpep_dropoff_datetime and Cluster on VendorID
Cluster on tpep_dropoff_datetime Partition by VendorID
Partition by tpep_dropoff_datetime and Partition by VendorID

```SQL
CREATE OR REPLACE TABLE `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024_partitioned_clustered` 
PARTITION BY
  DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024_external`;
```
Solution: Partition by tpep_dropoff_datetime and Cluster on VendorID

### Question 6:

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

    12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
    310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
    5.87 MB for non-partitioned table and 0 MB for the partitioned table
    310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

```SQL
SELECT 
  DISTINCT VendorID
FROM
  `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024`
WHERE 
  tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT 
  DISTINCT VendorID
FROM
  `celtic-surface-447817-d0.dataset_ny_taxi.yellow_tripdata_2024_partitioned_clustered` 
WHERE 
  tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
  ```
  Solution: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

### Question 7:

Where is the data stored in the External Table you created?

    Big Query
    Container Registry
    GCP Bucket
    Big Table

Solution: GCP Bucket

### Question 8:

It is best practice in Big Query to always cluster your data:

    True
    False
Solution: False 

For instance: When the bd is small it is not convenient.

### (Bonus: Not worth points) Question 9:
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read?

0 B

Why?

Because BigQuery can determinate how many rows a table has avoiding read every row in order to reduce the cost.
