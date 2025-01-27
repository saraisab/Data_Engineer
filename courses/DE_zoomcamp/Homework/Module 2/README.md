
 # Module 2 Homework
 ## DE Zoomcamp
 SQL Quizz Questions
 ## Question 3
How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

```
SELECT 
  COUNT(1) 
FROM 
  `yellow_tripdata` 
WHERE 
  LEFT(filename,20) = 'yellow_tripdata_2020';
  ```
Solution: 24,648,499
    
## Question 4
How many rows are there for the Green Taxi data for all CSV files in the year 2020?

```
SELECT 
  COUNT(1) 
FROM 
  `green_tripdata` 
WHERE 
  LEFT(filename,19) = 'green_tripdata_2020'

```
Solution: 1,734,051

## Question 5
How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

```
SELECT 
    COUNT(1) 
FROM 
    `yellow_tripdata_2021_03_ext`
```
Solution: 1,925,152

