# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

### Answer 3.1.3

``` python
print(spark.version)
```


## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

### Answer B -26.1 MB

``` python
repartitioned_df = df.repartition(4)
output_path = "gs://zoom-batch-storage/output/module5"
repartitioned_df.write.mode("overwrite").parquet(output_path)
```

## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

### Answer C -128893

``` python
df \
    .withColumn('pickup_date', F.to_date(df.tpep_pickup_datetime)) \
    .filter("pickup_date = '2024-10-15'") \
    .count()
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

### Answer C -162

``` python
spark.sql("""
SELECT
    to_date(tpep_pickup_datetime) AS pickup_date,
    MAX((CAST(tpep_dropoff_datetime AS LONG) - CAST(tpep_pickup_datetime AS LONG)) / (60*60)) AS duration
FROM 
    taxi_data
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 10;
""").show()
```

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

### Answer C -4040

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

### Answer A 

``` python
spark.sql("""
SELECT
    a.PULocationID,
    b.Zone,
    count(1) rec_count
FROM 
    taxi_data a
    left join zone_table b on a.PULocationID = b.LocationID
GROUP BY
    1,2
ORDER BY
    3
LIMIT 10;
""").show()
```
