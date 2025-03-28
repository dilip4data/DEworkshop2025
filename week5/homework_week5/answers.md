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

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)

### Answer:  3.5.1

```
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .getOrCreate()

pyspark.__version__

'3.5.1'
```

## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

### Answer:  25MB

```
-rw-r--r-- 1 root root 25397408 Mar 10 07:10 part-00002-7ee647aa-0096-4a08-a1d4-81bbe01107f8-c000.snappy.parquet
-rw-r--r-- 1 root root 25414796 Mar 10 07:10 part-00001-7ee647aa-0096-4a08-a1d4-81bbe01107f8-c000.snappy.parquet
-rw-r--r-- 1 root root 25416060 Mar 10 07:10 part-00003-7ee647aa-0096-4a08-a1d4-81bbe01107f8-c000.snappy.parquet
-rw-r--r-- 1 root root 25395947 Mar 10 07:10 part-00000-7ee647aa-0096-4a08-a1d4-81bbe01107f8-c000.snappy.parquet
-rw-r--r-- 1 root root        0 Mar 10 07:10 _SUCCESS

```

## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567

### Answer:  125,567

```
spark.sql("""
        SELECT COUNT(1) as Oct_15th_trips 
            FROM YELLOW_TRIPDATA  
            WHERE date_part('MONTH', pickup_datetime) = 10 and 
                  date_part('DAY', pickup_datetime) = 15 
        """).show()
```


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

### Answer:  162

```
df_yellow \
    .withColumn('duration_hours',
                (F.unix_timestamp(df_yellow.dropoff_datetime) - F.unix_timestamp(df_yellow.pickup_datetime))/ F.lit(3600)) \
    .withColumn('pickup_datetime',df_yellow.pickup_datetime) \
    .groupBy('pickup_datetime') \
    .agg(F.max('duration_hours').alias('max_duration_hours')) \
    .orderBy('max_duration_hours',ascending=False) \
    .limit(5) \
    .show()
```

## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

### Answer:  4040

`http://127.0.0.1:4040/jobs/`

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

### Answer:  Governor's Island/Ellis Island/Liberty Island

```
df_zone = spark.read \
        .option("header", "true") \
        .csv("taxi_zone_lookup.csv")

df_zone.registerTempTable("taxi_zone")

spark.sql("""
        SELECT t.zone, count(1) FROM YELLOW_TRIPDATA yt join taxi_zone t on yt.PULocationID = t.LocationID
        group by t.zone
        order by 2 asc
        """).show()

```
##   Link to Jupyter Notebook with detailed steps of homework

https://github.com/dilip4data/DEworkshop2025/blob/main/week5/homework_week5/Week5_HomeWork_10MAR2025.ipynb


