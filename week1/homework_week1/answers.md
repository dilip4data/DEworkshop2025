# Module 1 Homework: Docker & SQL

### Question 1. Understanding docker first run

Run docker with the python:3.12.8 image in an interactive mode, use the entrypoint bash.
What's the version of pip in the image?

``` $
    (new_environment2024)
    dilip@Dilipkumar MINGW64 ~
    $ docker run -it python:3.12.8 bash
    Unable to find image 'python:3.12.8' locally
    3.12.8: Pulling from library/python
    Digest: sha256:5893362478144406ee0771bd9c38081a185077fb317ba71d01b7567678a89708
    Status: Downloaded newer image for python:3.12.8
    root@a9e7e9378f2b:/# pip version
    ERROR: unknown command "version"
    root@a9e7e9378f2b:/# pip --version
    pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
    root@a9e7e9378f2b:/#
```        
### Answer : 24.3.1

### Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- db:5432

If there are more than one answers, select only one of them


**Code**

```
$ docker-compose up -d
 db Pulled
 Network week1_default  Creating
 Network week1_default  Created
 Volume "vol-pgdata"  Creating
 Volume "vol-pgdata"  Created
 Volume "vol-pgadmin_data"  Creating
 Volume "vol-pgadmin_data"  Created
 Container pgadmin  Creating
 Container postgres  Creating
 Container pgadmin  Created
 Container postgres  Created
 Container pgadmin  Starting
 Container postgres  Starting
 Container pgadmin  Started
 Container postgres  Started

$ docker ps -a
CONTAINER ID   IMAGE                   COMMAND                  CREATED             STATUS                         PORTS                           NAMES
68e61569291c   dpage/pgadmin4:latest   "/entrypoint.sh"         14 seconds ago      Up 13 seconds                  443/tcp, 0.0.0.0:8080->80/tcp   pgadmin
9164926ad91a   postgres:17-alpine      "docker-entrypoint.s…"   14 seconds ago      Up 13 seconds                  0.0.0.0:5433->5432/tcp          postgres


```

<img src="https://github.com/user-attachments/assets/7cf97061-dfc8-412b-9714-9698b5edc2c8" width="350" />
&emsp; &emsp;
<img src="https://github.com/user-attachments/assets/9b9a2501-fde1-4e22-ae16-01dcd0b1352b" width="350" />


### Answer : 
          - postgres:5432
          - db:5432


##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.


## Docker Run output :-

```
docker build -t taxi_ingest:v001 .


$ docker run --env-file .my_env -it  --network=week1_default    taxi_ingest:v001  --user=$USER  --password=$PASS  --host=$HOST  --port=$PORT  --db=$DB  --tb $TABLE_NAME1 $TABLE_NAME2  --url $URL1 $URL2

Downloading parquet datafile green_tripdata_2019-10.parquet ...

  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 7680k  100 7680k    0     0  25.8M      0 --:--:-- --:--:-- --:--:-- 25.8M


Downloading taxi zone CSV file taxi_zone_lookup.csv ...

  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 12331  100 12331    0     0   123k      0 --:--:-- --:--:-- --:--:--  122k


CSV yay!


Parquet oh yeahh!


Inserting batch 1....
Inserted! time taken     17.693 seconds.

Inserting batch 2....
Inserted! time taken     12.161 seconds.

Inserting batch 3....
Inserted! time taken     11.702 seconds.

Inserting batch 4....
Inserted! time taken     10.996 seconds.

Inserting batch 5....
Inserted! time taken      7.477 seconds.

Completed! Total time taken was     60.099 seconds for 5 batches.

```


## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles

```

SELECT count(*) FROM public.green_taxi_data where lpep_pickup_datetime::date >= '2019-10-01'
and lpep_dropoff_datetime::date < '2019-11-01'
and trip_distance <=1;

-- 104,802

SELECT count(*) FROM public.green_taxi_data where lpep_pickup_datetime::date >= '2019-10-01'
and lpep_dropoff_datetime::date < '2019-11-01'
and trip_distance > 1 and trip_distance <=3;

-- 198,924

SELECT count(*) FROM public.green_taxi_data where lpep_pickup_datetime::date >= '2019-10-01'
and lpep_dropoff_datetime::date < '2019-11-01'
and trip_distance > 3 and trip_distance <=7;

-- 109,603

SELECT count(*) FROM public.green_taxi_data where lpep_pickup_datetime::date >= '2019-10-01'
and lpep_dropoff_datetime::date < '2019-11-01'
and trip_distance > 7 and trip_distance <=10;

-- 27,678

SELECT count(*) FROM public.green_taxi_data where lpep_pickup_datetime::date >= '2019-10-01'
and lpep_dropoff_datetime::date < '2019-11-01'
and trip_distance > 10;

-- 35,189

```

### Answer : 

- 104,802;  198,924;  109,603;  27,678;  35,189

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

```
SELECT lpep_pickup_datetime::date, max(trip_distance) FROM public.green_taxi_data 
group by lpep_pickup_datetime::date
order by 2 desc;

```

### Answer : 2019-10-31


## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.

```

SELECT "Zone", sum(total_amount)  FROM green_taxi_data g join taxi_zone t
on g."PULocationID" = t."LocationID"
where lpep_pickup_datetime::date = '2019-10-18'
group by "Zone"
having sum(total_amount) > 13000


```
<img src="https://github.com/user-attachments/assets/871f5883-7624-4afe-8375-b9051d45b32b" width="350" />
&emsp; 

### Answer : East Harlem North, East Harlem South, Morningside Heights


## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
named "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

```
select "Zone" as largest_tip_dropoff_zone from taxi_zone
where "LocationID" = (select "DOLocationID" from (
SELECT "DOLocationID",max(tip_amount) FROM green_taxi_data g join taxi_zone t
on g."PULocationID" = t."LocationID" and "Zone" = 'East Harlem North'
where extract (year from lpep_pickup_datetime::date) = 2019
and extract (month from lpep_pickup_datetime::date) = 10
group by "DOLocationID"
order by 2 desc
limit 1) foo )
```
<img src="https://github.com/user-attachments/assets/fadd1135-4de7-4312-a097-c2330bc4ed9b" width="350" />
&emsp; 

### Answer : JFK Airport


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

### Answer :
- teraform init, terraform plan -auto-apply, terraform rm

