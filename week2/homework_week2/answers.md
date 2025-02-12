## Module 2 Homework

### Assignment

So far in the course, we processed data for the year 2019 and 2020. Your task is to extend the existing flows to include data for the year 2021.

![homework datasets](../../../02-workflow-orchestration/images/homework.png)

As a hint, Kestra makes that process really easy:
1. You can leverage the backfill functionality in the [scheduled flow](../../../02-workflow-orchestration/flows/07_gcp_taxi_scheduled.yaml) to backfill the data for the year 2021. Just make sure to select the time period for which data exists i.e. from `2021-01-01` to `2021-07-31`. Also, make sure to do the same for both `yellow` and `green` taxi data (select the right service in the `taxi` input).
2. Alternatively, run the flow manually for each of the seven months of 2021 for both `yellow` and `green` taxi data. Challenge for you: find out how to loop over the combination of Year-Month and `taxi`-type using `ForEach` task which triggers the flow for each combination using a `Subflow` task.

### Source code for Challenge question.

```

id: dilip_own_ForEach
namespace: zoomcamp

inputs:
  - id: taxi
    type: ARRAY
    itemType: STRING
    defaults: [yellow, green]

  - id: year_month
    type: ARRAY
    itemType: STRING
    defaults: ["2021-01", "2021-02", "2021-03", "2021-04", "2021-05", "2021-06", "2021-07", "2021-08", "2021-09", "2021-10", "2021-11", "2021-12"]

tasks:
  - id: t_type
    type: io.kestra.plugin.core.flow.ForEach
    values: "{{inputs.taxi}}"
    tasks:
      - id: t_ym
        type: io.kestra.plugin.core.flow.ForEach
        values: "{{inputs.year_month}}"
        tasks:
          - id: if_green
            type: io.kestra.plugin.core.flow.If
            condition: "{{ parents[0].taskrun.value == 'green'}}"
            then:
              - id: print_green
                type: io.kestra.plugin.core.debug.Return
                format: "{{ parents[1].taskrun.value }}_tripdata_{{parents[0].taskrun.value}}.csv"
          - id: if_yellow
            type: io.kestra.plugin.core.flow.If
            condition: "{{ parents[0].taskrun.value == 'yellow'}}"
            then:
              - id: print_yellow
                type: io.kestra.plugin.core.debug.Return
                format: "{{ parents[1].taskrun.value }}_tripdata_{{parents[0].taskrun.value}}.csv"


```

### Quiz Questions

Complete the Quiz shown below. It’s a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra and ETL pipelines for data lakes and warehouses.

1) Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
- 128.3 MB
- 134.5 MB
- 364.7 MB
- 692.6 MB

  <img src="https://github.com/user-attachments/assets/2f582047-11af-441a-9a4a-5ae52e4001ab" width="300" />
       
### Answer : 128.3 MB

2) What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?
- `{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv` 
- `green_tripdata_2020-04.csv`
- `green_tripdata_04_2020.csv`
- `green_tripdata_2020.csv`
  
### Answer :

   ```
   "{{render(vars.file)}}"  =>  green_tripdata_2020-04.csv 
   ```
  
3) How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?
- 13,537.299
- 24,648,499
- 18,324,219
- 29,430,127

  ```
  select count(*) from public.yellow_tripdata;
  ```
  
### Answer : 24,648,499

4) How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?
- 5,327,301
- 936,199
- 1,734,051
- 1,342,034
  
```
select count(*) from public.green_tripdata;
```
  
### Answer : 1,734,051

5) How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?
- 1,428,092
- 706,911
- 1,925,152
- 2,561,031

```
select count(*) from public.yellow_tripdata where filename LIKE '%2021-03%';
```
### Answer : 1,925,152

6) How would you configure the timezone to New York in a Schedule trigger?
- Add a `timezone` property set to `EST` in the `Schedule` trigger configuration  
- Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration
- Add a `timezone` property set to `UTC-5` in the `Schedule` trigger configuration
- Add a `location` property set to `New_York` in the `Schedule` trigger configuration  

### Answer  : Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration


