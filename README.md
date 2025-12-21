# NYC Taxi Trips - MS Fabric Project
![MSFabric](https://img.shields.io/badge/MS%20Fabric-Data%20Engineer-lightgreen)
![MSFabric](https://img.shields.io/badge/Dataflows%20Gen2-blue)
![MSFabric](https://img.shields.io/badge/Pipelines-grey)
![MSFabric](https://img.shields.io/badge/Lakehouse-blue)
![MSFabric](https://img.shields.io/badge/Stored%20Procedures-grey)
![MSFabric](https://img.shields.io/badge/Data%20Warehouse-blue)
![MSFabric](https://img.shields.io/badge/Semantic%20model-grey)
![MSFabric](https://img.shields.io/badge/PowerBI-yellow)

### <ins>Overview</ins>

In this project we will orchestrate the movement of NYC Taxi Trips raw data, starting from landing data into lakehouse to creating semantic models and visualizations using cleaned, transformed data from data warehouse presentation layer.

**Dataset is from nyc.gov and includes the following fields:** pickup and drop-off dates/times, pickup and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts (check data dictionary for more details).


### <ins>Architecture</ins>



<img width="1593" height="594" alt="NYC Taxi Data Architecture" src="https://github.com/user-attachments/assets/4a1d1a14-ee67-4066-ad6e-7ef8f468a9d7" />

---

### <ins>Project Details (step-by-step)</ins>

1)	Load NYC Taxi Trip records data and nyc_lookup_table data into Lakehouse which acts as storage layer for landing the raw data in the parquet format with each file containing 1 month of data.

### **i) <ins>Staging layer</ins>**

2)	Create a pipeline staging_lookup_table and use Data Factory copy data activity, configure up the source (Lakehouse) and sink (Warehouse) settings and ingest lookup table data into staging layer schema as table in Datawarehouse.

3)	Next, create a new pipeline staging_nyc_data, use copy data activity and configure source (Lakehouse), and sink (Warehouse) settings, load only Jan month parquet file and create a dynamic path, so create a variable for the date (V_file_date) using concate function in source and write the following pipeline expression:
   
```
@concat('yellow_tripdata_', variables('v_file_date'), '.parquet')
```


 now in sink configure and load the file into data warehouse as new table under staging schema(nyc_taxi_data).

4)	When you query for the max and min tpep_pickup_datetime, you notice results contain date from 2024, and Feb of 2025, but the query should only contain dates for Jan of 2025.
     ```
     select max(tpep_pickup_datetime), min (tpep_pickup_datetime) from staging.nyc_taxi_data
     ```

5)	To fix this we’ll create a stored procedure to filter the data and give start date and end date as parameters. 

```
Create procedure staging.nyc_date_cleaning
@enddate DATETIME2,
@startdate datetime2
AS
DELETE from staging.nyc_taxi_data WHERE tpep_pickup_datetime <@startdate or tpep_pickup_datetime > @enddate;
```


6)	Now go back to nyc_data staging pipeline and add a stored procedure to the pipeline upon on success and configure it. Click on import stored procedure parameters.
   
7)	Add set variables activity and create dynamic end date v_end_date using addToTime function: 

```@addToTime(concat(variables('v_file_date'),'-01'), 1,'month')```

   i) Set the end date in stored procedure as v_end_date

   ii) Set the start date in stored procedure as: @concat(variables('v_file_date'), '-01')

   iii) Now change the destination in copy activity to use existing table and run the pipeline.

8)	Next create a metadata schema and create table metadata.processing_log
   
9)	Create a stored procedure to insert into the table:

```
create procedure metadata.insert_staging_log1
@pipeline_run_id VARCHAR(255),
@table_name varchar(255),
@processed_datetime DATETIME2(5)
AS

  INSERT INTO metadata.processing_log(pipeline_run_id,
   table_processed,
   total_row_processed,
 latest_processed_pickup,
 processed_datetime)
 select 
 'placeholder' as pipeline_run_id,
'nyc_taxi_data' as table_processed,
count(*) as total_row_processed,
max(tpep_pickup_datetime) as latest_processed_pickup,
CURRENT_TIMESTAMP as processed_datetime
from staging.nyc_taxi_data;
```

10)	Create a stored procedure activity Sp_staging_metadata_log and configure the settings by importing stored procedure parameters.
    
11)	Now create a script (latest_processed_data) to auto update the start date variable in pipeline and write the following script to pick up the latest processed data from metadata.processing_log table.

```
select top 1
latest_processed_pickup
from metadata.processing_log
where table_processed = 'nyc_taxi_data'
order by latest_processed_pickup Desc;
```

12)	Create a set variable (v_file_date) activity to get the latest processed date and add 1 month to it and feed to the copy data pipeline to turn the entire pipeline activity to dynamic. Configure value in activity as: 

```
@formatdatetime(addTOTime(activity('latest_processed_data').output.resultsets[0].rows[0].latest_processed_pickup, 1, 'month'), 'yyyy-MM')
```


<img width="975" height="368" alt="image" src="https://github.com/user-attachments/assets/73386dd8-760a-452e-a9ac-87906a3ce1b0" />

 



13)	Next use Dataflow gen2 to perform cleansing and transformations on taxi zone lookup and taxi data tables from staging layer.
    

### **ii) <ins>Presentation Layer</ins>**
   	
    
15)	Create a dbo.pres_nyc_taxi_data table:

```
CREATE table dbo.pres_nyc_taxi_data
(
vendor VARCHAR(50),
tpep_pickup_datetime date,
tpep_dropoff_datetime date,
pu_borough VARCHAR(100),
pu_zone VARCHAR(50),
do_zone VARCHAR(50),
do_borough VARCHAR(100),
payment_method VARCHAR(50),
passenger_count int,
trip_distance float,
total_amount FLOAT
);
```

15)	Now create a dataflow pres_nyctaxi_processing_dfl2 -> add source-> warehouse, select nyc_taxi_data and perform cleaning and transformations on the table.
    
16)	Now we will combine both the tables data and load into Presentation layer in Datawarehouse which can be used for creating semantic models.



<img width="975" height="483" alt="image" src="https://github.com/user-attachments/assets/6a4fcb22-adb1-4bb4-9169-23431f7ae03c" />


 
17)	Create a Stored Procedure metadata.insert_pres_log using presentation table and this to dataflow activity:
```
CREATE procedure metadata.insert_pres_log
@pipeline_run_id VARCHAR(255),
@table_name varchar(255),
@processed_datetime DATETIME2(5)
AS

  INSERT INTO metadata.processing_log(pipeline_run_id,
   table_processed,
   total_row_processed,
 latest_processed_pickup,
 processed_datetime)
 select 
 'placeholder'as pipeline_run_id,
'pres_nyc_taxi_data' as table_processed,
count(*) as total_row_processed,
max(tpep_pickup_datetime) as latest_processed_pickup,
CURRENT_TIMESTAMP as processed_datetime
from dbo.pres_nyc_taxi_data;
```


<img width="975" height="478" alt="image" src="https://github.com/user-attachments/assets/7e1c6da5-f8fe-4c5f-88c4-35bc903454e4" />





18)	Create an orchestration pipeline to invoke both the staging pipeline and presentation pipeline to run one after the other.


<img width="975" height="481" alt="image" src="https://github.com/user-attachments/assets/04ca696b-0b73-470b-b881-a3e6db4becf9" />



### **iii) <ins>PowerBI</ins>**
   
19)	Expose the data from warehouse and build semantic model to create visualizations in Power BI.

<img width="975" height="485" alt="image" src="https://github.com/user-attachments/assets/4b4e798d-0246-4d1b-8a59-ec87882081a3" />


    
### **iv) <ins>Replace dataflow with stored procedure</ins>**


20)	Now replace the dataflow in (pres_nyc_dataprocessing_pl) pipeline with stored procedure to increase the efficiency.
    
21)	Create a Stored Procedure (dbo.orchestration_presentation) in the Data Warehouse using the following code:

```
CREATE PROCEDURE dbo.orchestration_presentation
AS
INSERT INTO dbo.pres_nyc_taxi_data
    SELECT
    CASE 
        WHEN nty.VendorID = 1 THEN 'Creative Mobile Technologies'
        WHEN nty.VendorID = 2 THEN 'VeriFone'
        else 'Unknown'
    end as vendor,
    format(nty.tpep_pickup_datetime,'yyyy-MM-dd') as tpep_pickup_datetime,
    format(nty.tpep_dropoff_datetime,'yyyy-MM-dd') as tpep_dropoff_datetime,
    lu1.Borough as pu_borough,
    lu1.Zone as pu_zone,
    lu2.Borough as pu_borough,
    lu2.Zone as pu_zone,
    CASE 
        WHEN nty.payment_type = 1 THEN 'Credit Card'
        WHEN nty.payment_type = 2 THEN 'Cash'
        WHEN nty.payment_type = 3 THEN 'No Charge'
        WHEN nty.payment_type = 4 THEN 'Dispute'
        WHEN nty.payment_type = 5 THEN 'Unknown'
        WHEN nty.payment_type = 6 THEN 'Voided Trip'
        else 'Unknown'
    end as payment_method,
    nty.passenger_count as passenger_count,
    nty.trip_distance as trip_distance,
    nty.total_amount as total_amount
    from staging.nyc_taxi_data nty
    left join staging.taxi_zone_lookup lu1
    on nty.PULocationID = lu1.LocationID
    left join staging.taxi_zone_lookup lu2
    on nty.DOLocationID = lu2.LocationID;

```

<img width="975" height="480" alt="image" src="https://github.com/user-attachments/assets/5f68c5f3-bc12-49e2-bf43-fa82bd9e9bfa" />



### **v) <ins>Monitoring</ins>**

22)	Go to monitor section to view the history, status and other details of pipeline.


<img width="975" height="383" alt="image" src="https://github.com/user-attachments/assets/52990c2a-a0b2-4e94-a36c-a68b5e6517b2" />


