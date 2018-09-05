--vvijay26@gmail.com
--Drop the Nov and Dec table data first--Though this step is not required, adding to prevent testing issues later.
drop table if exists nyc_taxi_data_vv_nov;
drop table if exists nyc_taxi_data_vv_dec;
drop table if exists nyc_taxi_data_vv_combined;

--Create Table with Nov 2017 data
create external table if not exists nyc_taxi_data_vv_nov(VendorID string,tpep_pickup_datetime  timestamp, tpep_dropoff_datetime timestamp, 
passenger_count int, trip_distance double, RatecodeID int, store_and_fwd_flag string, PULocationID int, 
DOLocationID int, payment_type int, fare_amount double, extra double, mta_tax double,tip_amount double, 
tolls_amount double, improvement_surcharge double, total_amount double)
row format delimited 
fields terminated BY ','
location '/common_folder/nyc_taxi_data/nov/'
--Skip first 2 rows (header and the null row after header)
tblproperties ("skip.header.line.count"="2"); 

--Create Table with Dec 2017 data
create external table if not exists nyc_taxi_data_vv_dec(VendorID string,tpep_pickup_datetime  timestamp, tpep_dropoff_datetime timestamp, 
passenger_count int, trip_distance double, RatecodeID int, store_and_fwd_flag string, PULocationID int, 
DOLocationID int, payment_type int, fare_amount double, extra double, mta_tax double,tip_amount double, 
tolls_amount double, improvement_surcharge double, total_amount double)
row format delimited 
fields terminated BY ','
location '/common_folder/nyc_taxi_data/dec/'
--Skip first 2 rows (header and the null row after header)
tblproperties ("skip.header.line.count"="2");


--Validate 5 records to check if data is loaded properly in Nov table
select * from nyc_taxi_data_vv_nov limit 5;
--Validate 5 records to check if data is loaded properly in Dec table
select * from nyc_taxi_data_vv_dec limit 5;

-- Create Merged internal table for Nov and Dec for runnig combined queries.

create table if not exists nyc_taxi_Data_vv_combined as 
select * from (select * from nyc_taxi_data_vv_nov
               union all
               select * from nyc_taxi_data_vv_dec) tmp;

--ASSIGNMENT--
--Basic Data Quality Checks --
--1.	How many records has each TPEP provider provided? Write a query that summarises the number of records of each provider.
select vendorid,count(*)
from nyc_taxi_data_vv_combined
group by vendorid;
--ANSWER
--1 -Creative Mobile Technologies, LLC; => 8447149
--2= VeriFone Inc. => 10345930

----------------------------------------
--2.	The data provided is for months November and December only. Check whether the data is consistent, and if not, identify the data quality issues. Mention all data quality issues in comments.
-- We have already identified that apart from the header row, the next row is NULL's and we have removed that while loading into HDFS.
-- Lets first identify the issues in Nov dataset -

-- Lets first see if the pickup times are in November ONLY
select distinct to_date(tpep_pickup_datetime) as Pickup
from nyc_taxi_data_vv_nov
order by Pickup;
-- From the above output, we can see pickup dates not in Nov 2017 - 2001-01-01, 2008-12-31, 2009-01-01, 2017-12-01, 2041-11-15 (Junk date)
-- Hence, for analysis with only NOV data we will need to add a where criteria with pickup dates between 2017-11-01 and 2017-11-30
-- Lets see how many records are outside of Nov-2017
select to_date(tpep_pickup_datetime), count(vendorid)
from nyc_taxi_data_vv_nov
where to_date(tpep_pickup_datetime) not between '2017-11-01' and '2017-11-30'
group by to_date(tpep_pickup_datetime);
-- So, we can see about 300 records have erroneous date (We are assuming Pick up date of 31-Oct is also erroneous - this can change as per Business case.

-- Lets see which other columns have incorrect or Erroneous data (Assumptions can be seen in where criteria, such as passengers >15 
-- ratecodeid not in defined values etc. is considered as erroneous) - 

select *
from nyc_taxi_data_vv_nov 
where vendorid is null 
   or tpep_pickup_datetime is null
   or tpep_dropoff_datetime is null -- no Data falling into this category
   
select *
from nyc_taxi_data_vv_nov 
where passenger_count > 15; -- 1 Record falls into this category - seems erroneous - 192 passengers.

select *
from nyc_taxi_data_vv_nov 
where ratecodeid not between 1 and 6; -- 89 Records fall into this category - Seems erroneous as only values allowed are 1 thru 6

select *
from nyc_taxi_data_vv_nov 
where store_and_fwd_flag not in ('Y','N')
   or pulocationid is null
   or dolocationid is null
   or payment_type not between 1 and 6; -- No records fall into this category

select count(*)
from nyc_taxi_data_vv_nov 
where fare_amount < 0
   or total_amount < 0; -- Most erroneous Data seems to be falling into this category, where amount is negative - 4408 Records.
   
-- Lets do similar analysis for Dec also.
select distinct to_date(tpep_pickup_datetime) as Pickup
from nyc_taxi_data_vv_dec
order by Pickup;
-- From the above output, we can see pickup dates not in Dec 2017 - 2003, 2008 and 2018 date etc.
-- Hence, for analysis with only DEC data we will need to add a where criteria with pickup dates between 2017-12-01 and 2017-12-31
-- Lets see how many records are outside of Dec-2017
select to_date(tpep_pickup_datetime), count(vendorid)
from nyc_taxi_data_vv_dec
where to_date(tpep_pickup_datetime) not between '2017-12-01' and '2017-12-31'
group by to_date(tpep_pickup_datetime);
-- So, we can see about 310+ records have erroneous date (We are assuming Pick up date of 30-Nov is also erroneous - this can change as per Business case.

-- Lets see which other columns have incorrect or Erroneous data (Assumptions can be seen in where criteria, such as passengers >15 
-- ratecodeid not in defined values etc. is considered as erroneous) - 

select *
from nyc_taxi_data_vv_dec 
where vendorid is null 
   or tpep_pickup_datetime is null
   or tpep_dropoff_datetime is null -- no Data falling into this category
   
select *
from nyc_taxi_data_vv_dec 
where passenger_count > 15; -- no Data falling into this category

select *
from nyc_taxi_data_vv_dec
where ratecodeid not between 1 and 6; -- 89 Records fall into this category - Seems erroneous as only values allowed are 1 thru 6

select *
from nyc_taxi_data_vv_dec 
where store_and_fwd_flag not in ('Y','N')
   or pulocationid is null
   or dolocationid is null
   or payment_type not between 1 and 6; -- No records fall into this category

select count(*)
from nyc_taxi_data_vv_dec 
where fare_amount < 0
   or total_amount < 0; -- Most erroneous Data seems to be falling into this category, where amount is negative - 4964 Records.

--3.	You might have encountered unusual or erroneous rows in the dataset. Can you conclude which vendor is doing a bad job in providing the records?

--Lets run a combined query once each on the Nov and Dec datasets and identify the Vendor who is doing a bad job 
-- We are NOT running these on the combined table as we want to identify issues with pickup dates also (if we merge, a nov data in dec
-- file will show up as correct

select vendorid, count(vendorid)
from nyc_taxi_data_vv_nov
where to_date(tpep_pickup_datetime) not between '2017-11-01' and '2017-11-30'
   or vendorid is null 
   or tpep_pickup_datetime is null
   or tpep_dropoff_datetime is null
   or passenger_count > 15
   or ratecodeid not between 1 and 6
   or store_and_fwd_flag not in ('Y','N')
   or pulocationid is null
   or dolocationid is null
   or payment_type not between 1 and 6
   or fare_amount < 0
   or total_amount < 0
group by vendorid; --- As can be seen from the output of this query, Vendor 1 (83 erroneous) is doing far better job than Vendor 2 (4735 erroneous)

--Similarly for Dec dataset - 

select vendorid, count(vendorid)
from nyc_taxi_data_vv_dec
where to_date(tpep_pickup_datetime) not between '2017-12-01' and '2017-12-31'
   or vendorid is null 
   or tpep_pickup_datetime is null
   or tpep_dropoff_datetime is null
   or passenger_count > 15
   or ratecodeid not between 1 and 6
   or store_and_fwd_flag not in ('Y','N')
   or pulocationid is null
   or dolocationid is null
   or payment_type not between 1 and 6
   or fare_amount < 0
   or total_amount < 0
group by vendorid; --- As can be seen from the output of this query, Vendor 1 (89 erroneous) is doing far better job than Vendor 2 (5275 erroneous)

--ANSWER for 3 - Vendor 1 (<200 erroneous for both Dec and Nov combined) is doing far better job than Vendor 2 (> 10K errors for Nov and Dec)

/* Analysis - I */

/* PLEASE NOTE - We are not going to ignore the bad records completely (case by case basis) in the below queries - 
since its very minimal as coompared to the total dataset size

/* 1.	Compare the overall average fare for November and December. */
select avg(fare_amount) from 
nyc_taxi_data_vv_nov where
fare_amount >=0 ; /* 13.15$ , we have assumed negative values as outliers and not considered for average */ 

select avg(fare_amount) from 
nyc_taxi_data_vv_dec where
fare_amount >=0 ; /* 12.96$,  we have assumed negative values as outliers and not considered for average */

/* 2.	Explore the ‘number of passengers per trip’ - how many trips are made by each level of ‘Passenger_count’? Do most people travel solo or with other people? */

select passenger_count, count(vendorid) as NbrofPassengers
from nyc_taxi_data_vv_combined
group by passenger_count
sort by NbrofPassengers desc; /*  Most people prefer SOLO rides (13251273 vs. next best of 2 riders count of 2816147 */

-- ANSWER 2 - Overall (for both Nov and Dec, SOLO rides are most common by far - 75+%)

--3.	Which is the most preferred mode of payment?

select payment_type, count(vendorid)
from nyc_taxi_Data_vv_combined
group by payment_type; /* Payment type 1 (12638715) - Credit Card is Most popular (Also, only payment type 1,2,3,4 are present, 5/6 not available (which is fine) */

--4.	What is the average tip paid? Compare the average tip with the 25th, 50th and 75th percentiles and comment 
-- whether the ‘average tip’ is a representative statistic (of the central tendency) of ‘tip amount paid’.

select vendorid, avg(tip_amount) as Average ,
percentile_approx(tip_amount,0.25) as 25th_Percentile,
percentile_approx(tip_amount,0.5) as 50th_Percentile,
percentile_approx(tip_amount,0.75) as 75th_Percentile,
percentile_approx(tip_amount,0.61) as 61st_Percentile
from nyc_taxi_Data_vv_combined
group by vendorid; 
/* ANSWER 4- 
The Average tip is around 1.8 for Vendor 1 and 1.88 for Vendor 2, 
IT does not look at the average tip is the representative statistic (as average is quite more than the 50th Percentile value
The 60-61st percentile looks more of a representative charecteristic of the whole group
*/

--5. Explore the ‘Extra’ (charge) variable - what fraction of total trips have an extra charge is levied?

select vendorid,extra,count(vendorid) Number_of_trips
from nyc_taxi_Data_vv_combined
group by vendorid,extra
sort by vendorid asc,Number_of_trips desc;

/* ANSWER 5 - As can be seen from the output of the above query,
For Vendor 1 - 4.5 million trips have no extra charge and 2.59 million have 0.5$ extra charge, next highest entries are 1$ (1.27 million)
and 4.5$ - 29K records, then not significant
For Vendor 2 - 5.6 million trips have no extra charge and 3.2 million have 0.5$ extra charge, next highest entries are 1$ (1.5 million)
and 4.5$ - 42K records, then not significant

One observation we can make is that during peak hours, the usual extra charges primarily vary between these 3 numbers - 0.5, 1 and 4.5$
*/

/* Analysis - II */

/*1.	What is the correlation between the number of passengers and tip paid? Do multiple travellers pay more compared to solo travellers?*/

select passenger_count, count(vendorid), avg(tip_amount) as Average 
from nyc_taxi_Data_vv_combined
group by passenger_count
sort by passenger_count; 

-- ANSWER 1 - From the output of the above query, we can see that there is no real trend (at least when passenger_count is 6 or below
-- The average tip varies from 1.85 for 1 passenger to 1.77 for 3 passengers and again increases to 1.87 for 5 passengers.
-- One thing we can observe is the average tip paid for 7,8,9 travellers is 4.6$, 8.5$ and 6.2$, which is substantially more than 
-- what it is for 6 passengers or below. So, we can say that average tip is usually way higher, when the passenger count is 7 or above(till 9).

/*2.	Create five buckets of ‘tip paid’: [0-5), [5-10), [10-15) , [15-20) and >=20. 
Calculate the percentage share of each bucket (i.e. the fraction of trips falling in each bucket).*/

/* There are multiple ways to do this, in this case, i am using the option of adding a tip paid bin as a separate column in the new internal 
combined table and then updating it based on tip range (this is done to practice/show some insert-overwrite also.*/

drop table if exists nyc_taxi_Data_vv_combined_tip_paid; /* temp table to be created later */

create table if not exists nyc_taxi_Data_vv_combined_tip_paid as 
select * from (select vendorid,tip_amount,CASE  
                        WHEN tip_amount between 0 and 5 THEN '0-5' 
                        WHEN tip_amount between 5 and 10 THEN '5-10' 
                        WHEN tip_amount between 10 and 15 THEN '10-15'
                        WHEN tip_amount between 15 and 20 THEN '15-20' 
                        ELSE '>20'
                    END as tip_paid from nyc_taxi_data_vv_combined) tmp;

select * from nyc_taxi_Data_vv_combined_tip_paid limit 50; -- Looks good.

-- Now next lets see the count of each bucket

SELECT tip_paid, count(*) as NbrOfEntries, count(*) * 100.0/ sum(count(*)) OVER () AS percent_share
FROM   nyc_taxi_Data_vv_combined_tip_paid
GROUP  BY tip_paid
ORDER  BY NbrOfEntries desc;

/* ANSWER 2 - 92 percent belongs to bucket 0-5, next best is 5-10 with ~5% and 10-15 with ~1.67% */

/*3.	Which month has a greater average ‘speed’ - November or December? Note that the variable ‘speed’ will have to be derived from other metrics.*/

/* We subtract the unix timestamp and divide by 3600 to get elapsed time in hours. If elapsed time is 
   less than 0, we default it to Then divide the 
   trip_distance by the elapsed time in hours to determine the miles per hour (mph) */

/* WE RUN these queries on individual data sets - NOV and DEC separately */

select avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600)) as Average_Speed_in_mph
from nyc_taxi_Data_vv_nov; /* Average is around 12.758 mph */

select avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600)) as Average_Speed_in_mph
from nyc_taxi_Data_vv_dec; /* Average is around 12.70 mph */

-- ANSWER 3 - Nov has the higher average speed.

/*4.	Analyse the average speed of the most happening days of the year i.e. 31st December (New year’s eve) and 
25th December (Christmas Eve) and compare it with the overall average.*/

-- Overall average -

select avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600)) as Average_Speed_in_mph
from nyc_taxi_Data_vv_combined; /* 12.73 mph */

-- Speed on special days 

select to_date(tpep_pickup_datetime) as SpecialDay, avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600)) as Average_Speed_in_mph
from nyc_taxi_Data_vv_dec 
where to_date(tpep_pickup_datetime) in ('2017-12-25','2017-12-31')
group by to_date(tpep_pickup_datetime);

--ANSWER 4 - Average speed is 16.76 on 25-Dec and 14.04 on 31 Dec. Much higher than overall average of 12.73

