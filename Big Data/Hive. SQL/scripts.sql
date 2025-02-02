create database taxi location 's3a://karpov-data/';

set hive.execution.engine=tez;

create external table taxi_data
(
vendor_id string,
tpep_pickup_datetime timestamp,
tpep_dropoff_datetime timestamp,
passenger_count int,
trip_distance double,
ratecode_id int,
store_and_fwd_flag string,
pulocation_id int,
dolocation_id int,
payment_type int,
fare_amount double,
extra double,
mta_tax double,
tip_amount double,
tolls_amount double,
improvement_surcharge double,
total_amount double,
congestion_surcharge double
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 's3a://karpov-data/2020'
TBLPROPERTIES ("skip.header.line.count"="1");


create external table taxi_data_part
(
vendor_id string,
tpep_pickup_datetime string,
tpep_dropoff_datetime string
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 's3a://karpov-data/2020';


alter table taxi_data_part set TBLPROPERTIES ("skip.header.line.count"="1");


use taxi;
-- 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc
create external table dim_vendor
(
    id int,
    name string
)
stored as orc;

insert into dim_vendor
select 1, 'Creative Mobile Technologies'
union all
select 2, 'VeriFone Inc';


/**
1= Standard rate
2=JFK
3=Newark
4=Nassau or Westchester
5=Negotiated fare
6=Group ride
*/
create external table rates
(
    id int,
    name string
)
stored as orc;


insert into rates
select 1, 'Standard rate'
union all
select 2, 'JFK'
union all
select 3, 'Newark'
union all
select 4, 'Nassau or Westchester'
union all
select 5, 'Negotiated fare'
union all
select 6, 'Group ride';

-- 1= Credit card
-- 2= Cash
-- 3= No charge
-- 4= Dispute
-- 5= Unknown
-- 6= Voided trip
create external table payment
(
    id int,
    name string
)
stored as orc;



with t as (select 1, 'Credit card'
union all
select 2, 'Cash'
union all
select 3, 'No charge'
union all
select 4, 'Dispute'
union all
select 5, 'Unknown'
union all
select 6, 'Voided trip')
insert into payment select * from t;



select p.name, sum(total_amount) as total_sum_amount
from taxi.trip t, taxi.payment p
where p.id = t.payment_type
and tpep_pickup_datetime = '2021-01-19'
group by p.name order by 2;



--
select count(1),
p.name
from taxi_data t join payment p on (t.payment_type = p.id)
group by p.name
order by 1 desc;



create external table trip_part
(
vendor_id string,
tpep_pickup_datetime timestamp,
tpep_dropoff_datetime timestamp,
passenger_count int,
trip_distance double,
ratecode_id int,
store_and_fwd_flag string,
pulocation_id int,
dolocation_id int,
payment_type int,
fare_amount double,
extra double,
mta_tax double,
tip_amount double,
tolls_amount double,
improvement_surcharge double,
total_amount double,
congestion_surcharge double
)
partitioned by (dt date)
stored as orc
location 's3a://karpov-data/warehouse/trips_part';


insert into taxi.trip partition(dt='2020-01-01')
select * from taxi_data t where to_date(t.tpep_pickup_datetime) = '2020-01-01';
