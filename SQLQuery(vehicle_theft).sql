create database Vehicle_theft_db
use Vehicle_theft_db

-- Bulk insert is to import the data from localdb into the RDBms by writing the query
--stolen vehicle table

create table st_veh
(vehicle_id varchar(max),vehicle_type varchar(max),make_id varchar(max),model_year varchar(max),vehicle_desc varchar(max),
color varchar(max),date_stolen varchar(max),location_id varchar(max))

--make details table

create table make_d
(make_id varchar(max),make_name varchar(max),make_type varchar(max))

--locations table

create table locations
(location_id varchar(max),	region varchar(max), country varchar(max),	
population varchar(max),density varchar(max))

--import the data

bulk insert locations
from 'C:\Users\Neha Singh\Downloads\locations.csv'
with (fieldterminator=',' , rowterminator='\n' , firstrow=2, maxerrors = 20)

select * from locations


bulk insert make_d
from 'C:\Users\Neha Singh\Downloads\make_details.csv'
with (fieldterminator=',' , rowterminator='\n' , firstrow=2, maxerrors = 20)

select * from make_d

bulk insert st_veh
from 'C:\Users\Neha Singh\Downloads\stolen_vehicles.csv'
with (fieldterminator=',' , rowterminator='\n' , firstrow=2, maxerrors = 20)

select * from st_veh

alter table st_veh
alter column date_stolen date

select date_stolen, try_convert(date, date_stolen) as formatted_date from st_veh
where try_convert(date, date_stolen) is null

update st_veh set date_stolen = case when date_stolen = '2021/15/10'
then '2021-10-15'
when date_stolen = '20200-02-13' then '2022-02-13'
else date_stolen end

--update the date_stolen column data type into date

update st_veh set date_stolen = convert(date, date_stolen)

alter table st_veh
alter column date_stolen date

select * from locations

alter table locations
alter column population int

alter table locations
alter column density decimal(5,2)

select count(*) from st_veh
select distinct count(*) from st_veh

--there are no duplicates

select count(*) from make_d
select distinct count(*) from make_d

select count(*) from locations
select distinct count(*) from locations

--will check the data distribution

select distinct vehicle_type from st_veh

select vehicle_type,color, count(vehicle_id) as 'no_of_vehicle_stolen'
from st_veh
group by vehicle_type,color
order by no_of_vehicle_stolen desc

-- in the vehicle type we have 1 null category, we convert it into the unknown type
update st_veh set vehicle_type = 'unknown_type'
where vehicle_type is null

select distinct vehicle_type,vehicle_desc from st_veh
where vehicle_desc is null

-- we will change the null or undefined veh_desc to not provided

update st_veh set vehicle_desc = 'not-provided'
where vehicle_desc is null

select distinct vehicle_type,vehicle_desc from st_veh
where vehicle_desc = 'not-provided'

select distinct vehicle_type, vehicle_desc from st_veh
where vehicle_type = 'boat trailer'

select distinct model_year from st_veh

--min and max model year
select min(model_year) as 'oldest_model_year',
max(model_year) as 'latest_model_year'
from st_veh

--will create the model_year group

--group vintage_model from 1940 to 1960
--classic_model from 1961 to 1990
--oldest model from 1991 to 2018
--latest_model above 2018

select *, case when model_year between 1940 and 1960
then 'vintage_model'
when model_year between 1961 and 1990
then 'classic_model'
when model_year between 1991 and 2018
then 'oldest_model'
when model_year > 2018
then 'latest_model'
else 'unknown' end as 'Model_group'
from st_veh

alter table st_veh
add Model_group varchar(40)

/*update st_veh set model_year = case when model_year between 1940 and 1960
then 'vintage_model'
when model_year between 1961 and 1990
then 'classic_model'
when model_year between 1991 and 2018
then 'oldest_model'
when model_year > 2018
then 'latest_model'
else 'unknown' end
*/
select * from st_veh

alter table st_veh
drop column model_year

create table st_veh1
(vehicle_id varchar(max),vehicle_type varchar(max),make_id varchar(max),model_year varchar(max),vehicle_desc varchar(max),
color varchar(max),date_stolen varchar(max),location_id varchar(max))

bulk insert st_veh1
from 'C:\Users\Neha Singh\Downloads\stolen_vehicles.csv'
with (fieldterminator=',' , rowterminator='\n' , firstrow=2, maxerrors = 20)

alter table st_veh
add model_year varchar(max)

update st_veh set model_year = st_veh1.model_year 
from st_veh
join st_veh1 on st_veh.vehicle_id = st_veh1.vehicle_id

select * from st_veh

-- now we can drop the temp table st_veh1

update st_veh set model_group = case when model_year between 1940 and 1960
then 'vintage_model(1940-1960)'
when model_year between 1961 and 1990
then 'classic_model(1961-1990)'
when model_year between 1991 and 2018
then 'oldest_model(1991-2018)'
when model_year > 2018
then 'latest_model(>2018)'
else 'unknown' end

select * from st_veh

select model_group, vehicle_type, vehicle_desc, count(vehicle_id) as 'total_st_veh_count' from st_veh 
group by model_group, vehicle_type, vehicle_desc
order by total_st_veh_count desc

-- the police department wants to create a stolen vehicle profile

with st_veh_profile as (select st.vehicle_id, m.make_id, l.location_id, vehicle_type, vehicle_desc,
color, model_group, model_year, make_name, make_type, region, 
population, density from st_veh st
join locations l
on st.location_id = l.location_id
join make_d m
on st.make_id = m.make_id)

select region, count(vehicle_id) as 'stolen_count',
count(distinct make_name) as 'make',
count(distinct color) as 'color',
avg(cast(population as float)) as 'avg_pop',
avg(density) as 'avg_density'
from st_veh_profile
group by region
order by avg_pop desc

-- monthly theft trend analysis
with monthly_theft as (select l.location_id, region, datename(month, date_stolen) as 'theft_month'
,count(vehicle_id) as 'no_of_veh_stolen'
from st_veh st
join locations l
on st.location_id = l.location_id
group by l.location_id, region,
datename(month, date_stolen))

,theft_pr as (select mt.location_id, mt.region, mt.theft_month, mt.no_of_veh_stolen
as 'Monthly_count',
lead(mt.no_of_veh_stolen) over (partition by mt.location_id order by mt.theft_month) as
'next_month_theft' from monthly_theft mt)

select * from theft_pr
-- we need to include only those months where trend of stolen is increased
where monthly_count < next_month_theft

-- we can also anlyse the long term trend by using 3 month rolling avg of stolen vehicles

with yearly_theft as (select l.location_id, region, year(date_stolen) as 'the_year', month(date_stolen) as 'the_month'
,count(vehicle_id) as 'no_of_veh_stolen'
from st_veh st
join locations l
on st.location_id = l.location_id
group by l.location_id, region, year(date_stolen),
month(date_stolen))

,theft_trend as (select yt.location_id, yt.region, yt.the_year, yt.the_month, yt.no_of_veh_stolen
as 'Monthly_count',
avg(yt.no_of_veh_stolen) over (partition by yt.location_id, yt.the_year order by yt.the_month
rows between 2 preceding and current row) as '3_month_rolling_avg'
from yearly_theft yt)

select * from theft_trend
order by region, the_year, the_month

--will calculate the probability for each make within a vehicle type

select  make_type, count(vehicle_id) as 'total_st_veh' from make_d m
join st_veh st
on m.make_id = st.make_id
group by make_type
--keeping the count more than 20
with vehicle_count as (select vehicle_type, make_name, m.make_id, m.make_type, count(vehicle_id) as 'theft_count'
from st_veh st
join
make_d m
on st.make_id = m.make_id
group by vehicle_type, m.make_id, make_name, make_type
having count(vehicle_id)>20)

select make_name, vehicle_type, theft_count, make_type,
(theft_count*1.0/sum(theft_count) over(partition by vehicle_type))*100 as 'theft_prob'
from vehicle_count
order by theft_prob desc

--correlation with population and density

select l.region, population, density, count(vehicle_id) as 'theft_count'
,count(vehicle_id)*1.0/density as 'theft_per_density'
from st_veh st
join locations l
on st.location_id = l.location_id
group by region, population, density
order by theft_per_density desc, density desc

--density and theft per density is least correlated

--checking the particular day of week maximum theft is happening

with weekly_ranking as (select datename(weekday , date_stolen) as 'day_name',
count(vehicle_id) as 'veh_stolen_count',
rank() over (order by count(vehicle_id) desc) as 'top_rank',
rank() over (order by count(vehicle_id)) as 'bottom_rank'
from st_veh
group by datename(weekday, date_stolen))
--keeping top 3 and bottom 3
select day_name, veh_stolen_count,
case when top_rank <= 3 then 'top' + cast(top_rank as varchar(5))
when bottom_rank <= 3 then 'bottom' + cast(bottom_rank as varchar(5))
else 'na' end as ranking_lable
from weekly_ranking
order by case when top_rank <= 3 then top_rank
when bottom_rank <= 3 then (bottom_rank+3)
else 4 end
















