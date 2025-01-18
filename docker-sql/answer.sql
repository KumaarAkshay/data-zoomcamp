--Q3
SELECT
	Sum(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) mile_1,
	Sum(CASE WHEN trip_distance > 1 and trip_distance <= 3 THEN 1 ELSE 0 END) mile_1_to_3,
	Sum(CASE WHEN trip_distance > 3 and trip_distance <= 7 THEN 1 ELSE 0 END) mile_3_to_7,
	Sum(CASE WHEN trip_distance > 7 and trip_distance <= 10 THEN 1 ELSE 0 END) mile_7_to_10,
	Sum(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) mile_10_plus
 FROM
     yellow_taxi_data
 WHERE
	 lpep_dropoff_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01' ;

-- Q4
SELECT lpep_pickup_datetime FROM yellow_taxi_data 
WHERE trip_distance = (SELECT max(trip_distance) FROM yellow_taxi_data);

-- Q5

SELECT distinct loc_id ,"Zone", cnt ,sm from (
(SELECT distinct loc_id ,"Zone" FROM taxi_zone_lookup ) a
JOIN
(
SELECT pickup_location ,COUNT(*) as cnt , sum(total_amount) sm FROM yellow_taxi_data
where DATE_TRUNC('day',lpep_pickup_datetime) = '2019-10-18'
group by pickup_location
having sum(total_amount) > 13000
order by COUNT(*) desc
Limit 3
) b ON a.loc_id = b.pickup_location
);

-- Q6
SELECT tip_amount ,pickup_location ,"DOLocationID" ,"Zone"
FROM (SELECT *
from yellow_taxi_data
where tip_amount = (
SELECT Max(tip_amount) from yellow_taxi_data
Where pickup_location IN (SELECT distinct loc_id FROM taxi_zone_lookup
WHERE "Zone" = 'East Harlem North'))) a JOIN
taxi_zone_lookup b ON a."DOLocationID" = b.loc_id
;
