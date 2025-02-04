
--1
WITH deduped AS (
	SELECT 
		g.game_date_est,
		g.season,
		g.home_team_id,
		gd.*,
		ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
	FROM game_details gd
	JOIN games g
	ON gd.game_id = g.game_id
	
)

SELECT *
FROM deduped 
WHERE row_num = 1

--2 


CREATE TABLE user_devices_cumulated (
	device_id TEXT,
	device_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (device_id,date)
)


INSERT INTO user_devices_cumulated

WITH yesterday AS (
	SELECT * 
	FROM 
	user_devices_cumulated
	WHERE date = DATE('2023-01-09')
	
),

today AS (
	SELECT 
		CAST(device_id AS TEXT) AS device_id,
		DATE(CAST(event_time AS TIMESTAMP)) AS date_active
	FROM 
	events
	WHERE DATE(CAST(event_time AS TIMESTAMP))  = '2023-01-09' 
	AND device_id IS NOT NULL
	GROUP BY device_id,DATE(CAST(event_time AS TIMESTAMP))
			

)


SELECT 
	COALESCE(t.device_id,y.device_id) AS device_id,
	CASE
		WHEN y.device_activity_datelist IS NULL
		THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.device_activity_datelist
		ELSE y.device_activity_datelist || ARRAY[t.date_active]
		END AS device_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
	  
FROM 
today t
FULL OUTER JOIN yesterday y
ON t.device_id = y.device_id


WITH devices AS (
		SELECT *
		FROM user_devices_cumulated
		WHERE date = DATE('2023-01-10')
	),

	series AS (
		SELECT *
		FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
	),
	
	place_holder_inst AS (
		SELECT 
			CASE 
				WHEN device_activity_datelist @> ARRAY [DATE(series_date)]
						THEN CAST(POW(2,32 - (date - DATE(series_date))) AS BIGINT)
						ELSE 0
					END  AS placeholder_int_value, 
			*
		FROM devices CROSS JOIN series
	) 

SELECT
	device_id,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) 
FROM 
place_holder_inst
GROUP BY device_id

--3



CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (host,date)
)


INSERT INTO hosts_cumulated

WITH yesterday AS (
	SELECT * 
	FROM 
	hosts_cumulated
	WHERE date = DATE('2023-01-09')
	
),

today AS (
	SELECT 
		host,
		DATE(CAST(event_time AS TIMESTAMP)) AS date_active
	FROM 
	events
	WHERE DATE(CAST(event_time AS TIMESTAMP))  = '2023-01-09' 
	AND host IS NOT NULL
	GROUP BY host,DATE(CAST(event_time AS TIMESTAMP))
			

)


SELECT 
	COALESCE(t.host,y.host) AS host,
	CASE
		WHEN y.host_activity_datelist IS NULL
		THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.host_activity_datelist
		ELSE y.host_activity_datelist || ARRAY[t.date_active]
		END AS host_activity_datelist,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
	  
FROM 
today t
FULL OUTER JOIN yesterday y
ON t.host = y.host


WITH hosts AS (
		SELECT *
		FROM hosts_cumulated
		WHERE date = DATE('2023-01-10')
	),

	series AS (
		SELECT *
		FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') AS series_date
	),
	
	place_holder_inst AS (
		SELECT 
			CASE 
				WHEN host_activity_datelist @> ARRAY [DATE(series_date)]
						THEN CAST(POW(2,32 - (date - DATE(series_date))) AS BIGINT)
						ELSE 0
					END  AS placeholder_int_value, 
			*
		FROM hosts CROSS JOIN series
	) 

SELECT
	host,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) 
FROM 
place_holder_inst
GROUP BY host

--4


CREATE TABLE host_activity_reduced (
	host TEXT,
	month_start DATE,
	metric_name TEXT,
	metric_array REAL[],
	PRIMARY KEY (host, month_start,metric_name)
)


INSERT INTO host_activity_reduced

WITH daily_aggregate AS (
	SELECT 
		host,
		DATE(event_time) AS date,
		COUNT(1) AS num_site_hits
	FROM events
	WHERE DATE(event_time) = DATE('2023-01-02')
	AND host IS NOT NULL
	GROUP BY host, DATE(event_time)
		
),

	yesterday_array AS (
		SELECT *
		FROM 
		host_activity_reduced
		WHERE month_start = DATE('2023-01-01')
	)

SELECT 
	COALESCE(da.host,ya.host) AS host,
	COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month,
	'site_hits' AS metric_name,
	CASE WHEN ya.metric_array IS NOT NULL THEN
		ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)]
	WHEN ya.metric_array IS NULL
		THEN ARRAY_FILL(0,ARRAY[COALESCE(date - DATE(DATE_TRUNC('month',date)),0)]) || ARRAY[COALESCE(da.num_site_hits,0)]
	END AS metric_array
	
FROM 
daily_aggregate da
FULL OUTER JOIN
yesterday_array ya
ON da.host = ya.host
ON CONFLICT(host, month_start,metric_name) 
DO 
	UPDATE SET metric_array = EXCLUDED.metric_array

--5 
WITH agg AS (
SELECT 
	metric_name, 
	month_start AS month,
	UNNEST(ARRAY[SUM(metric_array[1]),
				 SUM(metric_array[2]),
				 SUM(metric_array[3])]) AS summed_array
FROM
	host_activity_reduced
GROUP BY 
	metric_name,
	month_start
)

SELECT 
	metric_name,
	month_start + CAST(CAST(index - 1 AS TEXT) || 'day' AS INTERVAL), 
	elem AS value
FROM agg
CROSS JOIN
UNNEST(agg.summed_array)
WITH ORDINALITY AS a(elem,index)


