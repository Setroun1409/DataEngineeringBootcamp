--1.

CREATE TYPE films AS (
						film TEXT,
						votes INTEGER,
						rating REAL, 
						filmid TEXT
						)

CREATE TYPE quality_class AS ENUM('star','good','average','bad')	

CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	year INTEGER, 
	films films[],
	quality_class quality_class,
	years_since_film INTEGER,
	current_year INTEGER,
	is_active BOOLEAN, 
	PRIMARY KEY(actorid,year)
)

--2.

INSERT INTO actors

WITH yesterday_date AS (
	SELECT * FROM actors 
	WHERE current_year = 2003
),

	today_date AS (
	SELECT * FROM actor_films
	WHERE year = 2004
)

SELECT 
	COALESCE(t.actor, y.actor) AS actor,
	COALESCE(t.actorid, y.actorid) AS actorid,
	COALESCE(t.year, y.year) AS year,

	CASE 
		WHEN y.films IS NULL 
		THEN ARRAY[ROW(t.film,
						t.votes,
						t.rating,
						t.filmid)::films]
	    WHEN t.year IS NOT NULL THEN y.films || ARRAY[ROW(t.film,
															t.votes,
															t.rating,
															t.filmid)::films]
	ELSE y.films	
	END as films,
	CASE 
		WHEN t.year IS NOT NULL THEN 
			CASE WHEN t.rating > 8 THEN 'star'
				WHEN t.rating > 7 THEN 'good'
				WHEN t.rating > 6 THEN 'average'
				ELSE 'bad'
		END::quality_class
		ELSE y.quality_class
	END AS quality_class,
	
	CASE WHEN t.year IS NOT NULL THEN 0
		ELSE y.years_since_film + 1
			END AS years_since_film,
	COALESCE(t.year,y.current_year +1) AS current_year,
	CASE 
		WHEN t.film IS NOT NULL THEN TRUE
			ELSE FALSE END AS is_active	
			
FROM today_date t 
FULL OUTER JOIN yesterday_date y 
ON t.actorid = y.actorid
ON CONFLICT (actorid, year) DO NOTHING


--3.

create table actors_history_scd
(
	actor text,
	quality_class quality_class,
	is_active boolean,
	start_date integer,
	end_date integer,
	current_year INTEGER
);

INSERT INTO actors_history_scd
WITH streak_started AS (
    SELECT actor,
           current_year,
		   is_active,
           quality_class,
           LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) <> quality_class
               OR LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) IS NULL
               AS did_change
    FROM actors
),
     streak_identified AS (
         SELECT
            actor,
                quality_class,
				is_active,
                current_year,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier
         FROM streak_started
     ),
     aggregated AS (
         SELECT
            actor,
            quality_class,
			is_active,
            streak_identifier,
			current_year,
            MIN(current_year) AS start_date,
            MAX(current_year) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3,4,5
     )

    SELECT actor, quality_class, is_active,start_date, end_date,current_year
    FROM aggregated


--4


CREATE TYPE scd_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_date INTEGER,
                    end_date INTEGER
                        )


WITH last_date_scd AS (
    SELECT * FROM actors_history_scd
    WHERE current_year = 2004
    AND end_date = 2004
),
     historical_scd AS (
        SELECT
            actor,
            quality_class,
            is_active,
            start_date,
            end_date
        FROM actors_history_scd
        WHERE current_year = 2004
        AND end_date < 2004
     ),
     this_date_data AS (
         SELECT * FROM actors
         WHERE current_year = 2004
     ),
     unchanged_records AS (
         SELECT
                td.actor,
                td.quality_class,
                td.is_active,
                ld.start_date,
                td.current_year as end_date
        FROM this_date_data td
        JOIN last_date_scd ld
        ON ld.actor = td.actor
         WHERE td.quality_class = ld.quality_class
         AND td.is_active = ld.is_active
     ),
     changed_records AS (
        SELECT
                td.actor,
                UNNEST(ARRAY[
                    ROW(
                        ld.quality_class,
                        ld.is_active,
                        ld.start_date,
                        ld.end_date

                        )::scd_type,
                    ROW(
                        td.quality_class,
                        td.is_active,
                        td.current_year,
                        td.current_year
                        )::scd_type
                ]) as records
        FROM this_date_data td
        LEFT JOIN last_date_scd ld
        ON ld.actor = td.actor
         WHERE (td.quality_class <> ld.quality_class
          OR td.is_active <> ld.is_active)
     ),
     unnested_changed_records AS (

         SELECT actor,
                (records::scd_type).quality_class,
                (records::scd_type).is_active,
                (records::scd_type).start_date,
                (records::scd_type).end_date
                FROM changed_records
         ),
     new_records AS (

         SELECT
            td.actor,
                td.quality_class,
                td.is_active,
                td.current_year AS start_date,
                td.current_year AS end_date
         FROM this_date_data td
         LEFT JOIN last_date_scd ld
             ON td.actor = ld.actor
         WHERE ld.actor IS NULL

     )


SELECT *, 2022 AS current_year FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a

