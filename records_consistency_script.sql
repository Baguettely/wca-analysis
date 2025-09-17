/* WCA records assignment consistency check.

This script analyses the WCA database for record consistency.
It calculates all single and average records in a selected year and creates the table records_assignment which includes all results that include an assigned and/or calculated single or average record.
An inconsistency between an assigned record and a calculated record indicates an incorrectly assigned record.
Columns "single_action" and "average_action" indicate which action to take to correct this records assignment.

Please ensure the mysql.time_zone Table is installed before running.

Eleanor Sinnott
Last edited: 2025-09-17
*/

SET collation_connection = 'utf8mb4_unicode_ci';
SET @Year = 2024;
-- Determines the date of each round with a WCA schedule per the date of the latest end time of that round in the schedule, in the local timezone (per Regulation 9i2).
DROP TABLE IF EXISTS round_dates;
CREATE TEMPORARY TABLE round_dates AS
  SELECT
    cv.competition_id AS competition_id,
    SUBSTRING_INDEX(sa.activity_code, '-', 1) AS event_id,
    LEFT(SUBSTRING_INDEX(sa.activity_code, '-r', -1), 1) AS round,
    DATE(MAX(CONVERT_TZ(sa.end_time, 'UTC', cv.timezone_id))) AS round_date
  FROM
    schedule_activities sa
    JOIN venue_rooms vr ON sa.venue_room_id = vr.id
    JOIN competition_venues cv ON vr.competition_venue_id = cv.id
  WHERE SUBSTRING_INDEX(sa.activity_code, '-', 1) IN (SELECT id FROM events)
  GROUP BY
    competition_id,
    event_id,
    round
;

-- Assigns a numerical round number column to each round in the database corresponding to its round_type_id.
DROP TABLE IF EXISTS round_numbers;
CREATE TEMPORARY TABLE round_numbers AS
  SELECT
    t0.*,
    ROW_NUMBER() OVER (
      PARTITION BY t0.competition_id, t0.event_id
      ORDER BY rt.`rank`
    ) AS round
  FROM (
      SELECT DISTINCT
	r.competition_id,
        r.event_id,
        r.round_type_id
      FROM results r
      WHERE RIGHT(r.competition_id, 4) >= @Year
    ) t0
    JOIN round_types rt ON t0.round_type_id = rt.id
;

-- Calculates the NR singles of each country as of the end of the previous year.
DROP TABLE IF EXISTS old_nr_singles;
CREATE TEMPORARY TABLE old_nr_singles AS
  SELECT
    csr.country_id,
    csr.event_id,
    MIN(csr.best) AS old_NR_single
  FROM concise_single_results csr
  WHERE csr.year < @Year
  GROUP BY country_id, event_id
;

-- Fetches the NR averages of each country as of the end of the previous year.
DROP TABLE IF EXISTS old_nr_averages;
CREATE TEMPORARY TABLE old_nr_averages AS
  SELECT
    car.country_id,
    car.event_id,
    MIN(car.average) AS old_NR_average
  FROM concise_average_results car
  WHERE car.year < @Year
  GROUP BY country_id, event_id
;

-- Calculates the CR singles of each continent as of the end of the previous year.
DROP TABLE IF EXISTS old_cr_singles;
CREATE TEMPORARY TABLE old_cr_singles AS
  SELECT
    csr.continent_id,
    csr.event_id,
    MIN(csr.best) AS old_CR_single
  FROM concise_single_results csr
  WHERE csr.year < @Year
  GROUP BY continent_id, event_id
;

-- Fetches the CR averages of each continent as of the end of the previous year.
DROP TABLE IF EXISTS old_cr_averages;
CREATE TEMPORARY TABLE old_cr_averages AS
  SELECT
    car.continent_id,
    car.event_id,
    MIN(car.average) AS old_CR_average
  FROM concise_average_results car
  WHERE car.year < @Year
  GROUP BY continent_id, event_id
;

-- Calculates WR singles as of the end of the previous year.
DROP TABLE IF EXISTS old_wr_singles;
CREATE TEMPORARY TABLE old_wr_singles AS
  SELECT
    csr.event_id,
    MIN(csr.best) AS old_WR_single
  FROM concise_single_results csr
  WHERE csr.year < @Year
  GROUP BY event_id
;

-- Calculates WR averages as of the end of the previous year.
DROP TABLE IF EXISTS old_wr_averages;
CREATE TEMPORARY TABLE old_wr_averages AS
  SELECT
    car.event_id,
    MIN(car.average) AS old_WR_average
  FROM concise_average_results car
  WHERE car.year < @Year
  GROUP BY event_id
;

-- Joins round date to results table and filters out rows that are not <= NRs from previous years. Assigns ranking 1 to each single or average that is the best for that country for that day.
DROP TABLE IF EXISTS t1;
CREATE TEMPORARY TABLE t1 AS
SELECT
    r.id AS results_id,
    IF(rd.round_date IS NOT NULL, rd.round_date, c.start_date) AS round_date,
    r.person_id,
    r.country_id,
    r.competition_id,
    r.event_id,
    rn.round,
    RANK() OVER (
      PARTITION BY
          r.country_id,
          r.event_id,
          IF(rd.round_date IS NOT NULL, rd.round_date, c.start_date)
      ORDER BY CASE WHEN r.best > 0 THEN r.best ELSE 999999999999 END
    ) AS day_best_single,
    RANK() OVER(
      PARTITION BY
          r.country_id,
          r.event_id,
      IF(rd.round_date IS NOT NULL, rd.round_date, c.start_date)
      ORDER BY
        CASE WHEN r.average > 0 THEN r.average ELSE 999999999999 END
    ) AS day_best_average,
    r.best,
    r.average,
    IF(r.regional_single_record IS NULL, "", r.regional_single_record) AS stored_single,
    IF(r.regional_average_record IS NULL, "", r.regional_average_record) AS stored_average,
    ons.old_NR_single,
    ona.old_NR_average
  FROM
    results r
    JOIN competitions c
    ON c.id = r.competition_id
    JOIN competition_events ce
        ON r.competition_id = ce.competition_id
        AND r.event_id = ce.event_id
    JOIN round_numbers rn
    ON rn.competition_id = r.competition_id
        AND rn.event_id = r.event_id
        AND rn.round_type_id = r.round_type_id
    LEFT JOIN round_dates rd
    ON rd.competition_id = r.competition_id
        AND rd.event_id = r.event_id
        AND rd.round = rn.round
    LEFT JOIN old_nr_singles ons
    ON ons.country_id = r.country_id
        AND ons.event_id = r.event_id
    LEFT JOIN old_nr_averages ona
    ON ona.country_id = r.country_id
        AND ona.event_id = r.event_id
  WHERE
    RIGHT(r.competition_id, 4) >= @Year
    AND (
(
     (r.best > 0 AND (r.best <= ons.old_NR_single OR ons.old_NR_single IS NULL))
     OR (r.average > 0 AND
        (ona.old_NR_average IS NULL OR r.average <= ona.old_NR_average)
        )
     ) OR regional_single_record <> "" OR regional_average_record <> ""
)
;

-- Removes rows from t1 that are not the fastest result of that day. Calculates whether or not each result from remaining rows is NR single or average by whether the result is <= previous results from that year and <= the previous year's record (if there is one).
DROP TABLE IF EXISTS t2;
CREATE TEMPORARY TABLE t2 AS
  SELECT
    t1.results_id,
    t1.round_date,
    t1.person_id,
    t1.country_id,
    t1.competition_id,
    t1.event_id,
    t1.round,
    t1.best,
    t1.average,
    t1.stored_single,
    t1.stored_average,
    IF(
      MIN(
        CASE WHEN t1.best <= t1.old_NR_single OR t1.old_NR_single IS NULL
        THEN t1.best END
      ) OVER(
        PARTITION BY t1.event_id, t1.country_id
        ORDER BY t1.round_date
      ) = t1.best,
      1, 0
    ) AS NRsingle,
    IF(
      MIN(
        CASE WHEN t1.average > 0
          AND (t1.average <= t1.old_NR_average OR t1.old_NR_average IS NULL)
         THEN t1.average END
         ) OVER(
        PARTITION BY t1.event_id, t1.country_id
        ORDER BY t1.round_date
        ) = t1.average,
       1, 0) AS NRaverage
  FROM t1
  WHERE
    t1.day_best_single = 1
    OR t1.day_best_average = 1
    OR t1.stored_single <> ""
    OR t1.stored_average <> ""
;

-- Joins t2 to continental and world records from previous year. Calculates whether or not each result is CR or WR single/average by whether the result is <= previous results from that year and <= last year's best results.
DROP TABLE IF EXISTS t3;
CREATE TEMPORARY TABLE t3 AS
  SELECT
    c.continent_id,
    CASE c.continent_id
	WHEN "_Africa" THEN "AfR"
	WHEN "_Asia" THEN "AsR"
	WHEN "_Europe" THEN "ER"
	WHEN "_Oceania" THEN "OcR"
	WHEN "_North America" THEN "NAR"
	WHEN "_South America" THEN "SAR"
	END AS cr_id,
    t2.*,
    IF(
      MIN(
        CASE WHEN t2.best <= ocs.old_CR_single OR ocs.old_CR_single IS NULL 
        THEN best END
      ) OVER(PARTITION BY t2.event_id, c.continent_id
        ORDER BY t2.round_date, t2.best
      ) = t2.best,
      1, 0) CRsingle,
    IF(
      MIN(
        CASE WHEN average > 0 
             AND (average <= oca.old_CR_average OR oca.old_CR_average IS NULL) 
             THEN average END
        ) OVER(
        PARTITION BY t2.event_id, c.continent_id
        ORDER BY t2.round_date, t2.average
      ) = t2.average,
      1, 0) CRaverage,
    IF(
      MIN(
        CASE WHEN t2.best <= ows.old_WR_single OR ows.old_WR_single IS NULL 
        THEN best END
      ) OVER(
        PARTITION BY t2.event_id
        ORDER BY t2.round_date, t2.best
      ) = t2.best,
      1, 0) WRsingle,
    IF(
      MIN(
        CASE WHEN average > 0
        AND (average <= owa.old_WR_average OR owa.old_WR_average IS NULL) 
        THEN average END
      ) OVER(
        PARTITION BY t2.event_id
        ORDER BY t2.round_date, t2.average
      ) = t2.average,
      1, 0) WRaverage
  FROM t2
    JOIN countries c
	ON c.id = t2.country_id
    LEFT JOIN old_cr_singles ocs
	ON ocs.continent_id = c.continent_id
   	AND t2.event_id = ocs.event_id
    LEFT JOIN old_cr_averages oca
	ON oca.continent_id = c.continent_id
       AND t2.event_id = oca.event_id
    LEFT JOIN old_wr_singles ows
	ON t2.event_id = ows.event_id
    LEFT JOIN old_wr_averages owa
	ON t2.event_id = owa.event_id
  WHERE
    t2.stored_single <> ""
    OR t2.stored_average <> ""
    OR t2.NRaverage = 1
    OR t2.NRsingle = 1
;

-- combines NR, CR, and WR columns to assign a record id for each row.
DROP TABLE IF EXISTS t4;
CREATE TEMPORARY TABLE t4 AS
  SELECT
    t3.*,
    CASE WHEN t3.WRsingle = 1 THEN "WR"
    	WHEN t3.CRsingle = 1 THEN t3.cr_id
    	WHEN t3.NRsingle = 1 THEN "NR"
    	ELSE "" END AS calculated_single,
    CASE WHEN t3.WRaverage = 1 THEN "WR"
    	WHEN t3.CRaverage = 1 THEN t3.cr_id
    	WHEN t3.NRaverage = 1 THEN "NR"
    	ELSE "" END AS calculated_average
  FROM t3
  ;
  
-- Compares calculated records from t4 to assigned records and flags inconsistencies.
DROP TABLE IF EXISTS records;
CREATE TABLE records AS
SELECT
  t4.*,
  CASE WHEN t4.stored_single <> ""
	  AND t4.calculated_single <> ""
	  AND t4.stored_single <> t4.calculated_single
	  THEN CONCAT("single: replace ", t4.stored_single, " with ", calculated_single)
	WHEN t4.stored_single = ""
	  AND t4.calculated_single <> ""
	  THEN CONCAT("single: add ", t4.calculated_single)
	WHEN t4.stored_single <> ""
	  AND t4.calculated_single = ""
	  THEN CONCAT("single: remove ", t4.stored_single)
	  ELSE NULL END AS single_action,
  CASE WHEN t4.stored_average <> ""
	  AND t4.calculated_average <> ""
 	  AND t4.stored_average <> t4.calculated_average
	  THEN CONCAT("average: replace ", t4.stored_average, " with ", calculated_average)
	WHEN t4.stored_average = ""
	  AND t4.calculated_average <> ""
	  THEN CONCAT("average: add ", t4.calculated_average)
	WHEN t4.stored_average <> ""
      AND t4.calculated_average = ""
	  THEN CONCAT("average: remove ", t4.stored_average)
	  ELSE NULL END AS average_action,
    CONCAT(
        CASE WHEN calculated_single <> ""
	        AND stored_single <> calculated_single
	        THEN CONCAT("UPDATE results SET regional_single_record = '", calculated_single, "' WHERE id = ", results_id, "; ")
        WHEN calculated_single = ""
	        AND stored_single <> ""
	        THEN CONCAT("UPDATE results SET regional_single_record = NULL WHERE id = ", results_id, "; ")
            ELSE "" END,
        CASE WHEN calculated_average <> ""
	        AND stored_average <> calculated_average
	        THEN CONCAT("UPDATE results SET regional_average_record = '", calculated_average, "' WHERE id = ", results_id, "; ")
        WHEN calculated_average = ""
	        AND stored_average <> ""
	    THEN CONCAT("UPDATE results SET regional_average_record = NULL WHERE id = ", results_id, "; ")
           ELSE "" END
    ) AS Query
FROM t4
;
SELECT * FROM records
WHERE stored_single <> calculated_single OR stored_average <> calculated_average
	;
