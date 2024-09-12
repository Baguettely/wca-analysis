/* WCA records assignment consistency check.

This script analyses the WCA database for record consistency.
It calculates all single and average records in a selected year and creates the table records_assignment which includes all results that include an assigned and/or calculated single or average record.
An inconsistency between an assigned record and a calculated record indicates an incorrectly assigned record.
Columns "single_action" and "average_action" indicate which action to take to correct this records assignment.

Please ensure the mysql.time_zone Table is installed before running.

Eleanor Sinnott
Last edited: 2024-09-12
*/

SET collation_connection = 'utf8mb4_unicode_ci';
SET @Year = 2024;
-- Determines the date of each round with a WCA schedule per the date of the latest end time of that round in the schedule, in the local timezone (per Regulation 9i2).
DROP TEMPORARY TABLE IF EXISTS round_dates;
CREATE TEMPORARY TABLE round_dates AS
  SELECT
    cv.competition_id AS competitionId,
    SUBSTRING_INDEX(sa.activity_code, '-', 1) AS eventId,
    LEFT(SUBSTRING_INDEX(sa.activity_code, '-r', -1), 1) AS round,
    DATE(MAX(CONVERT_TZ(sa.end_time, 'UTC', cv.timezone_id))) AS round_date
  FROM
    schedule_activities sa
    JOIN venue_rooms vr ON sa.holder_id = vr.id
    JOIN competition_venues cv ON vr.competition_venue_id = cv.id
  WHERE
    sa.holder_type = 'VenueRoom'
    AND SUBSTRING_INDEX(sa.activity_code, '-', 1) IN (SELECT id FROM Events)
  GROUP BY
    competitionId,
    eventId,
    round;

-- Assigns a numerical round number column to each round in the database corresponding to its roundTypeId.
DROP TEMPORARY TABLE IF EXISTS round_numbers;
CREATE TEMPORARY TABLE round_numbers AS
  SELECT
    t0.*,
    ROW_NUMBER() OVER (
      PARTITION BY t0.competitionId, t0.eventId
      ORDER BY rt.`rank`
    ) AS round
  FROM (
      SELECT DISTINCT
	r.competitionId,
        r.eventId,
        r.roundTypeId
      FROM Results r
      WHERE RIGHT(r.competitionId, 4) >= @Year
    ) t0
    JOIN RoundTypes rt ON t0.roundTypeId = rt.id;
-- Fetches the NR singles of each country as of the end of the previous year.
DROP TEMPORARY TABLE IF EXISTS old_nr_singles;
CREATE TEMPORARY TABLE old_nr_singles AS
  SELECT
    csr.countryId,
    csr.eventId,
    MIN(csr.best) AS old_NR_single
  FROM ConciseSingleResults csr
  WHERE csr.year < @Year
  GROUP BY countryId, eventId;
-- Fetches the NR averages of each country as of the end of the previous year.
DROP TEMPORARY TABLE IF EXISTS old_nr_averages;
CREATE TEMPORARY TABLE old_nr_averages AS
  SELECT
    car.countryId,
    car.eventId,
    MIN(car.average) AS old_NR_average
  FROM ConciseAverageResults car
  WHERE car.year < @Year
  GROUP BY countryId, eventId;
-- Fetches the CR singles of each continent as of the end of the previous year.
DROP TEMPORARY TABLE IF EXISTS old_cr_singles;
CREATE TEMPORARY TABLE old_cr_singles AS
  SELECT
    csr.continentId,
    csr.eventId,
    MIN(csr.best) AS old_CR_single
  FROM ConciseSingleResults csr
  WHERE csr.year < @Year
  GROUP BY continentId, eventId;
-- Fetches the CR averages of each continent as of the end of the previous year.
DROP TEMPORARY TABLE IF EXISTS old_cr_averages;
CREATE TEMPORARY TABLE old_cr_averages AS
  SELECT
    car.continentId,
    car.eventId,
    MIN(car.average) AS old_CR_average
  FROM ConciseAverageResults car
  WHERE car.year < @Year
  GROUP BY continentId, eventId;
-- Fetches WR singles as of the end of the previous year.
DROP TEMPORARY TABLE IF EXISTS old_wr_singles;
CREATE TEMPORARY TABLE old_wr_singles AS
  SELECT
    csr.eventId,
    MIN(csr.best) AS old_WR_single
  FROM ConciseSingleResults csr
  WHERE csr.year < @Year
  GROUP BY eventId;
-- Fetches WR averages as of the end of the previous year.
DROP TEMPORARY TABLE IF EXISTS old_wr_averages;
CREATE TEMPORARY TABLE old_wr_averages AS
  SELECT
    car.eventId,
    MIN(car.average) AS old_WR_average
  FROM ConciseAverageResults car
  WHERE car.year < @Year
  GROUP BY eventId;
-- Joins round date to results table and filters out rows that are not <= NRs from previous years. Assigns ranking 1 to each single or average that is the best for that country for that day.
DROP TEMPORARY TABLE IF EXISTS t1;
CREATE TEMPORARY TABLE t1 AS
  SELECT
    r.id AS results_id,
    IF(rd.round_date IS NOT NULL, rd.round_date, c.start_date) AS round_date,
    r.personId,
    r.countryId,
    r.competitionId,
    r.eventId,
    rn.round,
    RANK() OVER (
      PARTITION BY
      	r.countryId,
      	r.eventId,
      	IF(rd.round_date IS NOT NULL, rd.round_date, c.start_date)
      ORDER BY r.best
    ) AS day_best_single,
    RANK() OVER(
      PARTITION BY
      	r.countryId,
      	r.eventId,
      IF(rd.round_date IS NOT NULL, rd.round_date, c.start_date)
      ORDER BY
        CASE WHEN r.average > 0 THEN r.average ELSE 999999999999 END
    ) AS day_best_average,
    r.best,
    r.average,
    IF(r.regionalSingleRecord IS NULL, "", r.regionalSingleRecord) AS stored_single,
    IF(r.regionalAverageRecord IS NULL, "", r.regionalAverageRecord) AS stored_average,
    ons.old_NR_single,
    ona.old_NR_average
  FROM
    Results r
    JOIN Competitions c
	ON c.id = r.competitionId
    JOIN competition_events ce
    	ON r.competitionId = ce.competition_id
    	AND r.eventId = ce.event_id
    JOIN round_numbers rn
	ON rn.competitionId = r.competitionId
    	AND rn.eventId = r.eventId
    	AND rn.roundTypeId = r.roundTypeId
    LEFT JOIN round_dates rd
	ON rd.competitionId = r.competitionId
    	AND rd.eventId = r.eventId
    	AND rd.round = rn.round
    LEFT JOIN old_nr_singles ons
	ON ons.countryId = r.countryId
    	AND ons.eventId = r.eventId
    LEFT JOIN old_nr_averages ona
	ON ona.countryId = r.countryId
    	AND ona.eventId = r.eventId
  WHERE
    RIGHT(r.competitionId, 4) >=@Year
    AND r.best > 0
    AND (
     (r.best <= ons.old_NR_single OR ons.old_NR_single IS NULL)
     OR (r.average > 0 AND
        (ona.old_NR_average IS NULL OR r.average <= ona.old_NR_average)
        )
     );
-- Removes rows from t1 that are not the fastest result of that day. Calculates whether or not each result from remaining rows is NR single or average by whether the result is <= previous results from that year and <= the previous year's record (if there is one).
DROP TEMPORARY TABLE IF EXISTS t2;
CREATE TEMPORARY TABLE t2 AS
  SELECT
    t1.results_id,
    t1.round_date,
    t1.personId,
    t1.countryId,
    t1.competitionId,
    t1.eventId,
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
        PARTITION BY t1.eventId, t1.countryId
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
        PARTITION BY t1.eventId, t1.countryId
        ORDER BY t1.round_date
        ) = t1.average,
       1, 0) AS NRaverage
  FROM t1
  WHERE
    t1.day_best_single = 1
    OR t1.day_best_average = 1
    OR t1.stored_single <> ""
    OR t1.stored_average <> "";

-- Joins t2 to continental and world records from previous year. Calculates whether or not each result is CR or WR single/average by whether the result is <= previous results from that year and <= last year's best results.
DROP TEMPORARY TABLE IF EXISTS t3;
CREATE TEMPORARY TABLE t3 AS
  SELECT
    c.continentId,
    CASE c.continentId
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
      ) OVER(PARTITION BY t2.eventId, c.continentId
        ORDER BY t2.round_date, t2.best
      ) = t2.best,
      1, 0) CRsingle,
    IF(
      MIN(
        CASE WHEN average > 0 
             AND (average <= oca.old_CR_average OR oca.old_CR_average IS NULL) 
             THEN average END
        ) OVER(
        PARTITION BY t2.eventId, c.continentId
        ORDER BY t2.round_date, t2.average
      ) = t2.average,
      1, 0) CRaverage,
    IF(
      MIN(
        CASE WHEN t2.best <= ows.old_WR_single OR ows.old_WR_single IS NULL 
        THEN best END
      ) OVER(
        PARTITION BY t2.eventId
        ORDER BY t2.round_date, t2.best
      ) = t2.best,
      1, 0) WRsingle,
    IF(
      MIN(
        CASE WHEN average > 0
        AND (average <= owa.old_WR_average OR owa.old_WR_average IS NULL) 
        THEN average END
      ) OVER(
        PARTITION BY t2.eventId
        ORDER BY t2.round_date, t2.average
      ) = t2.average,
      1, 0) WRaverage
  FROM t2
    JOIN Countries c
	ON c.id = t2.countryId
    LEFT JOIN old_cr_singles ocs
	ON ocs.continentId = c.continentId
   	AND t2.eventId = ocs.eventId
    LEFT JOIN old_cr_averages oca
	ON oca.continentId = c.continentId
       AND t2.eventId = oca.eventId
    LEFT JOIN old_wr_singles ows
	ON t2.eventId = ows.eventId
    LEFT JOIN old_wr_averages owa
	ON t2.eventId = owa.eventId
  WHERE
    t2.stored_single <> ""
    OR t2.stored_average <> ""
    OR t2.NRaverage = 1
    OR t2.NRsingle = 1;
-- combines NR, CR, and WR columns to assign a record id for each row.
DROP TEMPORARY TABLE IF EXISTS t4;
CREATE TEMPORARY TABLE t4 AS
  SELECT
    t3.results_id,
    t3.round_date,
    t3.personId,
    t3.countryId,
    t3.continentId,
    t3.competitionId,
    t3.eventId,
    t3.round,
    t3.best,
    t3.average,
    t3.stored_single,
    t3.stored_average,
    CASE WHEN t3.WRsingle = 1 THEN "WR"
    	WHEN t3.CRsingle = 1 THEN t3.cr_id
    	WHEN t3.NRsingle = 1 THEN "NR"
    	ELSE "" END AS calculated_single,
    CASE WHEN t3.WRaverage = 1 THEN "WR"
    	WHEN t3.CRaverage = 1 THEN t3.cr_id
    	WHEN t3.NRaverage = 1 THEN "NR"
    	ELSE "" END AS calculated_average
  FROM t3;
-- Compares calculated records from t4 to assigned records and flags inconsistencies.
SELECT
  t4.*,
  CASE WHEN t4.stored_single <> ""
	  AND t4.calculated_single <> ""
	  AND t4.stored_single <> t4.calculated_single
	  THEN CONCAT("replace ", t4.stored_single, " with ", calculated_single)
	WHEN t4.stored_single = ""
	  AND t4.calculated_single <> ""
	  THEN CONCAT("add ", t4.calculated_single)
	WHEN t4.stored_single <> ""
	  AND t4.calculated_single = ""
	  THEN CONCAT("remove ", t4.stored_single)
	  ELSE NULL END AS single_action,
  CASE WHEN t4.stored_average <> ""
	  AND t4.calculated_average <> ""
 	  AND t4.stored_average <> t4.calculated_average
	  THEN CONCAT("replace ", t4.stored_average, " with ", calculated_average)
	WHEN t4.stored_average = ""
	  AND t4.calculated_average <> ""
	  THEN CONCAT("add ", t4.calculated_average)
	WHEN t4.stored_average <> ""
      AND t4.calculated_average = ""
	  THEN CONCAT("remove ", t4.stored_average)
	  ELSE NULL END AS average_action,
    CONCAT(
        CASE WHEN calculated_single <> ""
	        AND stored_single <> calculated_single
	        THEN CONCAT("UPDATE Results SET regionalSingleRecord = '", calculated_single, "' WHERE id = ", results_id, "; ")
        WHEN calculated_single = ""
	        AND stored_single <> ""
	        THEN CONCAT("UPDATE Results SET regionalSingleRecord = NULL WHERE id = ", results_id, "; ")
            ELSE "" END,
        CASE WHEN calculated_average <> ""
	        AND stored_average <> calculated_average
	        THEN CONCAT("UPDATE Results SET regionalAverageRecord = '", calculated_average, "' WHERE id = ", results_id, "; ")
        WHEN calculated_average = ""
	        AND stored_average <> ""
	    THEN CONCAT("UPDATE Results SET regionalAverageRecord = NULL WHERE id = ", results_id, "; ")
           ELSE "" END
    ) AS Query
FROM t4
WHERE stored_single <> calculated_single OR stored_average <> calculated_average;
