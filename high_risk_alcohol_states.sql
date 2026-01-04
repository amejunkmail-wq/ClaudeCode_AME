/*
 * Query Requirements:
 * Using playground.bad_drivers, determine the states and their percentage of
 * alcohol impaired collisions where the risk of fatal collisions involving
 * alcohol is significantly higher than the national average.
 *
 * Consider a state as high-risk if its percent_alcohol_impaired is at least
 * 20% higher than the national average.
 *
 * Show the output in descending order of the percent_alcohol_impaired.
 */

WITH national_avg AS (
  SELECT
    AVG(percent_alcohol_impaired) AS avg_percent_alcohol_impaired
  FROM
    playground.bad_drivers
)
SELECT
  state,
  percent_alcohol_impaired
FROM
  playground.bad_drivers,
  national_avg
WHERE
  percent_alcohol_impaired >= (avg_percent_alcohol_impaired * 1.20)
ORDER BY
  percent_alcohol_impaired DESC;
