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
