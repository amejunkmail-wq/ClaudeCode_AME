SELECT
  day_of_week,
  CAST(AVG(births) AS INT64) AS avg_births
FROM
  playground.us_birth_stats
GROUP BY
  day_of_week
ORDER BY
  day_of_week;
