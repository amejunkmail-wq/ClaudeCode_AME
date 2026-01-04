SELECT
  s.state_name,
  b.day_of_week,
  CAST(AVG(b.births) AS INT64) AS avg_births_test
FROM
  playground.us_birth_stats AS b
JOIN
  playground.states AS s
ON
  b.state = s.state_abbr
GROUP BY
  s.state_name,
  b.day_of_week
ORDER BY
  s.state_name,
  b.day_of_week;
