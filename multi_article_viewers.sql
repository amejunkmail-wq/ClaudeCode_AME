SELECT DISTINCT
  viewer_id
FROM
  playground.views
GROUP BY
  viewer_id,
  view_date
HAVING
  COUNT(DISTINCT article_id) > 1
ORDER BY
  viewer_id ASC;
