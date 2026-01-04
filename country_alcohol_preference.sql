/*
 * Query Requirements:
 * Find all countries and their preference for beer, spirit, and wine,
 * based on the highest serving type as their preference.
 *
 * The output should show only the countries whose preferences are wine
 * and spirits, ordered in ascending order of the country name.
 */

SELECT
  country,
  CASE
    WHEN beer_servings >= spirit_servings AND beer_servings >= wine_servings THEN 'beer'
    WHEN spirit_servings >= beer_servings AND spirit_servings >= wine_servings THEN 'spirit'
    ELSE 'wine'
  END AS preference
FROM
  playground.alcohol_consumption
WHERE
  CASE
    WHEN beer_servings >= spirit_servings AND beer_servings >= wine_servings THEN 'beer'
    WHEN spirit_servings >= beer_servings AND spirit_servings >= wine_servings THEN 'spirit'
    ELSE 'wine'
  END IN ('wine', 'spirit')
ORDER BY
  country ASC;
