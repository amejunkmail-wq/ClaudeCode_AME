/*
 * Query Requirements:
 * Match each gift to the smallest package it fits into based on dimensions.
 * A gift fits in a package if its dimensions are less than or equal to those of the package.
 * A package is considered smaller than another if its volume is smaller.
 * Each package can hold only one gift.
 *
 * Output: package_type and number (count of gifts matched to each package_type)
 * Exclude package types not used.
 * Sort by package_type in ascending order.
 *
 * Assumptions:
 * - Every gift fits in at least one package
 * - No two packages have the same volume
 */

WITH gift_package_matches AS (
  SELECT
    g.gift_id,
    p.package_type,
    p.length * p.width * p.height AS package_volume,
    ROW_NUMBER() OVER (
      PARTITION BY g.gift_id
      ORDER BY p.length * p.width * p.height ASC
    ) AS rn
  FROM
    playground.gifts g
  INNER JOIN
    playground.packages p
  ON
    g.length <= p.length
    AND g.width <= p.width
    AND g.height <= p.height
)
SELECT
  package_type,
  COUNT(*) AS number
FROM
  gift_package_matches
WHERE
  rn = 1
GROUP BY
  package_type
ORDER BY
  package_type ASC;
