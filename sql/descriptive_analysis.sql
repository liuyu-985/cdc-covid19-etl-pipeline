
SELECT
  COUNT(*) AS total_rows
FROM
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`;


SELECT
  severity,
  COUNT(*) AS cnt,
  ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`),1) AS pct
FROM
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`
GROUP BY severity
ORDER BY cnt DESC;


SELECT
  DATE_TRUNC(report_date, MONTH) AS month_start,
  COUNT(*)                     AS case_count
FROM
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`
GROUP BY
  month_start
ORDER BY
  month_start;


SELECT
  APPROX_QUANTILES(days_from_onset, 4) AS deciles
FROM
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`
WHERE
  days_from_onset > 0;


SELECT
  age_mid,
  COUNT(*) AS cnt
FROM
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`
WHERE age_mid != 'Missing'
GROUP BY age_mid
ORDER BY SAFE_CAST(age_mid AS FLOAT64);
