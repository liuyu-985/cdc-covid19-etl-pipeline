
SELECT
  COUNT(*) AS raw_rows
FROM
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_raw`
WHERE
  DATE(_PARTITIONTIME) = DATE("2025-04-21");


SELECT
  COUNT(*) AS transformed_rows
FROM
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`;


SELECT
  COUNTIF(days_from_onset  IS NULL) AS null_days_from_onset,
  COUNTIF(days_from_spec   IS NULL) AS null_days_from_spec,
  COUNTIF(age_mid          Is NULL) AS null_age_mid,
  COUNTIF(severity         IS NULL) AS null_severity,
  COUNTIF(report_date      IS NULL) AS null_report_date
FROM
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`;


SELECT *
FROM `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`
WHERE DATE_DIFF(report_date, onset_date, DAY) < 0
LIMIT 50;