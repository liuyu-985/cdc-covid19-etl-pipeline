CREATE OR REPLACE TABLE
  `genuine-arena-456023-i8.covid_raw_warehouse.covid_transformed`
AS

WITH raw AS (
  SELECT
    * REPLACE(
      IFNULL(cdc_case_earliest_dt, 'Missing') AS cdc_case_earliest_dt,
      IFNULL(cdc_report_dt,         'Missing') AS cdc_report_dt,
      IFNULL(onset_dt,              'Missing') AS onset_dt,
      IFNULL(pos_spec_dt,           'Missing') AS pos_spec_dt
    )
  FROM
    `genuine-arena-456023-i8.covid_raw_warehouse.covid_raw`
  WHERE
    DATE(_PARTITIONTIME) = DATE("2025-04-21")
),

parsed AS (
  SELECT
    *,
    SAFE_CAST(cdc_case_earliest_dt AS TIMESTAMP) AS case_earliest_ts,
    SAFE_CAST(cdc_report_dt        AS TIMESTAMP) AS report_ts,
    SAFE_CAST(onset_dt             AS TIMESTAMP) AS onset_ts,
    SAFE_CAST(pos_spec_dt          AS TIMESTAMP) AS spec_ts
  FROM raw
),

dated AS (
  SELECT
    *,
    IFNULL(DATE(case_earliest_ts), DATE(report_ts)) AS case_earliest_date,
    IFNULL(DATE(report_ts),       DATE(case_earliest_ts)) AS report_date,
    IFNULL(DATE(onset_ts),        DATE(report_ts)) AS onset_date,
    IFNULL(DATE(spec_ts),         DATE(report_ts)) AS spec_date
  FROM parsed
),

features AS (
  SELECT
    dated.*,

    
    IFNULL(
      DATE_DIFF(
        COALESCE(report_date, onset_date),
        onset_date,
        DAY
      ),
      0
    ) AS days_from_onset,

    IFNULL(
      DATE_DIFF(
        COALESCE(report_date, spec_date),
        spec_date,
        DAY
      ),
      0
    ) AS days_from_spec,

    
    IFNULL(
      CASE
        WHEN age_group IS NULL
           OR age_group = 'Missing' THEN 'Missing'
        WHEN REGEXP_CONTAINS(age_group, r'^\d+\+') THEN
          REGEXP_EXTRACT(age_group, r'^(\d+)')
        ELSE
          CAST(
            SAFE_DIVIDE(
              CAST(REGEXP_EXTRACT(age_group, r'^(\d+)')       AS FLOAT64)
            + CAST(REGEXP_EXTRACT(age_group, r'[-–]\s*(\d+)') AS FLOAT64),
              2
            )
          AS STRING)
      END,
      'Missing'
    ) AS age_mid,

    
    CASE
      WHEN death_yn  = 'Yes'     THEN 'Fatal'
      WHEN death_yn  = 'Unknown' THEN 'Unknown'
      WHEN death_yn  = 'Missing' THEN 'Missing'
      WHEN icu_yn    = 'Yes'     THEN 'ICU'
      WHEN icu_yn    = 'Unknown' THEN 'Unknown'
      WHEN icu_yn    = 'Missing' THEN 'Missing'
      WHEN hosp_yn   = 'Yes'     THEN 'Hospitalized'
      WHEN hosp_yn   = 'Unknown' THEN 'Unknown'
      WHEN hosp_yn   = 'Missing' THEN 'Missing'
      ELSE                          'Non‑hospitalized'
    END AS severity

  FROM dated
)

SELECT
  *
FROM features;