-- CLEAN HEART RATE DATA

CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.heartrate_cleaned` AS
SELECT
  CAST(Id AS INT64) AS user_id,
  PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', Time) AS datetime,
  CAST(Value AS INT64) AS heart_rate
FROM `bold-momentum-471601-r3.bellabeat_dataset.heartrate_raw`
WHERE Value IS NOT NULL
  AND Value > 0;


--- REMOVE DUPLICATES (HEART RATE)

CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.heartrate_deduped` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY user_id, datetime
           ORDER BY heart_rate DESC
         ) AS row_num
  FROM `bold-momentum-471601-r3.bellabeat_dataset.heartrate_cleaned`
)
WHERE row_num = 1;


--- AGGREGATE HEART RATE (DAILY)

CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.heartrate_daily` AS
SELECT
  user_id,
  DATE(datetime) AS activity_date,
  AVG(heart_rate) AS avg_heart_rate
FROM `bold-momentum-471601-r3.bellabeat_dataset.heartrate_deduped`
GROUP BY user_id, activity_date;


-- CLEAN STEPS DATA

CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.steps_cleaned` AS
SELECT
  CAST(Id AS INT64) AS user_id,
  PARSE_DATE('%m/%d/%Y', ActivityDay) AS activity_date,
  CAST(StepTotal AS INT64) AS steps
FROM `bold-momentum-471601-r3.bellabeat_dataset.steps_raw`
WHERE StepTotal IS NOT NULL;



-- CLEAN SLEEP DATA

CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.sleep_cleaned` AS
SELECT
  CAST(Id AS INT64) AS user_id,
  DATE(SleepDay) AS sleep_date,
  CAST(TotalMinutesAsleep AS INT64) AS minutes_asleep,
  CAST(TotalTimeInBed AS INT64) AS time_in_bed
FROM `bold-momentum-471601-r3.bellabeat_dataset.sleep_raw`
WHERE TotalMinutesAsleep IS NOT NULL;



-- TRANSFORM STEPS

CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.steps_transformed` AS
SELECT
  *,
  CASE
    WHEN steps < 5000 THEN 'Sedentary'
    WHEN steps BETWEEN 5000 AND 9999 THEN 'Moderately Active'
    ELSE 'Active'
  END AS activity_level
FROM `bold-momentum-471601-r3.bellabeat_dataset.steps_cleaned`;



-- CREATE MASTER TABLE

CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.master_data` AS
SELECT
  s.user_id,
  s.activity_date,

  s.steps,
  s.activity_level,

  COALESCE(sl.minutes_asleep, 0) AS minutes_asleep,
  COALESCE(sl.time_in_bed, 0) AS time_in_bed,

  COALESCE(hr.avg_heart_rate, 0) AS avg_heart_rate

FROM `bold-momentum-471601-r3.bellabeat_dataset.steps_transformed` s

LEFT JOIN `bold-momentum-471601-r3.bellabeat_dataset.sleep_cleaned` sl
  ON s.user_id = sl.user_id
  AND s.activity_date = sl.sleep_date

LEFT JOIN `bold-momentum-471601-r3.bellabeat_dataset.heartrate_daily` hr
  ON s.user_id = hr.user_id
  AND s.activity_date = hr.activity_date;



-- VALIDATING DATA


-- Total rows
SELECT COUNT(*) AS total_rows
FROM `bold-momentum-471601-r3.bellabeat_dataset.master_data`;

-- Check for NULL steps
SELECT COUNT(*) AS null_steps
FROM `bold-momentum-471601-r3.bellabeat_dataset.master_data`
WHERE steps IS NULL;

-- Summary statistics
SELECT
  AVG(steps) AS avg_steps,
  AVG(minutes_asleep) AS avg_sleep,
  AVG(avg_heart_rate) AS avg_hr
FROM `bold-momentum-471601-r3.bellabeat_dataset.master_data`;
