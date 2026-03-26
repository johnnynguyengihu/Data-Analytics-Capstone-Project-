# Agreggate the final daily summary table
CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.master_daily_summary` AS

WITH 

-- Aggregate heart rate to daily level
aggregated_heartRate AS (
  SELECT
    Id,
    DATE(Time) AS ActivityDay,
    ROUND(AVG(DailyValue), 2) AS AvgHeartRate,
    MIN(DailyValue) AS MinHeartRate,
    MAX(DailyValue) AS MaxHeartRate
  FROM `bold-momentum-471601-r3.bellabeat_dataset.summary_heartrate_seconds`
  GROUP BY Id, ActivityDay
),

-- Aggregate weight to daily level
aggregated_weight AS (
  SELECT
    Id,
    CAST(Date AS DATE) AS ActivityDay,
    ROUND(AVG(WeightKg), 2) AS AvgWeightKg,
    ROUND(AVG(Fat), 2) AS AvgFat,
    ROUND(AVG(BMI), 2) AS AvgBMI
  FROM `bold-momentum-471601-r3.bellabeat_dataset.weightLogInfo_merged`
  GROUP BY Id, ActivityDay
)

-- Final master table
SELECT
  s.Id,
  s.ActivityDay,
  s.TotalSteps,
  s.TotalCalories,
  s.DailyTotalIntensity,
  s.DailyAverageIntensity,
  s.DailySedentaryMinutes,
  s.DailyLightlyActiveMinutes,
  s.DailyFairlyActiveMinutes,
  s.DailyVeryActiveMinutes,
  s.DailySedentaryActiveDistance,
  s.DailyLightActiveDistance,
  s.DailyModeratelyActiveDistance,
  s.DailyVeryActiveDistance,

  -- Sleep (handle missing values)
  COALESCE(ds.DailyTotalSleepRecords, 0) AS DailyTotalSleepRecords,
  COALESCE(ds.DailyTotalMinutesAsleep, 0) AS DailyTotalMinutesAsleep,
  COALESCE(ds.DailyTotalTimeInBed, 0) AS DailyTotalTimeInBed,

  -- Heart rate
  hr.AvgHeartRate,
  hr.MinHeartRate,
  hr.MaxHeartRate,

  -- Weight (leave NULL if missing)
  w.AvgWeightKg,
  w.AvgFat,
  w.AvgBMI

FROM `bold-momentum-471601-r3.bellabeat_dataset.daily_summary` s

-- Join daily sleep (cast SleepDay → ActivityDay)
LEFT JOIN (
  SELECT
    Id,
    CAST(SleepDay AS DATE) AS ActivityDay,
    DailyTotalSleepRecords,
    DailyTotalMinutesAsleep,
    DailyTotalTimeInBed
  FROM `bold-momentum-471601-r3.bellabeat_dataset.summary_dailySleep`
) ds
USING (Id, ActivityDay)

-- Join heart rate
LEFT JOIN aggregated_heartRate hr
USING (Id, ActivityDay)

-- Join weight
LEFT JOIN aggregated_weight w
USING (Id, ActivityDay);
