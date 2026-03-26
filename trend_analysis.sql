CREATE OR REPLACE TABLE `bold-momentum-471601-r3.bellabeat_dataset.master_daily_summary` AS

WITH 

aggregated_heartRate AS (
  SELECT
    Id,
    DATE(Time) AS ActivityDay,
    AVG(DailyValue) AS AvgHeartRate,
    MIN(DailyValue) AS MinHeartRate,
    MAX(DailyValue) AS MaxHeartRate
  FROM `bold-momentum-471601-r3.bellabeat_dataset.summary_heartrate_seconds`
  GROUP BY Id, ActivityDay
),
aggregated_weight AS (
  SELECT
    Id,
    CAST(Date AS DATE) AS ActivityDay,
    AVG(WeightKg) AS AvgWeightKg,
    AVG(Fat) AS AvgFat,
    AVG(BMI) AS AvgBMI
  FROM `bold-momentum-471601-r3.bellabeat_dataset.weightLogInfo_merged`
  GROUP BY Id, ActivityDay
)
SELECT
  s.Id,
  s.ActivityDay,
  s.DailySteps,
  s.DailyCalories,
  s.DailySedentaryMinutes,
  s.DailyLightlyActiveMinutes,
  s.DailyFairlyActiveMinutes,
  s.DailyVeryActiveMinutes,
  s.DailySedentaryActiveDistance,
  s.DailyLightActiveDistance,
  s.DailyModeratelyActiveDistance,
  s.DailyVeryActiveDistance,
  COALESCE(ds.DailyTotalSleepRecords, 0) AS DailyTotalSleepRecords,
  COALESCE(ds.DailyTotalMinutesAsleep, 0) AS DailyTotalMinutesAsleep,
  COALESCE(ds.DailyTotalTimeInBed, 0) AS DailyTotalTimeInBed,
  hr.AvgHeartRate,
  hr.MinHeartRate,
  hr.MaxHeartRate,
  COALESCE(w.AvgWeightKg, 0) AS AvgWeightKg,
  COALESCE(w.AvgFat, 0) AS AvgFat,
  COALESCE(w.AvgBMI, 0) AS AvgBMI
FROM `bold-momentum-471601-r3.bellabeat_dataset.summary_steps_calories_intensity_daily` s

-- Join dailySleep (cast SleepDay to DATE and rename as ActivityDay)
LEFT JOIN (
  SELECT
    Id,
    CAST(SleepDay AS DATE) AS ActivityDay,
    DailyTotalSleepRecords,
    DailyTotalMinutesAsleep,
    DailyTotalTimeInBed
  FROM `bold-momentum-471601-r3.bellabeat_dataset.summary_sleepDay`
) ds
USING(Id, ActivityDay)

LEFT JOIN aggregated_heartRate hr
  USING(Id, ActivityDay)

LEFT JOIN aggregated_weight w
  USING(Id, ActivityDay);