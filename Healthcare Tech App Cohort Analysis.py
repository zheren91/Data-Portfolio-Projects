# Databricks notebook source
from pyspark.sql.functions import from_json, col, round, when, sum, expr, date_sub, current_date, lit, first, mode, count,date_trunc, lead, to_date, mode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType, MapType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

# problematic sourceId excluded (possible test user and patients with changed prescription)
# all of these sourceId had very few rows of data

NotIn = ['00u6wlfbcqcocsoua417_0oaswb00iAZUz63zh416',
         '00u1bfafhuE3RzYw6417_0oaswb00iAZUz63zh416',
         '00u3egs7e9y3dm31u417_0oaswb00iAZUz63zh416'
         ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compliance week categories
# MAGIC - Perfect Compliant Week: Correctly recorded all required vitals 7/7 days
# MAGIC - Fully Compliant Week: Correctly recorded all required vitals 5 or 6 days out of 7 days
# MAGIC - Partial Compliant Week: Correctly recorded all required vitals 1 to 4 days out of 7 days
# MAGIC - Low Compliant Week: No days were correctly recorded all vitals, but recorded some but not all required vitals for the day
# MAGIC - Non-Compliant Week: Did not record vitals at all for the week

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compliance definitions
# MAGIC - Scheduled compliant: Recorded vitals on a schedule
# MAGIC - Session compliant: Recorded all required vitals in a session (day)
# MAGIC - Partially session compliant: Only recorded some of the vitals in a session

# COMMAND ----------

cohort = spark.sql("""
    SELECT DISTINCT sourceId
    FROM mypd.capd_cohort
""").collect()
cohort_sourceIds = [str(row.sourceId) for row in cohort]

# COMMAND ----------

cohort_NA = spark.sql("""
    SELECT DISTINCT sourceId
    FROM mypd.capd_cohort
    WHERE region = 'US, Canada'
""").collect()

cohort_sourceIds_NA = [str(row.sourceId) for row in cohort_NA]

# COMMAND ----------

capd_MAU = spark.sql(f"""
SELECT
    DATE_FORMAT(t.treatmenttime, 'yyyy-MM') as Month,
    COUNT(DISTINCT CASE WHEN t.patientRegion = 'PRD-4' THEN t.sourceId END) as USCanada_Count,
    COUNT(DISTINCT CASE WHEN t.patientRegion = 'PRD-8' THEN t.sourceId END) as LatinAmerica_Count
FROM prodeu.mypd_treatment t
WHERE t.patientRegion IN ('PRD-4', 'PRD-8')
GROUP BY Month
""")  
display(capd_MAU)

# COMMAND ----------

capd_vitals_taken = spark.sql(f"""
    SELECT 
        date(e.payload.vital.timestamp) as VitalDate,
        e.payload.sourceId,
        e.payload.vital.id as VitalId,
        get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') as PatientType,
        e.payload.vital.type as VitalType,
        CASE WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN 'Latin America'
        END as Region
    FROM prodeu.events e
    --LEFT JOIN prodeu.mypd_prescription prescription  on prescription.sourceId = e.payload.sourceId
    WHERE e.type = 'vital-recorded'
    AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') = 'capd'
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    ORDER BY VitalDate DESC
""") 
display(capd_vitals_taken)

# COMMAND ----------

churned = spark.sql(f"""
select * from mypd.churned_and_reactivated_capd
""")
display(churned)

# COMMAND ----------

# number churned 

# COMMAND ----------

na_cohort = spark.sql("""
    SELECT *
    FROM mypd.capd_cohort
    WHERE region = 'US, Canada'
""")
display(na_cohort)

# COMMAND ----------

capd_daily_missed_updated = spark.sql(f"""
SELECT 
*
FROM mypd.capd_daily_missed_updated_connectiontypes_streak
where region != 'Unknown'
""")

display(capd_daily_missed_updated)

# Due to large data results, not all results are displayed (Databricks warning)

# COMMAND ----------



# COMMAND ----------

NA_capd_daily_missed_updated = spark.sql(f"""
SELECT 
*
FROM mypd.capd_daily_missed_updated_connectiontypes_streak
WHERE Region = 'US, Canada'
""")

display(NA_capd_daily_missed_updated)

# COMMAND ----------

LA_capd_daily_missed_updated = spark.sql(f"""
SELECT 
*
FROM mypd.capd_daily_missed_updated_connectiontypes_streak
WHERE Region = 'Latin America'
""")

display(LA_capd_daily_missed_updated)

# COMMAND ----------

# combined logs
combined_compliant_logs = spark.sql(f"""
    SELECT
    c.sourceid,
    c.region,
    c.Status,
    COUNT(distinct VitalDate) FILTER(WHERE name = 'bluetooth_reading_error') as reading_error_day,
    COUNT(distinct VitalDate) FILTER(WHERE name = 'bluetooth_reading_initiated') as reading_initiated_day,
    reading_initiated_day - reading_error_day as no_reading_error_day
    FROM mypd.capd_daily_missed_updated_connectiontypes_streak c
    LEFT JOIN (
    SELECT *
    FROM prodeu.mypd_btlog_cleaned  
    WHERE Name in ('bluetooth_reading_error', 'bluetooth_reading_initiated')
    ) b
    ON c.sourceId = b.sourceId AND c.VitalDate = b.`timestamp`
    WHERE Status in ('Correctly Recorded All Vitals','Only Recorded Some Vitals')
    AND region = 'US, Canada'
    GROUP BY c.sourceId,c.region, c.status
""")
display(combined_compliant_logs)

# COMMAND ----------

# partially compliant bt logs left join
# count number of days with error and the number of days without error

partially_compliant_logs = spark.sql(f"""
    SELECT
    c.sourceid,
    c.region,
    COUNT(distinct VitalDate) FILTER(WHERE name = 'bluetooth_reading_error') as reading_error_day,
    COUNT(distinct VitalDate) FILTER(WHERE name = 'bluetooth_reading_initiated') as reading_initiated_day,
    reading_initiated_day - reading_error_day as no_reading_error_day
    FROM mypd.capd_daily_missed_updated_connectiontypes_streak c
    LEFT JOIN (
    SELECT *
    FROM prodeu.mypd_btlog_cleaned  
    WHERE Name in ('bluetooth_reading_error', 'bluetooth_reading_initiated')
    ) b
    ON c.sourceId = b.sourceId AND c.VitalDate = b.`timestamp`
    WHERE Status = 'Only Recorded Some Vitals'
    AND region = 'US, Canada'
    GROUP BY c.sourceId,c.region
""")
display(partially_compliant_logs)


# COMMAND ----------

# Convert the DataFrame to a Pandas DataFrame
partially_compliant_logs_counts_pd = partially_compliant_logs.select(
    'sourceId',
    'reading_error_day',
    'no_reading_error_day'
).toPandas()

# Calculate the total sum of all compliance week categories
total_sum = partially_compliant_logs_counts_pd[['reading_error_day', 'no_reading_error_day']].sum().sum()

# Prepare the data for the pie chart
categories = ['Session with reading error', 'Session with no reading error']
counts = partially_compliant_logs_counts_pd[['reading_error_day', 'no_reading_error_day']].sum()

# Calculate the proportions
proportions = counts / total_sum

# Create the pie chart
plt.figure(figsize=(6, 6))
plt.pie(proportions, labels=categories, autopct='%1.1f%%', startangle=140)

# Set the aspect ratio to be equal to ensure a circular pie chart
plt.axis('equal')

# Add a title to the chart
plt.title('Non Compliant Sessions and Bluetooth Reading Error')

# Show the chart
plt.show()

# COMMAND ----------



# COMMAND ----------

# perfect compliant bt logs left join
# each error within a session = 1 error

perfect_compliant_logs = spark.sql(f"""
    SELECT
    c.sourceid,
    c.region,
    COUNT(distinct VitalDate) FILTER(WHERE name = 'bluetooth_reading_error') as reading_error_day,
    COUNT(distinct VitalDate) FILTER(WHERE name = 'bluetooth_reading_initiated') as reading_initiated_day,
    reading_initiated_day - reading_error_day as no_reading_error_day
    FROM mypd.capd_daily_missed_updated_connectiontypes_streak c
    LEFT JOIN (
    SELECT *
    FROM prodeu.mypd_btlog_cleaned  
    WHERE Name in ('bluetooth_reading_error', 'bluetooth_reading_initiated')
    ) b
    ON c.sourceId = b.sourceId AND c.VitalDate = b.`timestamp`
    WHERE Status = 'Correctly Recorded All Vitals'
    AND region = 'US, Canada'
    GROUP BY c.sourceId,c.region
""")
display(perfect_compliant_logs)

# COMMAND ----------

# Convert the DataFrame to a Pandas DataFrame
perfect_compliant_logs_counts_pd = perfect_compliant_logs.select(
    'sourceId',
    'reading_error_day',
    'no_reading_error_day'
).toPandas()

# Calculate the total sum of all compliance week categories
total_sum = perfect_compliant_logs_counts_pd[['reading_error_day', 'no_reading_error_day']].sum().sum()

# Prepare the data for the pie chart
categories = ['Session with reading error', 'Session with no reading error']
counts = perfect_compliant_logs_counts_pd[['reading_error_day', 'no_reading_error_day']].sum()

# Calculate the proportions
proportions = counts / total_sum

# Create the pie chart
plt.figure(figsize=(6, 6))
plt.pie(proportions, labels=categories, autopct='%1.1f%%', startangle=140)

# Set the aspect ratio to be equal to ensure a circular pie chart
plt.axis('equal')

# Add a title to the chart
plt.title('Session Compliant and Bluetooth Reading Error')

# Show the chart
plt.show()

# COMMAND ----------

#

# COMMAND ----------

# partially compliant bt logs left join

partially_compliant_analysis = spark.sql(f"""
    SELECT 
        distinct c.sourceId,
        region,
        COUNT(CASE WHEN Name = 'bluetooth_reading_error' THEN 1 END) as Reading_error_count,
        COUNT(CASE WHEN ConnectionType = 'Manual' THEN 1 END) as manual_count,
        COUNT(CASE WHEN ConnectionType = 'Bluetooth' THEN 1 END) as bluetooth_count,
        COUNT(CASE WHEN TotalSupposed_BP > TotalTaken_BP AND TotalTaken_BP = 0 THEN 1 END) as Completely_Missed_BP,
        COUNT(CASE WHEN TotalSupposed_BP > TotalTaken_BP AND TotalTaken_BP != 0 THEN 1 END) as Missed_Some_BP,
        COUNT(CASE WHEN TotalSupposed_Pulse > TotalTaken_Pulse AND TotalTaken_Pulse = 0 THEN 1 END) as Completely_Missed_Pulse,
        COUNT(CASE WHEN TotalSupposed_Pulse > TotalTaken_Pulse AND TotalTaken_Pulse != 0 THEN 1 END) as Missed_Some_Pulse,
        COUNT(CASE WHEN TotalSupposed_Glucose > TotalTaken_Glucose AND TotalTaken_Glucose = 0 THEN 1 END) as Completely_Missed_Glucose,
        COUNT(CASE WHEN TotalSupposed_Glucose > TotalTaken_Glucose AND TotalTaken_Glucose != 0 THEN 1 END) as Missed_Some_Glucose,
        COUNT(CASE WHEN TotalSupposed_Temp > TotalTaken_Temp AND TotalTaken_Temp = 0 THEN 1 END) as Completely_Missed_Temp,
        COUNT(CASE WHEN TotalSupposed_Temp > TotalTaken_Temp AND TotalTaken_Temp != 0 THEN 1 END) as Missed_Some_Temp,
        COUNT(CASE WHEN TotalSupposed_Weight > TotalTaken_Weight AND TotalTaken_Weight = 0 THEN 1 END) as Completely_Missed_Weight,
        COUNT(CASE WHEN TotalSupposed_Weight> TotalTaken_Weight AND TotalTaken_Weight != 0 THEN 1 END) as Missed_Some_Weight,
        Completely_Missed_BP + Completely_Missed_Pulse + Completely_Missed_Glucose + Completely_Missed_Temp + Completely_Missed_Weight as Total_Completly_Missed_Vital,
        Missed_Some_BP + Missed_Some_Pulse + Missed_Some_Glucose + Missed_Some_Temp + Missed_Some_Weight as Total_Missed_Some_Vital
    FROM mypd.capd_daily_missed_updated_connectiontypes_streak c
    LEFT JOIN (
    SELECT *
    FROM prodeu.mypd_btlog_cleaned  
    WHERE Name in ('bluetooth_reading_error', 'bluetooth_reading_initiated')
    ) b
    ON c.sourceId = b.sourceId AND c.VitalDate = b.`timestamp`
    WHERE Status = 'Only Recorded Some Vitals'
    AND region = 'US, Canada'
    GROUP BY c.sourceId, region
""")
display(partially_compliant_analysis)


# COMMAND ----------



# COMMAND ----------

partially_compliant_logs = spark.sql("""
    SELECT 
        *,
        CASE WHEN TotalSupposed_BP > TotalTaken_BP AND TotalTaken_BP = 0 THEN 1 ELSE 0 END as Completely_Missed_BP,
        CASE WHEN TotalSupposed_BP > TotalTaken_BP AND TotalTaken_BP != 0 THEN 1 ELSE 0 END as Missed_Some_BP,
        CASE WHEN TotalSupposed_Pulse > TotalTaken_Pulse AND TotalTaken_Pulse = 0 THEN 1 ELSE 0 END as Completely_Missed_Pulse,
        CASE WHEN TotalSupposed_Pulse > TotalTaken_Pulse AND TotalTaken_Pulse != 0 THEN 1 ELSE 0 END as Missed_Some_Pulse,
        CASE WHEN TotalSupposed_Glucose > TotalTaken_Glucose AND TotalTaken_Glucose = 0 THEN 1 ELSE 0 END as Completely_Missed_Glucose,
        CASE WHEN TotalSupposed_Glucose > TotalTaken_Glucose AND TotalTaken_Glucose != 0 THEN 1 ELSE 0 END as Missed_Some_Glucose,
        CASE WHEN TotalSupposed_Temp > TotalTaken_Temp AND TotalTaken_Temp = 0 THEN 1 ELSE 0 END as Completely_Missed_Temp,
        CASE WHEN TotalSupposed_Temp > TotalTaken_Temp AND TotalTaken_Temp != 0 THEN 1 ELSE 0 END as Missed_Some_Temp,
        CASE WHEN TotalSupposed_Weight > TotalTaken_Weight AND TotalTaken_Weight = 0 THEN 1 ELSE 0 END as Completely_Missed_Weight,
        CASE WHEN TotalSupposed_Weight > TotalTaken_Weight AND TotalTaken_Weight != 0 THEN 1 ELSE 0 END as Missed_Some_Weight,
        CASE WHEN (TotalSupposed_BP > TotalTaken_BP AND TotalTaken_BP = 0) OR
                  (TotalSupposed_Pulse > TotalTaken_Pulse AND TotalTaken_Pulse = 0) OR
                  (TotalSupposed_Glucose > TotalTaken_Glucose AND TotalTaken_Glucose = 0) OR
                  (TotalSupposed_Temp > TotalTaken_Temp AND TotalTaken_Temp = 0) OR
                  (TotalSupposed_Weight > TotalTaken_Weight AND TotalTaken_Weight = 0)
             THEN 1 ELSE 0 END as Total_Completely_Missed_Vital,
        CASE WHEN (TotalSupposed_BP > TotalTaken_BP AND TotalTaken_BP != 0) OR
                  (TotalSupposed_Pulse > TotalTaken_Pulse AND TotalTaken_Pulse != 0) OR
                  (TotalSupposed_Glucose > TotalTaken_Glucose AND TotalTaken_Glucose != 0) OR
                  (TotalSupposed_Temp > TotalTaken_Temp AND TotalTaken_Temp != 0) OR
                  (TotalSupposed_Weight > TotalTaken_Weight AND TotalTaken_Weight != 0)
             THEN 1 ELSE 0 END as Total_Missed_Some_Vital
    FROM mypd.capd_daily_missed_updated_connectiontypes_streak c
    LEFT JOIN (
        SELECT *
        FROM prodeu.mypd_btlog_cleaned  
        WHERE Name in ('bluetooth_reading_error', 'bluetooth_reading_initiated')
    ) b
    ON c.sourceId = b.sourceId AND c.VitalDate = b.`timestamp`
    WHERE Status = 'Only Recorded Some Vitals'
    AND Region = 'US, Canada'
""")
display(partially_compliant_logs)


# COMMAND ----------



# COMMAND ----------

# Session compliance analysis
# combined
session_compliance = spark.sql(f"""
SELECT 
*
FROM mypd.capd_daily_missed_updated_connectiontypes_streak
WHERE Status not in ('Did not Record Vital At All')
AND region != 'Unknown'
""")
display(session_compliance)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from prodeu.mypd_btlog_cleaned

# COMMAND ----------

# session compliance vs bluetooth read error
perfect_session_compliance = spark.sql(f"""
SELECT 
*
FROM mypd.capd_daily_missed_updated_connectiontypes_streak 
WHERE Status = 'Correctly Recorded All Vitals'

""")
display(perfect_session_compliance)

# COMMAND ----------

# partial session compliance vs bluetooth read error
partial_session_compliance = spark.sql(f"""
SELECT 
*
FROM mypd.capd_daily_missed_updated_connectiontypes_streak
WHERE Status = 'Only Recorded Some Vitals'
and region != 'Unknown'
""")
display(partial_session_compliance)

# COMMAND ----------

# Session compliance analysis
session_compliance_days = spark.sql(f"""
SELECT 
Region,
count(VitalDate) filter(WHERE status = 'Correctly Recorded All Vitals') as FullyCompliantDays,
count(VitalDate) filter(WHERE status = 'Only Recorded Some Vitals') as PartiallyCompliantDays
FROM mypd.capd_daily_missed_updated_connectiontypes_streak
WHERE Status not in ('Did not Record Vital At All')
and region != 'Unknown'
GROUP BY Region
""")
display(session_compliance_days)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from mypd.capd_daily_missed_updated_connectiontypes_streak
# MAGIC where sourceId = '00u19whqfaVnQ5yh3417_0oaswb00iAZUz63zh416'

# COMMAND ----------

NA_schedule_compliance_cohort = spark.sql(f"""
   SELECT 
   *,
    coalesce(FullyCompliantWeekCount, 0) + coalesce(PerfectCompliantWeekCount, 0) as FullyCompliantWeekCombined,
    YearWeekCount - (coalesce(FullyCompliantWeekCount, 0) + coalesce(PerfectCompliantWeekCount, 0) + coalesce(PartialCompliantWeekCount, 0) + coalesce(NonCompliantWeekCount, 0)) as LowCompliantWeeks
   FROM mypd.capd_cohort 
   WHERE region = 'US, Canada'                                   
""")
display(NA_schedule_compliance_cohort)

LA_schedule_compliance_cohort = spark.sql(f"""
   SELECT 
   *,
    coalesce(FullyCompliantWeekCount, 0) + coalesce(PerfectCompliantWeekCount, 0) as FullyCompliantWeekCombined,
    YearWeekCount - (coalesce(FullyCompliantWeekCount, 0) + coalesce(PerfectCompliantWeekCount, 0) + coalesce(PartialCompliantWeekCount, 0) + coalesce(NonCompliantWeekCount, 0)) as LowCompliantWeeks

   FROM mypd.capd_cohort   
   WHERE region = 'Latin America'                                  
""")
display(LA_schedule_compliance_cohort)

# this compliance category does not account for instances where a patient entered some of the vitals 
# since the patient (1. did not have a single day where they correctly entered all vitals, and 2, have some vitals entered)
# this category would be the total number of weeks - all the categories = weeks where only partial vitals are entered (only entered some vitas + some days with missing vitals)

# COMMAND ----------

from pyspark.sql.functions import col, datediff

cohort_analysis = spark.sql("""
    SELECT *, 
        CASE
            WHEN ComplianceRate > 90 THEN 'Very Compliant'
            WHEN ComplianceRate > 60 THEN 'Compliant'
            WHEN ComplianceRate > 30 THEN 'Seldom Compliant'
            WHEN ComplianceRate >= 0 THEN 'Non-Compliant'
            ELSE 'Invalid Compliance Rate'
        END AS ComplianceCategory,
        coalesce(FullyCompliantWeekCount, 0) + coalesce(PerfectCompliantWeekCount, 0) as FullyCompliantWeekCombined,
        round(datediff(CurrentDate, StartingDate) / 7,0) as NumberOfWeeks
    FROM mypd.capd_cohort 
    WHERE region = 'US, Canada'
""")

display(cohort_analysis)

# COMMAND ----------

from pyspark.sql.functions import col, datediff

cohort_analysis = spark.sql("""
    SELECT *, 
        CASE
            WHEN ComplianceRate > 90 THEN 'Very Compliant'
            WHEN ComplianceRate > 60 THEN 'Compliant'
            WHEN ComplianceRate > 30 THEN 'Seldom Compliant'
            WHEN ComplianceRate >= 0 THEN 'Non-Compliant'
            ELSE 'Invalid Compliance Rate'
        END AS ComplianceCategory,
        coalesce(FullyCompliantWeekCount, 0) + coalesce(PerfectCompliantWeekCount, 0) as FullyCompliantWeekCombined,
        round(datediff(CurrentDate, StartingDate) / 7,0) as NumberOfWeeks
    FROM mypd.capd_cohort 
    WHERE region != 'Unknown'
""")

display(cohort_analysis)


# COMMAND ----------

# compliance scatter


cohort_analysis_scatter = spark.sql("""
    SELECT *

    FROM mypd.capd_cohort 
    WHERE num_day_reading_error is not null
    
""")

display(cohort_analysis_scatter)

# COMMAND ----------



# Convert the DataFrame to a Pandas DataFrame
compliance_week_counts_pd = NA_schedule_compliance_cohort.select(
    'sourceId',
    'PerfectCompliantWeekCount',
    'FullyCompliantWeekCount',
    'PartialCompliantWeekCount',
    'NonCompliantWeekCount',
    'LowCompliantWeeks'
).toPandas()

# Calculate the total sum of all compliance week categories
total_sum = compliance_week_counts_pd[['PerfectCompliantWeekCount', 'FullyCompliantWeekCount', 'PartialCompliantWeekCount', 'NonCompliantWeekCount', 'LowCompliantWeeks']].sum().sum()

# Prepare the data for the pie chart
categories = ['Perfect Compliant Week', 'Fully Compliant Week', 'Partial Compliant Week', 'Non-Compliant Week','Low Compliant Week']
counts = compliance_week_counts_pd[['PerfectCompliantWeekCount', 'FullyCompliantWeekCount', 'PartialCompliantWeekCount', 'NonCompliantWeekCount','LowCompliantWeeks']].sum()

# Calculate the proportions
proportions = counts / total_sum

# Create the pie chart
plt.figure(figsize=(6, 6))
plt.pie(proportions, labels=categories, autopct='%1.1f%%', startangle=120)

# Set the aspect ratio to be equal to ensure a circular pie chart
plt.axis('equal')

# Add a title to the chart
plt.title('US Canada Compliance Week Proportions')

# Show the chart
plt.show()


# COMMAND ----------



# Convert the DataFrame to a Pandas DataFrame
compliance_week_counts_pd = LA_schedule_compliance_cohort.select(
    'sourceId',
    'PerfectCompliantWeekCount',
    'FullyCompliantWeekCount',
    'PartialCompliantWeekCount',
    'NonCompliantWeekCount',
    'LowCompliantWeeks'
).toPandas()

# Calculate the total sum of all compliance week categories
total_sum = compliance_week_counts_pd[['PerfectCompliantWeekCount', 'FullyCompliantWeekCount', 'PartialCompliantWeekCount', 'NonCompliantWeekCount', 'LowCompliantWeeks']].sum().sum()

# Prepare the data for the pie chart
categories = ['Perfect Compliant Week', 'Fully Compliant Week', 'Partial Compliant Week', 'Non-Compliant Week','Low Compliant Week']
counts = compliance_week_counts_pd[['PerfectCompliantWeekCount', 'FullyCompliantWeekCount', 'PartialCompliantWeekCount', 'NonCompliantWeekCount','LowCompliantWeeks']].sum()

# Calculate the proportions
proportions = counts / total_sum

# Create the pie chart
plt.figure(figsize=(6, 6))
plt.pie(proportions, labels=categories, autopct='%1.1f%%', startangle=10)

# Set the aspect ratio to be equal to ensure a circular pie chart
plt.axis('equal')

# Add a title to the chart
plt.title('Latin America Compliance Week Proportions')

# Show the chart
plt.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mypd.capd_cohort
# MAGIC where region != 'Unknown'

# COMMAND ----------

bt_reading_session_error = spark.sql(f"""
    select
        sourceId,
        region,
        entry_pattern,
        total_num_entered,
        num_day_reading_error,
        num_day_reading_initiated - num_day_reading_error as num_day_no_error,
        num_day_reading_initiated,
        reading_error_day_rate,
        avg_bt_tries_count,
        max_count
    from mypd.capd_cohort
    WHERE region != 'Unknown'
""")
display(bt_reading_session_error)

# cell value: average bt reading error rate in a session


# COMMAND ----------

bt_reading_session_error = spark.sql(f"""
    select
        sourceId,
        region,
        entry_pattern,
        total_num_entered,
        num_day_reading_error,
        num_day_reading_initiated - num_day_reading_error as num_day_no_error,
        num_day_reading_initiated,
        reading_error_day_rate,
        avg_bt_tries_count,
        max_count
    from mypd.capd_cohort
    WHERE region = 'US, Canada'
""")
display(bt_reading_session_error)

# COMMAND ----------



# Convert the DataFrame to a Pandas DataFrame
bt_reading_session_error_counts_pd = bt_reading_session_error.select(
    'sourceId',
    'num_day_reading_error',
    'num_day_no_error',
    'num_day_reading_initiated'
).toPandas()

# Calculate the total sum of all compliance week categories
total_sum = bt_reading_session_error_counts_pd[['num_day_reading_error', 'num_day_no_error']].sum().sum()

# Prepare the data for the pie chart
categories = ['Session with reading error', 'Session with no reading error']
counts = bt_reading_session_error_counts_pd[['num_day_reading_error', 'num_day_no_error']].sum()

# Calculate the proportions
proportions = counts / total_sum

# Create the pie chart
plt.figure(figsize=(6, 6))
plt.pie(proportions, labels=categories, autopct='%1.1f%%', startangle=140)

# Set the aspect ratio to be equal to ensure a circular pie chart
plt.axis('equal')

# Add a title to the chart
plt.title('Session bluetooth reading error vs no bluetooth reading error')

# Show the chart
plt.show()


# COMMAND ----------

# heavy manual, and experienced a lot of bt reading error
# these users are all from Latin America, it seems like they are getting BT errors 
bt_reading_session_error_heavy_manual = spark.sql(f"""
    select
        sourceId,
        region,
        entry_pattern,
        total_num_entered,
        num_day_reading_error,
        num_day_reading_initiated - num_day_reading_error as num_day_no_error,
        num_day_reading_initiated,
        reading_error_day_rate,
        avg_bt_tries_count,
        max_count
    from mypd.capd_cohort
    WHERE entry_pattern = 'Heavy Manual'
    AND num_day_reading_error is not null
    and num_day_reading_error != 0
""")
display(bt_reading_session_error_heavy_manual)

# COMMAND ----------

#

# COMMAND ----------

capd_union = spark.sql(f"""
   SELECT * 
   FROM mypd.capd_union                    
                       
""")
#display(capd_union)

windowSpec = Window.partitionBy("sourceId", date_trunc("date", col("event_timestamp"))).orderBy("event_timestamp")

# Create columns with the next row's data for the same sourceId
capd_union_with_next_row = capd_union.withColumn("next_sourceId", lead(col("sourceId")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_ConnectionType", lead(col("ConnectionType")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_two_ConnectionType", lead(col("ConnectionType"), 2).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_three_ConnectionType", lead(col("ConnectionType"), 3).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_four_ConnectionType", lead(col("ConnectionType"), 4).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_five_ConnectionType", lead(col("ConnectionType"), 5).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_six_ConnectionType", lead(col("ConnectionType"), 6).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_seven_ConnectionType", lead(col("ConnectionType"), 7).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_eight_ConnectionType", lead(col("ConnectionType"), 8).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_VitalType", lead(col("VitalType")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_Region", lead(col("Region")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_event_timestamp", lead(col("event_timestamp")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_enight_event_timestamp", lead(col("event_timestamp"),8).over(windowSpec))
# Filter rows where ConnectionType is 'bluetooth_reading_error'
capd_bt_next_row = capd_union_with_next_row.filter(col("ConnectionType") == "bluetooth_reading_error")


capd_bt_next_row = capd_bt_next_row.select(
    'sourceId',
    'ConnectionType',
    'next_ConnectionType',
    'next_two_ConnectionType',
    'next_three_ConnectionType',
    'next_four_ConnectionType',
    'next_five_ConnectionType',
    'next_six_ConnectionType',
    'next_seven_ConnectionType',
    'next_eight_ConnectionType',
    'next_event_timestamp',
    'next_enight_event_timestamp'
)

# Display the resulting DataFrame
#display(capd_bt_next_row)

#-----------------------------------------------------------------------------------------------------------------------------------------

from pyspark.sql.functions import col, to_date, lag
from pyspark.sql.window import Window

# Convert event_timestamp to date type
capd_union = capd_union.withColumn("date", to_date(col("event_timestamp").cast("timestamp")))

# Define a window specification to order the rows by event_timestamp
windowSpec = Window.partitionBy("sourceId").orderBy("event_timestamp")

# Add the previous row's ConnectionType column
capd_union = capd_union.withColumn("prev_ConnectionType", lag(col("ConnectionType")).over(windowSpec))
capd_union = capd_union.withColumn("next_ConnectionType", lead(col("ConnectionType")).over(windowSpec))

# Filter rows with 'bluetooth_reading_initiated' and the next row's ConnectionType is not 'bluetooth_reading_initiated' or 'bluetooth_reading_error'
# previous row = error, current row = initiate, next row != initiated or error
# how many times some one retries after getting error in a day
# this average is only the average of the days when they encounter a reading error, and how many times they tried
# this average will be always less than the total number of reading error encountered
# prev = error, next = error, != initiate, count how many tries

count_per_date = capd_union.filter((col("ConnectionType") == "bluetooth_reading_initiated") &
                                    (col("prev_ConnectionType") == "bluetooth_reading_error") &
                                    ((col("next_ConnectionType") != "bluetooth_reading_initiated") |
                                    (col("next_ConnectionType") == "bluetooth_reading_error")) 
                                    ) \
    .groupBy("date", "sourceId") \
    .count()

# Display the result
display(count_per_date)



# COMMAND ----------

streak_count = spark.sql(f"""
select 
    sourceid,
    region,
    entry_pattern,
    count(distinct sourceId) filter(where max_streak = 0 ) as No_streak,
    count(distinct sourceId) filter(where max_streak < 5 and max_streak > 1) as Less_than_five,
    count(distinct sourceId) filter(where max_streak >= 5 ) as Five_or_More,
    count(distinct sourceId) filter(where max_streak >= 10 ) as Ten_or_More,
    count(distinct sourceId) filter(where max_streak >= 20 ) as Twenty_or_More,
    count(distinct sourceId) filter(where max_streak >= 40 ) as Fourty_or_More,
    count(distinct sourceId) filter(where max_streak >= 80 ) as Eighty_or_More,
    count(distinct sourceId) filter(where max_streak >= 160 ) as One_sixty_or_More

from mypd.capd_cohort   
where region != 'Unknown'                   
group by sourceId, region, entry_pattern  
                         
""")

display(streak_count)

# COMMAND ----------

# Compliance style
# cross tab in categories x bt error rates, compliance rate ect.

# i dont care much style (sporatic compliance, gone and back gone and back)
# very compliant while active, and dropped off completely
# use it less and less over time and stop
# streaks + offline + streaks
# very compliant (at least a (2) few weeks), then burnout, then drop off to very low
# never really schedule compliant, but 




# COMMAND ----------

# filter for those who has experienced at least 10 reading errors
cohort_analysis_error_rate_filter = cohort_analysis.filter(col('reading_error')>10)
display(cohort_analysis_error_rate_filter)

# COMMAND ----------

cohort_analysisNA = spark.sql(f"""
   SELECT *
   FROM mypd.capd_cohort
   WHERE region = 'US, Canada'                           
                              
""")
display(cohort_analysisNA)

# COMMAND ----------

cohort_analysis_bt = cohort_analysisNA.filter(cohort_analysisNA.avg_bt_tries_count.isNotNull())

from pyspark.sql.functions import when
cohort_analysis_bt = cohort_analysis_bt.withColumn(
    'choice_after_reading_error',
    when(cohort_analysis_bt.choice_after_reading_error == 'bluetooth_reading_initiated', 'Retry Using Bluetooth')
    .otherwise(cohort_analysis_bt.choice_after_reading_error)
)

display(cohort_analysis_bt)

# COMMAND ----------

# last online? 
# cohort_analysis


# COMMAND ----------

# MAGIC %md
# MAGIC # Bluetooth connectivity logs
# MAGIC
# MAGIC - Limitations: Only recovered limited amount of bluetooth connectivity data
# MAGIC - Failed reading ratio is calculated using (number of failed reading) / (total number of reading attempts) * 100%
# MAGIC - Failed pairing ratio is calculated using (number of failed pairing) / (total number of pairing attempts) * 100%

# COMMAND ----------

# generate list of sourceId in cohort analysis
cohort = spark.sql("""
    SELECT DISTINCT sourceId
    FROM mypd.capd_cohort
""").collect()
cohort_sourceIds = [str(row.sourceId) for row in cohort]




# COMMAND ----------

cohort_NA = spark.sql("""
    SELECT DISTINCT sourceId
    FROM mypd.capd_cohort
    WHERE region = 'US, Canada'
""").collect()
cohort_sourceIds_NA = [str(row.sourceId) for row in cohort_NA]

# COMMAND ----------


# 40 / 69 patients have logs
# AND sourceId IN {format(tuple(cohort_sourceIds))}
# cohort_analysis last online
# last is first, first is last
# use asc, first

testt = spark.sql(f"""
            SELECT distinct sourceId,
            first(vitaldate) vitaldate

            FROM prodeu.mypd_capd
            WHERE sourceId IN {format(tuple(cohort_sourceIds))}
            group by sourceId
            order by vitaldate asc
    
""")

display(testt)


# COMMAND ----------



# COMMAND ----------

# reading error
btlog_reading_error = spark.sql(f"""
    SELECT 
    sourceId,
    data.devicename,
    --appOSType,
    data.error
    FROM prodeu.mypd_btlog_cleaned
    WHERE name = 'bluetooth_reading_error'
    AND sourceId IN {format(tuple(cohort_sourceIds_NA))}
    AND appOSType NOT IN('','web')

""")
display(btlog_reading_error)

# COMMAND ----------

# BT pairing error
btlog_pairing_error = spark.sql(f"""
    SELECT 
    sourceId,
    data.devicename,
    --appOSType,
    data.error
    FROM prodeu.mypd_btlog_cleaned
    WHERE name = 'bluetooth_pairing_error'
    AND appOSType NOT IN('','web')
    AND sourceId IN {format(tuple(cohort_sourceIds_NA))}

""")
display(btlog_pairing_error)

# COMMAND ----------

# reading error 
readingerror = spark.sql(f"""
    SELECT 
    count(distinct sourceId),
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_error') as reading_error,
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_initiated') as reading_initiated,
    round(COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_error')  / COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_initiated'),4) * 100 as reading_error_rate
    FROM prodeu.mypd_btlog_cleaned
    WHERE appOSType NOT IN ('', 'web')
    AND sourceId IN {format(tuple(cohort_sourceIds))}
""")
display(readingerror)


# COMMAND ----------

btlog_reading_success = spark.sql(f"""
    SELECT 
    sourceId,
    data.devicename
    --appOSType,
    --data.error
    FROM prodeu.mypd_btlog_cleaned
    WHERE name = 'bluetooth_reading_successful'
    AND appOSType NOT IN('','web')
    AND sourceId IN {format(tuple(cohort_sourceIds_NA))}

""")
display(btlog_reading_success)

# COMMAND ----------



# COMMAND ----------

# btlog error breakdown
btlog_breakdown = spark.sql(f"""
    SELECT 
    DISTINCT sourceId,
    SUM(CASE WHEN appOSType = 'android' THEN 1 ELSE 0 END) as Android_count,
    SUM(CASE WHEN appOSType = 'ios' THEN 1 ELSE 0 END) as IOS_count,
    SUM(CASE WHEN name = 'bluetooth_pairing_error' THEN 1 ELSE 0 END) AS count_bluetooth_pairing_error,
    SUM(CASE WHEN name = 'bluetooth_reading_error' THEN 1 ELSE 0 END) AS count_bluetooth_reading_error,
    SUM(CASE WHEN name = 'client_login_failed' THEN 1 ELSE 0 END) AS count_client_login_failed

    FROM prodeu.mypd_btlog_cleaned
    WHERE appOSType not in('', 'web')
    and sourceId != 'UNKNOWN'
    AND sourceId IN  {format(tuple(cohort_sourceIds_NA))}
    GROUP BY sourceId

""")

# COMMAND ----------

# btlog error breakdown
btlog_breakdown = spark.sql(f"""
    SELECT 
    DISTINCT sourceId,
    SUM(CASE WHEN appOSType = 'android' THEN 1 ELSE 0 END) as Android_count,
    SUM(CASE WHEN appOSType = 'ios' THEN 1 ELSE 0 END) as IOS_count,
    SUM(CASE WHEN name = 'bluetooth_pairing_error' THEN 1 ELSE 0 END) AS count_bluetooth_pairing_error,
    SUM(CASE WHEN name = 'bluetooth_reading_error' THEN 1 ELSE 0 END) AS count_bluetooth_reading_error,
    SUM(CASE WHEN name = 'client_login_failed' THEN 1 ELSE 0 END) AS count_client_login_failed

    FROM prodeu.mypd_btlog_cleaned
    WHERE appOSType not in('', 'web')
    and sourceId != 'UNKNOWN'
    AND sourceId IN  {format(tuple(cohort_sourceIds))}
    GROUP BY sourceId

""")


# Create a new column 'pairing_error' based on the 'count_bluetooth_pairing_error' column
btlog_breakdown = btlog_breakdown.withColumn(
    'pairing_error',
    when(btlog_breakdown['count_bluetooth_pairing_error'] > 0, 'Experienced BT Pairing Error')
    .otherwise('No error')
)
btlog_breakdown = btlog_breakdown.withColumn(
    'reading_error',
    when(btlog_breakdown['count_bluetooth_reading_error'] > 0, 'Experienced BT Reading Error')
    .otherwise('No error')
)
btlog_breakdown = btlog_breakdown.withColumn(
    'login_failed',
    when(btlog_breakdown['count_client_login_failed'] > 0, 'Client Login Failed')
    .otherwise('No error')
)


btlog_breakdown = btlog_breakdown.withColumn(
    'AppOS',
    when((btlog_breakdown['IOS_count'] > 0) & (btlog_breakdown['Android_count'] > 0), 'Use both IOS and Android')
    .when(btlog_breakdown['IOS_count'] > 0, 'IOS')
    .when(btlog_breakdown['Android_count'] > 0, 'Android')
    .otherwise('null')
)


display(btlog_breakdown)




# COMMAND ----------

# btlog error breakdown
btlog_breakdown = spark.sql(f"""
    SELECT 
    DISTINCT sourceId,
    SUM(CASE WHEN appOSType = 'android' THEN 1 ELSE 0 END) as Android_count,
    SUM(CASE WHEN appOSType = 'ios' THEN 1 ELSE 0 END) as IOS_count,
    SUM(CASE WHEN name = 'bluetooth_pairing_error' THEN 1 ELSE 0 END) AS count_bluetooth_pairing_error,
    SUM(CASE WHEN name = 'bluetooth_reading_error' THEN 1 ELSE 0 END) AS count_bluetooth_reading_error,
    SUM(CASE WHEN name = 'client_login_failed' THEN 1 ELSE 0 END) AS count_client_login_failed

    FROM prodeu.mypd_btlog_cleaned
    WHERE appOSType not in('', 'web')
    and sourceId != 'UNKNOWN'
    AND sourceId IN  {format(tuple(cohort_sourceIds_NA))}
    GROUP BY sourceId

""")


# Create a new column 'pairing_error' based on the 'count_bluetooth_pairing_error' column
btlog_breakdown = btlog_breakdown.withColumn(
    'pairing_error',
    when(btlog_breakdown['count_bluetooth_pairing_error'] > 0, 'Experienced BT Pairing Error')
    .otherwise('No error')
)
btlog_breakdown = btlog_breakdown.withColumn(
    'reading_error',
    when(btlog_breakdown['count_bluetooth_reading_error'] > 0, 'Experienced BT Reading Error')
    .otherwise('No error')
)
btlog_breakdown = btlog_breakdown.withColumn(
    'login_failed',
    when(btlog_breakdown['count_client_login_failed'] > 0, 'Client Login Failed')
    .otherwise('No error')
)


btlog_breakdown = btlog_breakdown.withColumn(
    'AppOS',
    when((btlog_breakdown['IOS_count'] > 0) & (btlog_breakdown['Android_count'] > 0), 'Use both IOS and Android')
    .when(btlog_breakdown['IOS_count'] > 0, 'IOS')
    .when(btlog_breakdown['Android_count'] > 0, 'Android')
    .otherwise('null')
)


display(btlog_breakdown)




# COMMAND ----------

# cleaned version, excluded empty and unknown

btlog_ratio = spark.sql(f"""
    SELECT
        b.sourceId,
        appOSType,
        appVersion,
        b.appOSVersion,
        SUM(CASE WHEN name = 'bluetooth_pairing_successful' THEN 1 ELSE 0 END) AS count_bluetooth_pairing_successful,
        SUM(CASE WHEN name = 'bluetooth_pairing_error' THEN 1 ELSE 0 END) AS count_bluetooth_pairing_error,
        SUM(CASE WHEN name = 'bluetooth_pairing_initiated' THEN 1 ELSE 0 END) AS count_bluetooth_pairing_initiated,
        SUM(CASE WHEN name = 'bluetooth_reading_successful' THEN 1 ELSE 0 END) AS count_bluetooth_reading_successful,
        SUM(CASE WHEN name = 'bluetooth_reading_error' THEN 1 ELSE 0 END) AS count_bluetooth_reading_error,
        SUM(CASE WHEN name = 'bluetooth_reading_initiated' THEN 1 ELSE 0 END) AS count_bluetooth_reading_initiated,
        SUM(CASE WHEN name = 'client_login_failed' THEN 1 ELSE 0 END) AS count_client_login_failed,
        SUM(CASE WHEN name = 'client_login_successful' THEN 1 ELSE 0 END) AS count_client_login_successful,
        round(SUM(CASE WHEN name = 'bluetooth_pairing_error' THEN 1 ELSE 0 END) / SUM(CASE WHEN name = 'bluetooth_pairing_initiated' THEN 1 ELSE 0 END),2) AS failed_pairing_ratio,
        round(SUM(CASE WHEN name = 'bluetooth_reading_error' THEN 1 ELSE 0 END) / SUM(CASE WHEN name = 'bluetooth_reading_initiated' THEN 1 ELSE 0 END),2) AS failed_reading_ratio

    FROM
        prodeu.mypd_btlog_cleaned b

    WHERE appOSType not in('','web')
    AND b.sourceId != 'UNKNOWN'
    AND sourceId IN {format(tuple(cohort_sourceIds_NA))}
    GROUP BY
        b.sourceId, appOSType,  appVersion, appOSVersion
""")

display(btlog_ratio)


# COMMAND ----------

# filter for initiated -> 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct sourceId 
# MAGIC from mypd.capd_union
# MAGIC where ConnectionType = 'bluetooth_reading_error'

# COMMAND ----------



# COMMAND ----------

capd_union = spark.sql(f"""
select * 
from mypd.capd_union
order by event_timestamp ASC
""")
display(capd_union)

# COMMAND ----------



# COMMAND ----------


# Assuming `capd_union` is the DataFrame containing the unioned data
# Define a window specification to order the rows by event_timestamp
#windowSpec = Window.partitionBy("sourceId").orderBy("event_timestamp")
windowSpec = Window.partitionBy("sourceId", date_trunc("date", col("event_timestamp"))).orderBy("event_timestamp")

# Create columns with the next row's data for the same sourceId
capd_union_with_next_row = capd_union.withColumn("next_sourceId", lead(col("sourceId")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_ConnectionType", lead(col("ConnectionType")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_two_ConnectionType", lead(col("ConnectionType"), 2).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_three_ConnectionType", lead(col("ConnectionType"), 3).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_four_ConnectionType", lead(col("ConnectionType"), 4).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_five_ConnectionType", lead(col("ConnectionType"), 5).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_six_ConnectionType", lead(col("ConnectionType"), 6).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_seven_ConnectionType", lead(col("ConnectionType"), 7).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_eight_ConnectionType", lead(col("ConnectionType"), 8).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_VitalType", lead(col("VitalType")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_Region", lead(col("Region")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_event_timestamp", lead(col("event_timestamp")).over(windowSpec))
capd_union_with_next_row = capd_union_with_next_row.withColumn("next_enight_event_timestamp", lead(col("event_timestamp"),8).over(windowSpec))
# Filter rows where ConnectionType is 'bluetooth_reading_error'
capd_bt_next_row = capd_union_with_next_row.filter(col("ConnectionType") == "bluetooth_reading_error")


capd_bt_next_row = capd_bt_next_row.select(
    'sourceId',
    'ConnectionType',
    'next_ConnectionType',
    'next_two_ConnectionType',
    'next_three_ConnectionType',
    'next_four_ConnectionType',
    'next_five_ConnectionType',
    'next_six_ConnectionType',
    'next_seven_ConnectionType',
    'next_eight_ConnectionType',
    'next_event_timestamp',
    'next_enight_event_timestamp'
)

# Display the resulting DataFrame
display(capd_bt_next_row)


# COMMAND ----------




# count the number of tries per sourceId per day, before they can successfully record vitals using BT with no long receive BT error
# Convert event_timestamp to date type
capd_union = capd_union.withColumn("date", to_date(col("event_timestamp").cast("timestamp")))

# Add the next row's ConnectionType column
capd_union = capd_union.withColumn("next_ConnectionType", lead(col("ConnectionType")).over(windowSpec))

# Filter rows with 'bluetooth_reading_initiated' and the next row's ConnectionType is not 'bluetooth_reading_initiated' or 'bluetooth_reading_error' or 'Manual'
count_per_date = capd_union.filter((col("ConnectionType") == "bluetooth_reading_initiated") & 
                                   (col("next_ConnectionType").isNotNull()) & 
                                   (col("next_ConnectionType") != "bluetooth_reading_initiated") & 
                                   (col("next_ConnectionType") != "bluetooth_reading_error") &
                                   (col("next_ConnectionType") != "Manual")) \
    .groupBy("date", "sourceId") \
    .count()

# Display the result
display(count_per_date)


# COMMAND ----------

from pyspark.sql.functions import avg, round

# of the days that they encountered errors, how many tries before they can successfully record vitals USING BT??
# Calculate average count per sourceId and round to 2 decimal places
average_count_per_sourceId = count_per_date.groupBy("sourceId").agg(round(avg("count"), 2).alias("avg_bt_tries_count"))


# Display the result
display(average_count_per_sourceId)



# COMMAND ----------

capd_cohort = spark.sql(f"""
select * from mypd.capd_cohort""")
testcohort = capd_cohort.join(average_count_per_sourceId, on ='sourceId', how = 'left')
display(testcohort)

# COMMAND ----------

# how many average tries before someone switches to manual?


# COMMAND ----------

# how many users had paired

# COMMAND ----------

# for each sourceid, how many of them have churned? 
# following error three times, what do they do?

# COMMAND ----------

churned_and_reactivated_capd = spark.sql("""
    SELECT
        t1.sourceId,
        CASE WHEN t1.patientRegion = 'PRD-4' THEN 'US Canada'
             WHEN t1.patientRegion = 'PRD-8' THEN 'Latin America'
             END AS Region,
        CASE
            WHEN (t1.patientRegion = 'PRD-4' OR t1.patientRegion = 'PRD-8') AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t2
                WHERE t2.sourceId = t1.sourceId
                    AND t2.treatmentTime > t1.treatmentTime
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 30)
            ) AND EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t3
                WHERE t3.sourceId = t1.sourceId
                    AND t3.treatmentTime > DATE_ADD(t1.treatmentTime, 30)
            ) AND EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t4
                WHERE t4.sourceId = t1.sourceId
                    AND t4.treatmentTime > DATE_ADD(t1.treatmentTime, 30)
                    AND t4.treatmentTime <= DATE_ADD(t1.treatmentTime, 60)
            ) THEN 'reactivated then churned'
            WHEN (t1.patientRegion = 'PRD-4' OR t1.patientRegion = 'PRD-8') AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t5
                WHERE t5.sourceId = t1.sourceId
                    AND t5.treatmentTime > DATE_ADD(t1.treatmentTime, 30)
                    AND t5.treatmentTime <= DATE_ADD(t1.treatmentTime, 60)
            ) THEN 'reactivated'
            ELSE 'churned'
        END AS status
    FROM prodeu.mypd_treatment t1
    WHERE 
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
        AND DATEDIFF(CURRENT_DATE(), t1.treatmentTime) >= 30
    GROUP BY t1.sourceId, status,Region
""")

display(churned_and_reactivated_capd)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

churned_and_reactivated_capd = spark.sql("""
    SELECT
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-4' AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t2
                WHERE t2.sourceId = t1.sourceId
                    AND t2.treatmentTime > t1.treatmentTime
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 60)
            ) THEN t1.sourceId
            ELSE NULL
        END) AS num_churned_capd_USCanada,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-8' AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t2
                WHERE t2.sourceId = t1.sourceId
                    AND t2.treatmentTime > t1.treatmentTime
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 60)
            ) THEN t1.sourceId
            ELSE NULL
        END) AS num_churned_capd_LatinAmerica,

        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-4' AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t2
                WHERE t2.sourceId = t1.sourceId
                    AND t2.treatmentTime > t1.treatmentTime
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 60)
            ) AND EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t3
                WHERE t3.sourceId = t1.sourceId
                    AND t3.treatmentTime > DATE_ADD(t1.treatmentTime, 60)
            ) THEN t1.sourceId
            ELSE NULL
        END) AS num_reactivated_capd_USCanada,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-8' AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t2
                WHERE t2.sourceId = t1.sourceId
                    AND t2.treatmentTime > t1.treatmentTime
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 60)
            ) AND EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t3
                WHERE t3.sourceId = t1.sourceId
                    AND t3.treatmentTime > DATE_ADD(t1.treatmentTime, 60)
            ) THEN t1.sourceId
            ELSE NULL
        END) AS num_reactivated_capd_LatinAmerica,
        num_churned_capd_USCanada - num_reactivated_capd_USCanada as US_Canada_churned,
        num_churned_capd_LatinAmerica - num_reactivated_capd_LatinAmerica as LA_churned
    FROM prodeu.mypd_treatment t1
    WHERE 
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
    GROUP BY activity_month
    HAVING DATEDIFF(CURRENT_DATE(), MAX(t1.treatmentTime)) >= 30
""")

display(churned_and_reactivated_capd)

# COMMAND ----------



# COMMAND ----------

# 30 days
churned_capd = spark.sql("""
    SELECT
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-4' AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t2
                WHERE t2.sourceId = t1.sourceId
                    AND t2.treatmentTime > t1.treatmentTime
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 30)
            ) THEN t1.sourceId
            ELSE NULL
        END) AS num_churned_capd_USCanada,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-8' AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t2
                WHERE t2.sourceId = t1.sourceId
                    AND t2.treatmentTime > t1.treatmentTime
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 30)
            ) THEN t1.sourceId
            ELSE NULL
        END) AS num_churned_capd_LatinAmerica
    FROM prodeu.mypd_treatment t1
    WHERE 
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
    GROUP BY activity_month
    HAVING DATEDIFF(CURRENT_DATE(), MAX(t1.treatmentTime)) >= 30
""")

display(churned_capd)


# COMMAND ----------

#churn rates over time

# capd churn rate by region
# numb of churned capd in a given month / numb of active numb of capd for the month = churn rate for capd by month

churned_capd = spark.sql("""
    SELECT
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-4' THEN t1.sourceId
            ELSE NULL
        END) AS num_churned_capd_USCanada,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-8' THEN t1.sourceId
            ELSE NULL
        END) AS num_churned_capd_LatinAmerica
    FROM prodeu.mypd_treatment t1
    WHERE 
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
        AND NOT EXISTS (
            SELECT 1 FROM prodeu.mypd_treatment t2
            WHERE
                t2.sourceId = t1.sourceId
                AND t2.treatmentTime > t1.treatmentTime 
                AND t2.treatmentTime <= add_months(t1.treatmentTime, 1)
        )
    GROUP BY activity_month
    HAVING DATEDIFF(CURRENT_DATE(), MAX(t1.treatmentTime)) >= 30
""")

display(churned_capd)

# COMMAND ----------

#churn rates over time

# capd churn rate by region
# numb of churned capd in a given month / numb of active numb of capd for the month = churn rate for capd by month

churned_capd = spark.sql("""
    SELECT
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-4' THEN t1.sourceId
            ELSE NULL
        END) AS num_churned_capd_USCanada,
        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-8' THEN t1.sourceId
            ELSE NULL
        END) AS num_churned_capd_LatinAmerica
    FROM prodeu.mypd_treatment t1
    WHERE 
        DATE_FORMAT(t1.treatmentTime, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
        AND NOT EXISTS (
            SELECT 1 FROM prodeu.mypd_treatment t2
            WHERE
                t2.sourceId = t1.sourceId
                AND t2.treatmentTime > t1.treatmentTime 
                AND t2.treatmentTime <= add_months(t1.treatmentTime, 1)
        )
    GROUP BY activity_month
    HAVING DATEDIFF(CURRENT_DATE(), MAX(t1.treatmentTime)) >= 30
""")

display(churned_capd)

capd_active_patient = spark.sql("""
    SELECT
        DATE_FORMAT(t.treatmentTime, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT CASE WHEN t.patientRegion = 'PRD-4' THEN t.sourceId ELSE NULL END) AS vital_num_active_USCanada,
        COUNT(DISTINCT CASE WHEN t.patientRegion = 'PRD-8' THEN t.sourceId ELSE NULL END) AS vital_num_active_LatinAmerica
    FROM prodeu.mypd_treatment t
    WHERE 
            DATE_FORMAT(t.treatmentTime, 'yyyy-MM') < DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
        AND DATE_FORMAT(t.treatmentTime, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
    GROUP BY activity_month
""")

#display(capd_active_patient)

from pyspark.sql.functions import col, round

# Joining churned_apd and active_customers
capd_joined_df = churned_capd.join(capd_active_patient, on='activity_month', how='inner')

# Calculating churn rate for each region and rounding to two digits
capd_churn_rate_df = capd_joined_df.withColumn('churn_rate_USCanada', round(col('num_churned_capd_USCanada') / col('vital_num_active_USCanada'), 2)) \
    .withColumn('churn_rate_LatinAmerica', round(col('num_churned_capd_LatinAmerica') / col('vital_num_active_LatinAmerica'), 2)) \
    .select('activity_month', 'churn_rate_USCanada', 'churn_rate_LatinAmerica')

display(capd_churn_rate_df)




# COMMAND ----------

# btlog_ratio
# inner joined to cohort analysis

#inner_joined_cohort = capd_cohort.join(btlog_breakdown, on = 'sourceId', how = 'inner')
#display(inner_joined_cohort)

# COMMAND ----------

# usage pattern  Create a separate dataframe
# day of the week with most 'did not record at all'
# User activity pattern : Currently Active, Currently inactive, returned(now active), returned(now inactive)
# user vital pattern: while active (most vital pattern)
# 



# COMMAND ----------



# COMMAND ----------

# histogram of active distribution?

# COMMAND ----------

# capd_daily_missed_updated_connectiontypes
# longest streak of collected all vitals?
# longest streak of did not record vitals at all?
# Most missed vital?
# longest active in days?

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# first generate column streak, then generate streak number for each sourceId, then generate unique sourceId by max streak? = max streak
# average streak? by sourceId
# active period? longest active period? avg active period?

#test = capd_daily_missed_updated_connectiontypes
#display(test)

# COMMAND ----------



# COMMAND ----------

# 00u1c21swrOE3VbWZ417_0oaswb00iAZUz63zh416
# 00u19nhvvadW7Srdh417_0oaswb00iAZUz63zh416
# 00u5d8vf11dEflHTJ417_0oaswb00iAZUz63zh416 (weird) (very active but ALWAYS only take some vitals, did not take all vitals required)


#wt = capd_daily_missed_updated_connectiontypes_streak.filter(col('sourceId') == '00u5d8vf11dEflHTJ417_0oaswb00iAZUz63zh416')
#display(wt)

# COMMAND ----------



# COMMAND ----------

#from pyspark.sql.functions import max as pyspark_max

# Group the 'test' dataframe by sourceId and calculate the maximum continuous_days
#max_continuous_days = test.groupBy('sourceId').agg(pyspark_max('continuous_days').alias('max_continuous_days'))

# Display the resulting dataframe
#display(max_continuous_days)


# COMMAND ----------

#from pyspark.sql.functions import col

# Perform a left anti-join to find sourceId values present in capd_cohort but not in max_continuous_days
#missing_sourceId_cohort = capd_cohort.join(max_continuous_days, on='sourceId', how='left_anti')

# Perform a left anti-join to find sourceId values present in max_continuous_days but not in capd_cohort
#missing_sourceId_continuous_days = max_continuous_days.join(capd_cohort, on='sourceId', how='left_anti')

#display(missing_sourceId_cohort)


# COMMAND ----------

#testfilter

#distinct_sourceIdstest = testfilter.select('sourceId').distinct()

#display(distinct_sourceIdstest)

# COMMAND ----------



# COMMAND ----------

# Completion Rate


# COMMAND ----------

# Total number of opportunities to capture

# COMMAND ----------

# Distribution of vitals capture compliance
# frequency of vitals capture
# usage patterns
# user type

# COMMAND ----------

# Compliance rate

# COMMAND ----------

# CPD VS APD

# COMMAND ----------

# Streaks of exchanges

# COMMAND ----------


