# Databricks notebook source
from pyspark.sql.functions import from_json, col, round, when, sum, expr, date_sub, current_date, lit, first, mode, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType, MapType
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

# problematic sourceId excluded (possible test user and patients with changed prescription)
# all of these sourceId had very few rows of data

NotIn = ['00u6wlfbcqcocsoua417_0oaswb00iAZUz63zh416',
         '00u1bfafhuE3RzYw6417_0oaswb00iAZUz63zh416',
         '00u3egs7e9y3dm31u417_0oaswb00iAZUz63zh416'
         ]

# COMMAND ----------

# MAGIC %md 
# MAGIC # Definitions
# MAGIC - Entry_Patterns:
# MAGIC   - Manual Only: Manually entered 100% of their vitals
# MAGIC   - Mostly Manual: Manually entered 91% to 99% of their vitals
# MAGIC   - Heavy Manual: Manually entered 75% to 90% of their vitals
# MAGIC   - Medium Manual: Manually entered 60% to 74% of their vitals
# MAGIC   - Light Manual: Manually entered 51% to 59% of their vitals
# MAGIC   - Equally Used: Equally used manual and bluetooth when entering vitals
# MAGIC   - Light Bluetooth:  Used bluetooth to entry 51% to 59% of their vitals
# MAGIC   - Medium Bluetooth: Used bluetooth to entry 60% to 74% of their vitals
# MAGIC   - Heavy Bluetooth: Used bluetooth to entry 75% to 90% of their vitals
# MAGIC   - Mostly Bluetooth: Used bluetooth to entry 91% to 99% of their vitals
# MAGIC   - Bluetooth Only: Used bluetooth to enter 100% of their vitals

# COMMAND ----------

# MAGIC %md
# MAGIC - num_bluetooth: Number of vitals entered using bluetooth
# MAGIC - num_manual: Number of vitals entered manually
# MAGIC - total_num_connection: sum of the total number of vitals entered using bluetooth and manually
# MAGIC - percentage_bt: percentage of vitals entered using bluetooth
# MAGIC - percentage_manual: percentage of vitals entered manually
# MAGIC - Streaks: Streaks are calculated based on the number of days where a patient correctly recorded all vitals
# MAGIC - Active: A patient is considered active if they have recorded vitals, a patient is inactive is when they have not logged their vitals for more than 14 days
# MAGIC - Active_Status: Is based on whether the patient is currently active or not, based on the defined 7-day period
# MAGIC - Churned_Status: A patient is considered churned if they have been inactive for 30 or more days
# MAGIC - max_streak: Is the maximum days which a patient maintained a streak in days
# MAGIC - avg_streak: Is the average of all that patient's streaks in days
# MAGIC - max_active_days: Is the maximum period which a patient is considered active in days
# MAGIC - avg_active_days: is the average number in days which a patient is considered active
# MAGIC - Most_missed_day_of_week: Is the day of the week which that patient missed the most vitals 
# MAGIC

# COMMAND ----------

# capd patient total count by region

capd_count_total = spark.sql(f"""
SELECT
    CASE WHEN t.patientRegion = 'PRD-4' THEN 'US, Canada'
         WHEN t.patientRegion = 'PRD-8' THEN 'Latin America'
         END as Region,
    COUNT(DISTINCT t.sourceId) as CAPD_Num_Patient

FROM prodeu.mypd_treatment t
WHERE t.patientRegion IN ('PRD-4', 'PRD-8')
GROUP BY Region
""")  
display(capd_count_total)

# COMMAND ----------

# capd bluetooth
Capd_count_bluetooth = spark.sql("""
    SELECT 

        CASE WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN 'Latin America'
        END AS CAPD_Region,
        Count(CASE WHEN e.payload.vital.source.type = 'Bluetooth' THEN e.payload.sourceId END) AS Counts_Bluetooth,
        Count(CASE WHEN e.payload.vital.source.type = 'Manual' THEN e.payload.sourceId END )AS Counts_Manual
    FROM prodeu.events e
    WHERE e.type = 'vital-recorded'
        AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') IN ('capd')
        AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    GROUP BY CAPD_Region
""")
display(Capd_count_bluetooth)

# COMMAND ----------

# bluetooth vs manual over time
Capd_count_bluetooth_date = spark.sql("""
    SELECT 

        date(e.timestamp) as date,
        CASE WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN 'Latin America'
        END AS CAPD_Region,
        Count(CASE WHEN e.payload.vital.source.type = 'Bluetooth' THEN e.payload.sourceId END) AS Counts_Bluetooth,
        Count(CASE WHEN e.payload.vital.source.type = 'Manual' THEN e.payload.sourceId END )AS Counts_Manual
    FROM prodeu.events e
    WHERE e.type = 'vital-recorded'
        AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') IN ('capd')
        AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    GROUP BY date, CAPD_Region
""")
display(Capd_count_bluetooth_date)

# COMMAND ----------

# ADP Patient total count by region

apd_count_total = spark.sql(f"""
    SELECT 
        CASE WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion')= 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN 'Latin America'
        END as Region,
        count(distinct e.payload.sourceId) as APD_Num_Patient
    FROM prodeu.events e
    WHERE e.type = 'vital-recorded'
    AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    GROUP BY Region

""")  
display(apd_count_total)

# COMMAND ----------

# APD Bluetooth vs manual
apd_count_bluetooth = spark.sql("""
    SELECT 

        CASE WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN 'Latin America'
        END AS APD_Region,
        Count(CASE WHEN e.payload.vital.source.type = 'Bluetooth' THEN e.payload.sourceId END) AS Counts_Bluetooth,
        Count(CASE WHEN e.payload.vital.source.type = 'Manual' THEN e.payload.sourceId END )AS Counts_Manual
    FROM prodeu.events e
    WHERE e.type = 'vital-recorded'
        AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
        AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    GROUP BY APD_Region
""")
display(apd_count_bluetooth)



# COMMAND ----------

# apd bluetooth vs manual
apd_count_bluetooth_date = spark.sql("""
    SELECT 
        date(e.timestamp) as date,
        CASE WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN 'Latin America'
        END AS APD_Region,
        Count(CASE WHEN e.payload.vital.source.type = 'Bluetooth' THEN e.payload.sourceId END) AS Counts_Bluetooth,
        Count(CASE WHEN e.payload.vital.source.type = 'Manual' THEN e.payload.sourceId END )AS Counts_Manual
    FROM prodeu.events e
    WHERE e.type = 'vital-recorded'
        AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
        AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    GROUP BY date, APD_Region
""")
display(apd_count_bluetooth_date)

# COMMAND ----------

# apd only
# vital-recorded events
apd_MAU = spark.sql(f"""
    SELECT 
        
        DATE_FORMAT(e.payload.vital.timestamp, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT CASE 
            WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN e.payload.sourceId 
            ELSE NULL 
        END) as Count_USCanada,
        COUNT(DISTINCT CASE 
            WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN e.payload.sourceId 
            ELSE NULL 
        END) as Count_LatinAmerica
    FROM prodeu.events e
    WHERE e.type = 'vital-recorded'
    AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    GROUP BY activity_month
    ORDER BY activity_month DESC
""")  
display(apd_MAU)


# COMMAND ----------

# apd only
# vital-recorded events
apd_DAU = spark.sql(f"""
    SELECT 
        
        date(e.payload.vital.timestamp) as date,
        COUNT(DISTINCT CASE 
            WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN e.payload.sourceId 
            ELSE NULL 
        END) as Count_USCanada,
        COUNT(DISTINCT CASE 
            WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN e.payload.sourceId 
            ELSE NULL 
        END) as Count_LatinAmerica
    FROM prodeu.events e
    WHERE e.type = 'vital-recorded'
    AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    GROUP BY date
    ORDER BY date DESC
""")  
display(apd_DAU)

# COMMAND ----------

# capd only MAU
# 'capd-treatment-recorded' events from treatment table
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

# capd only DAU
# 'capd-treatment-recorded' events from treatment table
capd_DAU = spark.sql(f"""
SELECT
    date(t.treatmenttime) as date,
    COUNT(DISTINCT CASE WHEN t.patientRegion = 'PRD-4' THEN t.sourceId END) as USCanada_Count,
    COUNT(DISTINCT CASE WHEN t.patientRegion = 'PRD-8' THEN t.sourceId END) as LatinAmerica_Count
FROM prodeu.mypd_treatment t
WHERE t.patientRegion IN ('PRD-4', 'PRD-8')
GROUP BY date
""")  
display(capd_DAU)

# COMMAND ----------

# Count apd
apd_count = spark.sql(f"""
    SELECT 
        COUNT(DISTINCT CASE 
            WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN e.payload.sourceId 
            ELSE NULL 
        END) as apd_Count_USCanada,
        COUNT(DISTINCT CASE 
            WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN e.payload.sourceId 
            ELSE NULL 
        END) as apd_Count_LatinAmerica
    FROM prodeu.events e
    WHERE e.type = 'vital-recorded'
    AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')

""")  

capd_Count= spark.sql(f"""
SELECT
    COUNT(DISTINCT CASE WHEN t.patientRegion = 'PRD-4' THEN t.sourceId END) as Capd_USCanada_Count,
    COUNT(DISTINCT CASE WHEN t.patientRegion = 'PRD-8' THEN t.sourceId END) as Capd_LatinAmerica_Count
FROM prodeu.mypd_treatment t
WHERE t.patientRegion IN ('PRD-4', 'PRD-8')

""")  
# Total count of patients by region
merged_count = apd_count.join(capd_Count)
display(merged_count)


# COMMAND ----------

# myPd count by region

MyPD_count = spark.sql(f"""
    SELECT 
        CASE WHEN get_json_object(e.payload.value, '$.region') = 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.value, '$.region') = 'PRD-8' THEN 'Latin America'
        END as Region,
        count(distinct e.payload.sourceId) as Num_Patient

    FROM prodeu.events e
    WHERE e.type = "sharesource-patient-settings-configured"
    AND get_json_object(e.payload.value, '$.region') IN ('PRD-4', 'PRD-8')
    GROUP BY region
""")

display(MyPD_count)

# COMMAND ----------

#  myPd count by region

MyPD_active_region_MAU = spark.sql(f"""
    SELECT 
        DATE_FORMAT(e.timestamp, 'yyyy-MM') as activity_month,
        CASE WHEN get_json_object(e.payload.value, '$.region') = 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.value, '$.region') = 'PRD-8' THEN 'Latin America'
        END as Region,
        count(distinct e.payload.sourceId) as num_user

    FROM prodeu.events e
    WHERE e.type = "sharesource-patient-settings-configured"
    AND get_json_object(e.payload.value, '$.region') IN ('PRD-4', 'PRD-8')
    GROUP BY activity_month,region


""")

display(MyPD_active_region_MAU)

# COMMAND ----------

# MAU APD VS CAPD
active_customers = spark.sql("""
    SELECT
        DATE_FORMAT(e.timestamp, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT e.payload.sourceId) FILTER(WHERE e.type IN ('capd-treatment-recorded', 'vital-recorded') AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') = 'capd') AS capd_num_active,
        --COUNT(DISTINCT e.payload.sourceId) FILTER(WHERE e.type IN ('capd-treatment-recorded')) AS capd_num_active,
        COUNT(DISTINCT e.payload.sourceId) FILTER(WHERE get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') IN ('amia','claria','kaguya')) AS apd_num_active
    FROM prodeu.events e
    WHERE e.tenant = '2'
    -- AND e.type IN ('capd-treatment-recorded', 'vital-recorded')
    AND DATE_FORMAT(e.timestamp, 'yyyy-MM') < DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
    AND DATE_FORMAT(e.timestamp, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
    GROUP BY activity_month
""")
display(active_customers)


# COMMAND ----------

# apd vitals_taken dataframe
apd_vitals_taken = spark.sql(f"""
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
    AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') IN ('PRD-4', 'PRD-8')
    ORDER BY VitalDate DESC
""")  

#display(apd_vitals_taken)
# Create a window partitioned by sourceId and ordered by VitalDate
window_spec = Window.partitionBy('sourceId').orderBy('VitalDate')

# Add a column to track the first recorded VitalDate for each sourceId
first_apd_vitals_taken = apd_vitals_taken.withColumn('FirstVitalDate', F.first('VitalDate').over(window_spec))
# Group by FirstVitalDate and count the number of unique sourceIds
first_vital_dates = first_apd_vitals_taken.groupBy('FirstVitalDate', 'Region').agg(F.countDistinct('sourceId').alias('NewUniqueSourceIds'))

# Pivot the data to have separate columns for each region
first_vital_dates_pivoted = first_vital_dates.groupBy('FirstVitalDate').pivot('Region').sum('NewUniqueSourceIds')

# Rename the columns to have the region-specific names
first_vital_dates_pivoted = first_vital_dates_pivoted.withColumnRenamed('US, Canada', 'NewUniqueSourceIds_USCanada') \
    .withColumnRenamed('Latin America', 'NewUniqueSourceIds_LatinAmerica')
first_vital_dates_pivoted = first_vital_dates_pivoted.na.fill(0)

# Create a window spec for cumulative sum by region
window_spec_region = Window.partitionBy().orderBy('FirstVitalDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add a cumulative sum column for each region in UniqueSourceIdCount
apd_cumulative_sourceIds_region = first_vital_dates_pivoted.withColumn(
    'CumulativeSourceIds_USCanada',
    F.sum(F.col('NewUniqueSourceIds_USCanada')).over(window_spec_region)
).withColumn(
    'CumulativeSourceIds_LatinAmerica',
    F.sum(F.col('NewUniqueSourceIds_LatinAmerica')).over(window_spec_region)
)

# Display the result
#display(apd_cumulative_sourceIds_region)

# Count the number of unique sourceIds by region and group by date and region
apd_unique_sourceIds = apd_vitals_taken.groupBy('VitalDate', 'Region').agg(F.countDistinct('sourceId').alias('UniqueSourceIdCount'))

# Pivot the data to have separate columns for each region in UniqueSourceIdCount
unique_sourceIds_pivoted = apd_unique_sourceIds.groupBy('VitalDate').pivot('Region').sum('UniqueSourceIdCount')

# Rename the columns to have the region-specific names in UniqueSourceIdCount
unique_sourceIds_pivoted = unique_sourceIds_pivoted.withColumnRenamed('US, Canada', 'UniqueSourceIdCount_USCanada') \
    .withColumnRenamed('Latin America', 'UniqueSourceIdCount_LatinAmerica')

unique_sourceIds_pivoted= unique_sourceIds_pivoted.na.fill(0)
# Display the result
#display(unique_sourceIds_pivoted)

# Create a window spec for cumulative sum by region
window_spec_region = Window.partitionBy().orderBy('FirstVitalDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add a cumulative sum column for each region in UniqueSourceIdCount
apd_cumulative_sourceIds_region = first_vital_dates_pivoted.withColumn(
    'CumulativeSourceIds_USCanada',
    F.sum(F.col('NewUniqueSourceIds_USCanada')).over(window_spec_region)
).withColumn(
    'CumulativeSourceIds_LatinAmerica',
    F.sum(F.col('NewUniqueSourceIds_LatinAmerica')).over(window_spec_region)
)

# Display the result
#display(apd_cumulative_sourceIds_region)

# Rename 'VitalDate' column in apd_CumulativeSourceIds
apd_cumulative_sourceIds_region = apd_cumulative_sourceIds_region.withColumnRenamed('FirstVitalDate', 'VitalDate')

# Join unique_sourceId_by_date with apd_CumulativeSourceIds
apd_joined_df = unique_sourceIds_pivoted.join(apd_cumulative_sourceIds_region, 'VitalDate', 'left')


# Define a window specification to order by VitalDate in ascending order
window_spec = Window.orderBy('VitalDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)


# Fill null values in 'NewUniqueSourceIds' with the last non-null value
apd_filled_unique_sourceId = apd_joined_df.withColumn('NewUniqueSourceIds_LatinAmerica', F.last('NewUniqueSourceIds_LatinAmerica', ignorenulls=True).over(window_spec)) \
    .withColumn('NewUniqueSourceIds_USCanada', F.last('NewUniqueSourceIds_USCanada', ignorenulls=True).over(window_spec))

apd_filled_cumulative_sourceIds = apd_filled_unique_sourceId.withColumn('CumulativeSourceIds_LatinAmerica', F.last('CumulativeSourceIds_LatinAmerica', ignorenulls=True).over(window_spec)) \
    .withColumn('CumulativeSourceIds_USCanada', F.last('CumulativeSourceIds_USCanada', ignorenulls=True).over(window_spec))


# Create a new column by subtracting 'UniqueSourceIdCount' from 'CumulativeSourceIds'
apd_num_missed_vitals_by_region = apd_filled_cumulative_sourceIds.withColumn('Num_Missed_Vitals_USCanada', apd_filled_cumulative_sourceIds['CumulativeSourceIds_USCanada'] - apd_filled_cumulative_sourceIds['UniqueSourceIdCount_USCanada']) \
    .withColumn('Num_Missed_LatinAmerica', apd_filled_cumulative_sourceIds['CumulativeSourceIds_LatinAmerica'] - apd_filled_cumulative_sourceIds['UniqueSourceIdCount_LatinAmerica'])

# Display missed apd vitals
display(apd_num_missed_vitals_by_region)
display(apd_vitals_taken)
# missing rate over time? us canada vs latin america?

# COMMAND ----------


# APD completion rate by region
# apd completion rate = (1 - (num of apd patients who missed vitals for the day / total cumulative num of apd patients at that specific time )


apd_completion_rate = apd_num_missed_vitals_by_region.withColumn('USCanada_apd_completion_rate', 1 - (round(apd_num_missed_vitals_by_region['Num_Missed_Vitals_USCanada'] / apd_num_missed_vitals_by_region['CumulativeSourceIds_USCanada'], 2))) \
    .withColumn('LatinAmerica_apd_completion_rate', 1-( round(apd_num_missed_vitals_by_region['Num_Missed_LatinAmerica'] / apd_num_missed_vitals_by_region['CumulativeSourceIds_LatinAmerica'], 2)))

# Display the rounded APD missed rates
display(apd_completion_rate)


# COMMAND ----------

# apd churn rate by region
churned_apd = spark.sql("""
    SELECT
        DATE_FORMAT(e1.payload.vital.timestamp, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT CASE
            WHEN get_json_object(e1.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN e1.payload.sourceId
            ELSE NULL
        END) AS num_churned_apd_USCanada,
        COUNT(DISTINCT CASE
            WHEN get_json_object(e1.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN e1.payload.sourceId
            ELSE NULL
        END) AS num_churned_apd_LatinAmerica
    FROM prodeu.events e1
    WHERE 
        e1.type = 'vital-recorded'
        AND get_json_object(e1.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
        AND DATE_FORMAT(e1.payload.vital.timestamp, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
        AND NOT EXISTS (
            SELECT 1 FROM prodeu.events e2
            WHERE
                e2.payload.sourceId = e1.payload.sourceId
                AND e2.payload.vital.timestamp > e1.payload.vital.timestamp 
                AND e2.payload.vital.timestamp <= add_months(e1.payload.vital.timestamp, 1)
        )
    GROUP BY activity_month
    HAVING DATEDIFF(CURRENT_DATE(), MAX(e1.payload.vital.timestamp)) >= 30
""")

#display(churned_apd)


apd_active_patient = spark.sql("""
    SELECT
        DATE_FORMAT(e.timestamp, 'yyyy-MM') as activity_month,
        COUNT(DISTINCT CASE WHEN e.type = 'vital-recorded' AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN e.payload.sourceId ELSE NULL END) AS vital_num_active_USCanada,
        COUNT(DISTINCT CASE WHEN e.type = 'vital-recorded' AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN e.payload.sourceId ELSE NULL END) AS vital_num_active_LatinAmerica
    FROM prodeu.events e
    WHERE e.tenant = '2'
        AND e.type = 'vital-recorded'
        AND get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') NOT IN ('capd')
        AND DATE_FORMAT(e.timestamp, 'yyyy-MM') < DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
        AND DATE_FORMAT(e.timestamp, 'yyyy-MM') != DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM')
    GROUP BY activity_month
""")

#display(apd_active_patient)

from pyspark.sql.functions import col, round

# Joining churned_apd and active_customers
apd_joined_df = churned_apd.join(apd_active_patient, on='activity_month', how='inner')

# Calculating churn rate for each region and rounding to two digits
apd_churn_rate_df = apd_joined_df.withColumn('churn_rate_USCanada', round(col('num_churned_apd_USCanada') / col('vital_num_active_USCanada'), 2)) \
    .withColumn('churn_rate_LatinAmerica', round(col('num_churned_apd_LatinAmerica') / col('vital_num_active_LatinAmerica'), 2)) \
    .select('activity_month', 'churn_rate_USCanada', 'churn_rate_LatinAmerica')

display(apd_churn_rate_df)

# COMMAND ----------

# capd vitals_taken dataframe
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
#display(capd_vitals_taken)
# Create a window partitioned by sourceId and ordered by VitalDate
window_spec = Window.partitionBy('sourceId').orderBy('VitalDate')

# Add a column to track the first recorded VitalDate for each sourceId
first_capd_vitals_taken = capd_vitals_taken.withColumn('FirstVitalDate', F.first('VitalDate').over(window_spec))
# Group by FirstVitalDate and count the number of unique sourceIds
first_vital_dates = first_capd_vitals_taken.groupBy('FirstVitalDate', 'Region').agg(F.countDistinct('sourceId').alias('NewUniqueSourceIds'))

# Pivot the data to have separate columns for each region
first_vital_dates_pivoted = first_vital_dates.groupBy('FirstVitalDate').pivot('Region').sum('NewUniqueSourceIds')

# Rename the columns to have the region-specific names
first_vital_dates_pivoted = first_vital_dates_pivoted.withColumnRenamed('US, Canada', 'NewUniqueSourceIds_USCanada') \
    .withColumnRenamed('Latin America', 'NewUniqueSourceIds_LatinAmerica')
first_vital_dates_pivoted = first_vital_dates_pivoted.na.fill(0)

# Create a window spec for cumulative sum by region
window_spec_region = Window.partitionBy().orderBy('FirstVitalDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add a cumulative sum column for each region in UniqueSourceIdCount
capd_cumulative_sourceIds_region = first_vital_dates_pivoted.withColumn(
    'CumulativeSourceIds_USCanada',
    F.sum(F.col('NewUniqueSourceIds_USCanada')).over(window_spec_region)
).withColumn(
    'CumulativeSourceIds_LatinAmerica',
    F.sum(F.col('NewUniqueSourceIds_LatinAmerica')).over(window_spec_region)
)

# Display the result
#display(capd_cumulative_sourceIds_region)

# Count the number of unique sourceIds by region and group by date and region
capd_unique_sourceIds = capd_vitals_taken.groupBy('VitalDate', 'Region').agg(F.countDistinct('sourceId').alias('UniqueSourceIdCount'))

# Pivot the data to have separate columns for each region in UniqueSourceIdCount
unique_sourceIds_pivoted = capd_unique_sourceIds.groupBy('VitalDate').pivot('Region').sum('UniqueSourceIdCount')

# Rename the columns to have the region-specific names in UniqueSourceIdCount
unique_sourceIds_pivoted = unique_sourceIds_pivoted.withColumnRenamed('US, Canada', 'UniqueSourceIdCount_USCanada') \
    .withColumnRenamed('Latin America', 'UniqueSourceIdCount_LatinAmerica')

unique_sourceIds_pivoted = unique_sourceIds_pivoted.na.fill(0)
# Display the result
#display(unique_sourceIds_pivoted)

# Create a window spec for cumulative sum by region
window_spec_region = Window.partitionBy().orderBy('FirstVitalDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Add a cumulative sum column for each region in UniqueSourceIdCount
capd_cumulative_sourceIds_region = first_vital_dates_pivoted.withColumn(
    'CumulativeSourceIds_USCanada',
    F.sum(F.col('NewUniqueSourceIds_USCanada')).over(window_spec_region)
).withColumn(
    'CumulativeSourceIds_LatinAmerica',
    F.sum(F.col('NewUniqueSourceIds_LatinAmerica')).over(window_spec_region)
)

# Display the result
#display(capd_cumulative_sourceIds_region)

# Rename 'VitalDate' column in capd_CumulativeSourceIds
capd_cumulative_sourceIds_region = capd_cumulative_sourceIds_region.withColumnRenamed('FirstVitalDate', 'VitalDate')

# Join unique_sourceId_by_date with capd_CumulativeSourceIds
capd_joined_df = unique_sourceIds_pivoted.join(capd_cumulative_sourceIds_region, 'VitalDate', 'left')


# Define a window specification to order by VitalDate in ascending order
window_spec = Window.orderBy('VitalDate').rowsBetween(Window.unboundedPreceding, Window.currentRow)


# Fill null values in 'NewUniqueSourceIds' with the last non-null value
capd_filled_unique_sourceId = capd_joined_df.withColumn('NewUniqueSourceIds_LatinAmerica', F.last('NewUniqueSourceIds_LatinAmerica', ignorenulls=True).over(window_spec)) \
    .withColumn('NewUniqueSourceIds_USCanada', F.last('NewUniqueSourceIds_USCanada', ignorenulls=True).over(window_spec))

capd_filled_cumulative_sourceIds = capd_filled_unique_sourceId.withColumn('CumulativeSourceIds_LatinAmerica', F.last('CumulativeSourceIds_LatinAmerica', ignorenulls=True).over(window_spec)) \
    .withColumn('CumulativeSourceIds_USCanada', F.last('CumulativeSourceIds_USCanada', ignorenulls=True).over(window_spec))


# Create a new column by subtracting 'UniqueSourceIdCount' from 'CumulativeSourceIds'
capd_num_missed_vitals_by_region = capd_filled_cumulative_sourceIds.withColumn('Num_Missed_Vitals_USCanada', capd_filled_cumulative_sourceIds['CumulativeSourceIds_USCanada'] - capd_filled_cumulative_sourceIds['UniqueSourceIdCount_USCanada']) \
    .withColumn('Num_Missed_LatinAmerica', capd_filled_cumulative_sourceIds['CumulativeSourceIds_LatinAmerica'] - capd_filled_cumulative_sourceIds['UniqueSourceIdCount_LatinAmerica'])

# Display missed CAPD vitals
display(capd_num_missed_vitals_by_region)
display(capd_vitals_taken)

# COMMAND ----------

# CAPD completion rate by region
# capd completion rate = (1 - (num of capd patients who missed vitals for the day / total cumulative num of capd patients at that specific time )

capd_completion_rate = capd_num_missed_vitals_by_region.withColumn('USCanada_capd_completion_rate', 1 - (round(capd_num_missed_vitals_by_region['Num_Missed_Vitals_USCanada'] / capd_num_missed_vitals_by_region['CumulativeSourceIds_USCanada'], 2))) \
    .withColumn('LatinAmerica_capd_completion_rate', 1 - (round(capd_num_missed_vitals_by_region['Num_Missed_LatinAmerica'] / capd_num_missed_vitals_by_region['CumulativeSourceIds_LatinAmerica'], 2)))

# Display the rounded CAPD missed rates
display(capd_completion_rate)


# COMMAND ----------


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

#display(churned_capd)

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

# The number of vitals missed depending on how often they need to be taking vitals
# if they need to take a lot of vitals in a week, they are likely to miss more than those who don't need to take that many vitals in a week.
# compliance rate? if all vitals taken as required, then they are compliant?


# COMMAND ----------

# need to verify

# capd vitals and prescription
# for capd: obtain the counts of missed vitals by region and by type and by month, how many supposed vitals according to prescription
# treatment table joined prescription table joined vital-recorded events => this should be the data for capd vital recorded events with their prescription and treatment data
# Using this table to calculate for the metrics on missed vital

# Calculation process:
# 1. Join vital-recorded events to treatment table + prescription table (this will get capd only, because only capd patients have prescriptions)
# 2. Pivot the table so that vital types are displayed as separate columns (blood pressure, blood glucos, temperature ect).
# 3. Get the min vital taken date as the first day, sequence explode to create a calendar table from that day to current date
# 4. Cross join with the pivoted table to get a 'complete table' where even the days the patients who missed submitting a vital will show up as a null record for the day
# 5. For the null (cross-join created rows), fill the null with their prescription based on before or after the row where the prescription columns are not null and for the same sourceId; 
#    (This fill get their prescription even for the days they missed, we can infer how many they have missed, and how many they should have taken)
# 6. For each row, based on the prescription 'Both', 'First', 'NotAsked' and convert to numbers (2, 1, 0) to determine how many times they need to take vitals per day
# 7. Since many patients may needs to take multiple vitals of different type in one day, I use the first() to obtain the requirement based on their prescription, and aggregate it based on 
#    daily/weekly/monthly to determine how much they should take, and how many they missed, how many they have taken.
# 8. Calculate the proportion of compliance rate based on daily events

capd_vitals_prescription = spark.sql(f"""
    SELECT
        date(tmp.vital_date) as VitalDate,
        tmp.treatmentsourceId AS sourceId,
        tmp.eventsourceId AS events_sourceId,
        e.type,
        CASE WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-4' THEN 'US, Canada'
             WHEN get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') = 'PRD-8' THEN 'Latin America'
        END as Region,
        CASE WHEN events_sourceId IS NULL THEN 'Missed Vital'
             WHEN events_sourceId IS NOT NULL THEN 'Recorded Vital'
             END AS vital_recorded,
        e.payload.vital.source.type as ConnectionType,
        tmp.treatmentDevice,
        tmp.vitalId,
        tmp.treatmentId,
        tmp.type as VitalType,
        tmp.treatmentTime,
        prescription.dayOfTheWeek,
        prescription.BPCollectionOrder,
        prescription.pulse.collectionOrder as PulseOrder,
        prescription.glucose.collectionOrder as GlucoseOrder,
        prescription.temperature.collectionOrder as TempOrder,
        prescription.weight.collectionOrder as WeightOrder,
        prescription.urineOutput
    FROM (
        -- 'capd-treatment-recorded' events are stored as prodeu.mypd_treatment
        SELECT
            t.sourceId AS treatmentsourceId,
            e.payload.sourceId AS eventsourceId,
            e.payload.vital.id AS vitalId,
            t.treatmentId,
            e.payload.vital.type,
            e.payload.vital.timestamp AS vital_date,
            CASE WHEN get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') = 'capd' then 'capd' END AS treatmentDevice,
            t.treatmentTime,
            ROW_NUMBER() OVER (PARTITION BY e.payload.vital.id, t.treatmentId ORDER BY e.payload.vital.timestamp DESC) AS row_number
        FROM prodeu.mypd_treatment t
        LEFT JOIN prodeu.events e ON e.payload.sourceId = t.sourceId AND DATE(e.payload.vital.timestamp) = DATE(t.treatmentTime)
    ) tmp
    LEFT JOIN prodeu.mypd_prescription prescription ON prescription.treatmentId = tmp.treatmentId AND prescription.sourceId = tmp.treatmentsourceId
    LEFT JOIN prodeu.events e on e.payload.sourceId = tmp.treatmentsourceId and e.payload.vital.id = tmp.vitalId
    WHERE tmp.row_number = 1
    AND e.type = 'vital-recorded'
    AND tmp.treatmentsourceId Not In {format(tuple(NotIn))}
    AND tmp.eventsourceId NOT IN {format(tuple(NotIn))}
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') != 'PRD-3' -- Exclude PRD-3
    GROUP BY tmp.vitalId, tmp.treatmentId, tmp.treatmentsourceId, tmp.eventsourceId, tmp.type, tmp.vital_date, tmp.treatmentTime, tmp.treatmentDevice,
             prescription.dayOfTheWeek, prescription.BPCollectionOrder, PulseOrder, GlucoseOrder,
             TempOrder, WeightOrder, prescription.urineOutput, e.type, region, e.payload.vital.source.type
    ORDER BY vital_date DESC
""")
#display(capd_vitals_prescription)

from pyspark.sql import functions as F

# Pivot the VitalType column
pivoted_capd_vitals_prescription = capd_vitals_prescription.groupBy(
    'VitalDate', 'treatmentTime','sourceId', 'events_sourceId', 'type', 'region','ConnectionType','vital_recorded', 'treatmentDevice',
    'vitalId', 'treatmentId', 'dayOfTheWeek', 'BPCollectionOrder', 'PulseOrder', 'GlucoseOrder', 'TempOrder', 'WeightOrder', 'urineOutput'
).pivot('VitalType').agg(F.first('VitalType'))  # Modify the aggregation function based on your requirements

# Display the pivoted DataFrame
#display(pivoted_capd_vitals_prescription)


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------

# Get the minimum and maximum dates in the existing DataFrame
min_date = pivoted_capd_vitals_prescription.select(F.min('VitalDate')).first()[0]
max_date = pivoted_capd_vitals_prescription.select(F.max('VitalDate')).first()[0]

# Generate the complete date range using explode and sequence
date_range = spark.sql(f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'), INTERVAL 1 day)) AS VitalDate")

# Cross join date_range with unique sourceId values
sourceIds = pivoted_capd_vitals_prescription.select('sourceId').distinct()
#date_range_cross_join = sourceIds.crossJoin(date_range)
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#added
min_vital_date_df = pivoted_capd_vitals_prescription.groupBy('sourceId').agg(F.min('VitalDate').alias('min_vital_date'))

# Generate the complete date range for each sourceId
date_range_cross_join = min_vital_date_df.crossJoin(date_range) \
    .where(F.col('VitalDate') >= F.col('min_vital_date')) \
    .drop('min_vital_date')

# Add missing columns to date_range_cross_join
complete_df = date_range_cross_join.join(pivoted_capd_vitals_prescription, ['VitalDate', 'sourceId'], 'left_outer')
#display(complete_df)

#-----------------------------------------------------------------------------------------------------------------------------------------

# complete_df, for every sourceId, when the treatmentTime is null, fill the same row with on columns 'region BPCollectionOrder PulseOrder GlucoseOrder TempOrder WeightOrder' with the previous row of matching sourceId

from pyspark.sql import functions as F
from pyspark.sql.window import Window
# filled everything
# Define the columns to fill
columns_to_fill = ['region', 'BPCollectionOrder', 'PulseOrder', 'GlucoseOrder', 'TempOrder', 'WeightOrder']

# Define the window specification partitioned by sourceId and ordered by VitalDate
windowSpec = Window.partitionBy('sourceId').orderBy('VitalDate').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Fill the specified columns with the previous non-null value within the same sourceId
for column in columns_to_fill:
    complete_df = complete_df.withColumn(
        column,
        F.last(F.col(column), ignorenulls=True).over(windowSpec)
    )

#display(complete_df)


# try to fill missing records ---------------------------------------------------------------------------------------------------------------------------------------------------------------

from pyspark.sql.window import Window
from pyspark.sql.functions import when, col, sum, expr

# Create new columns for the number of vitals supposed to be taken by type and region
# changed to complete_df
capd_df = complete_df.withColumn('NumVitalsSupposed_BP', when(col('BPCollectionOrder') == 'Both', 2)
                                                          .when(col('BPCollectionOrder') == 'First', 1)
                                                          .otherwise(0)) \
    .withColumn('NumVitalsSupposed_Pulse', when(col('PulseOrder') == 'Both', 2)
                                              .when(col('PulseOrder') == 'First', 1)
                                              .otherwise(0)) \
    .withColumn('NumVitalsSupposed_Glucose', when(col('GlucoseOrder') == 'Both', 2)
                                                .when(col('GlucoseOrder') == 'First', 1)
                                                .otherwise(0)) \
    .withColumn('NumVitalsSupposed_Temp', when(col('TempOrder') == 'Both', 2)
                                             .when(col('TempOrder') == 'First', 1)
                                             .otherwise(0)) \
    .withColumn('NumVitalsSupposed_Weight', when(col('WeightOrder') == 'Both', 2)
                                               .when(col('WeightOrder') == 'First', 1)
                                               .otherwise(0)) \
    .withColumn('Region', when(col('region').isNull(), 'Unknown').otherwise(col('region')))

# Create window specification for calculating the number of vitals taken
# added sourceid for testing
window_spec = Window.partitionBy('sourceId','treatmentId', 'vitaldate')

# Create new columns for the number of vitals actually taken by type and region
capd_df = capd_df.withColumn('NumVitalsTaken_BP', sum(when(col('bloodPressure').isNotNull(), 1).otherwise(0)).over(window_spec)) \
    .withColumn('NumVitalsTaken_Pulse', sum(when(col('pulse').isNotNull(), 1).otherwise(0)).over(window_spec)) \
    .withColumn('NumVitalsTaken_Glucose', sum(when(col('bloodGlucose').isNotNull(), 1).otherwise(0)).over(window_spec)) \
    .withColumn('NumVitalsTaken_Temp', sum(when(col('temperature').isNotNull(), 1).otherwise(0)).over(window_spec)) \
    .withColumn('NumVitalsTaken_Weight', sum(when(col('weight').isNotNull(), 1).otherwise(0)).over(window_spec))

# Calculate the number of vitals missed by type and region
capd_df = capd_df.withColumn('NumVitalsMissed_BP', when(col('NumVitalsSupposed_BP') > col('NumVitalsTaken_BP'),
                                                        col('NumVitalsSupposed_BP') - col('NumVitalsTaken_BP')).otherwise(0)) \
    .withColumn('NumVitalsMissed_Pulse', when(col('NumVitalsSupposed_Pulse') > col('NumVitalsTaken_Pulse'),
                                              col('NumVitalsSupposed_Pulse') - col('NumVitalsTaken_Pulse')).otherwise(0)) \
    .withColumn('NumVitalsMissed_Glucose', when(col('NumVitalsSupposed_Glucose') > col('NumVitalsTaken_Glucose'),
                                                col('NumVitalsSupposed_Glucose') - col('NumVitalsTaken_Glucose')).otherwise(0)) \
    .withColumn('NumVitalsMissed_Temp', when(col('NumVitalsSupposed_Temp') > col('NumVitalsTaken_Temp'),
                                             col('NumVitalsSupposed_Temp') - col('NumVitalsTaken_Temp')).otherwise(0)) \
    .withColumn('NumVitalsMissed_Weight', when(col('NumVitalsSupposed_Weight') > col('NumVitalsTaken_Weight'),
                                               col('NumVitalsSupposed_Weight') - col('NumVitalsTaken_Weight')).otherwise(0))

# Display the updated dataframe
#display(capd_df)
# capd_df_connectiontype  for different data exploration use

capd_df_connectiontype = capd_df
#
#####
from pyspark.sql.functions import sum, expr, first
from pyspark.sql.window import Window

# Calculate the first value for each sourceId and vitaldate
first_values = capd_df.groupby('sourceId', 'VitalDate') \
                     .agg(first('NumVitalsSupposed_BP').alias('FirstSupposed_BP'),
                          first('NumVitalsMissed_BP').alias('FirstMissed_BP'),
                          first('NumVitalsTaken_BP').alias('FirstTaken_BP'),
                          first('NumVitalsSupposed_Pulse').alias('FirstSupposed_Pulse'),
                          first('NumVitalsMissed_Pulse').alias('FirstMissed_Pulse'),
                          first('NumVitalsTaken_Pulse').alias('FirstTaken_Pulse'),
                          first('NumVitalsSupposed_Glucose').alias('FirstSupposed_Glucose'),
                          first('NumVitalsMissed_Glucose').alias('FirstMissed_Glucose'),
                          first('NumVitalsTaken_Glucose').alias('FirstTaken_Glucose'),
                          first('NumVitalsSupposed_Temp').alias('FirstSupposed_Temp'),
                          first('NumVitalsMissed_Temp').alias('FirstMissed_Temp'),
                          first('NumVitalsTaken_Temp').alias('FirstTaken_Temp'),
                          first('NumVitalsSupposed_Weight').alias('FirstSupposed_Weight'),
                          first('NumVitalsMissed_Weight').alias('FirstMissed_Weight'),
                          first('NumVitalsTaken_Weight').alias('FirstTaken_Weight')
                          )

# daily missed = first value
capd_daily_missed = first_values
# Change column names starting with "First" to "Total"
for column in capd_daily_missed.columns:
    if column.startswith("First"):
        new_column_name = "Total" + column[5:]
        capd_daily_missed = capd_daily_missed.withColumnRenamed(column, new_column_name)


# Group by sourceId,  and week of the year, and sum the first values
capd_weekly_missed = first_values.withColumn('WeekOfYear', expr("weekofyear(VitalDate)")) \
                                .groupby('sourceId',  'WeekOfYear') \
                                .agg(sum('FirstSupposed_BP').alias('TotalSupposed_BP'),
                                     sum('FirstMissed_BP').alias('TotalMissed_BP'),
                                     sum('FirstTaken_BP').alias('TotalTaken_BP'),
                                     sum('FirstSupposed_Pulse').alias('TotalSupposed_Pulse'),
                                     sum('FirstMissed_Pulse').alias('TotalMissed_Pulse'),
                                     sum('FirstTaken_Pulse').alias('TotalTaken_Pulse'),
                                     sum('FirstSupposed_Glucose').alias('TotalSupposed_Glucose'),
                                     sum('FirstMissed_Glucose').alias('TotalMissed_Glucose'),
                                     sum('FirstTaken_Glucose').alias('TotalTaken_Glucose'),
                                     sum('FirstSupposed_Temp').alias('TotalSupposed_Temp'),
                                     sum('FirstMissed_Temp').alias('TotalMissed_Temp'),
                                     sum('FirstTaken_Temp').alias('TotalTaken_Temp'),
                                     sum('FirstSupposed_Weight').alias('TotalSupposed_Weight'),
                                     sum('FirstMissed_Weight').alias('TotalMissed_Weight'),
                                     sum('FirstTaken_Weight').alias('TotalTaken_Weight')
                                     ) \
                                .orderBy('sourceId', 'WeekOfYear')

# Group by sourceId, year, and month, and sum the first values
capd_monthly_missed = first_values.withColumn('Year', expr("year(VitalDate)")) \
                                 .withColumn('Month', expr("month(VitalDate)")) \
                                 .groupby('sourceId', 'Year', 'Month') \
                                 .agg(sum('FirstSupposed_BP').alias('TotalSupposed_BP'),
                                      sum('FirstMissed_BP').alias('TotalMissed_BP'),
                                      sum('FirstTaken_BP').alias('TotalTaken_BP'),
                                      sum('FirstSupposed_Pulse').alias('TotalSupposed_Pulse'),
                                      sum('FirstMissed_Pulse').alias('TotalMissed_Pulse'),
                                      sum('FirstTaken_Pulse').alias('TotalTaken_Pulse'),
                                      sum('FirstSupposed_Glucose').alias('TotalSupposed_Glucose'),
                                      sum('FirstMissed_Glucose').alias('TotalMissed_Glucose'),
                                      sum('FirstTaken_Glucose').alias('TotalTaken_Glucose'),
                                      sum('FirstSupposed_Temp').alias('TotalSupposed_Temp'),
                                      sum('FirstMissed_Temp').alias('TotalMissed_Temp'),
                                      sum('FirstTaken_Temp').alias('TotalTaken_Temp'),
                                      sum('FirstSupposed_Weight').alias('TotalSupposed_Weight'),
                                      sum('FirstMissed_Weight').alias('TotalMissed_Weight'),
                                      sum('FirstTaken_Weight').alias('TotalTaken_Weight')
                                      ) \
                                 .orderBy('sourceId', 'Year', 'Month')

# Join capd_df with capd_daily_missed on sourceId
capd_daily_missed = capd_daily_missed.join(
    capd_df.select('sourceId', 'Region').distinct(),
    on='sourceId'
).groupBy('sourceId', 'VitalDate').agg(
    first('Region').alias('Region'),
    *[sum(col).alias(col) for col in capd_daily_missed.columns if col != 'sourceId' and col != 'VitalDate']
)
# Join capd_df with capd_weekly_missed on sourceId
capd_weekly_missed = capd_weekly_missed.join(
    capd_df.select('sourceId', 'Region').distinct(),
    on='sourceId'
).groupBy('sourceId', 'WeekOfYear').agg(
    first('Region').alias('Region'),
    *[sum(col).alias(col) for col in capd_weekly_missed.columns if col != 'sourceId' and col != 'WeekOfYear']
)
# Join capd_df with capd_monthly_missed on sourceId
capd_monthly_missed = capd_monthly_missed.join(
    capd_df.select('sourceId', 'Region').distinct(),
    on='sourceId'
).groupBy('sourceId', 'Year', 'Month').agg(
    first('Region').alias('Region'),
    *[sum(col).alias(col) for col in capd_monthly_missed.columns if col != 'sourceId' and col != 'Year' and col != 'Month']
)


# Display the daily missed vitals
display(capd_daily_missed)

# Display the average missed vitals on a weekly basis
display(capd_weekly_missed)

# Display the average missed vitals on a monthly basis
display(capd_monthly_missed)


# COMMAND ----------



# COMMAND ----------

# calculated based on cross-joined dates (where there are no record but when they are supposed to be taking vitals)



capd_weekly_missed_updated = capd_weekly_missed.withColumn('BP_Miss_Rate', round(col('TotalMissed_BP') / col('TotalSupposed_BP'), 2)) \
    .withColumn('Pulse_Miss_Rate', round(col('TotalMissed_Pulse') / col('TotalSupposed_Pulse'), 2)) \
    .withColumn('Glucose_Miss_Rate', round(col('TotalMissed_Glucose') / col('TotalSupposed_Glucose'), 2)) \
    .withColumn('Temp_Miss_Rate', round(col('TotalMissed_Temp') / col('TotalSupposed_Temp'), 2)) \
    .withColumn('Weight_Miss_Rate', round(col('TotalMissed_Weight') / col('TotalSupposed_Weight'), 2))

display(capd_weekly_missed_updated)

capd_monthly_missed_updated = capd_monthly_missed.withColumn('BP_Miss_Rate', round(col('TotalMissed_BP') / col('TotalSupposed_BP'), 2)) \
    .withColumn('Pulse_Miss_Rate', round(col('TotalMissed_Pulse') / col('TotalSupposed_Pulse'), 2)) \
    .withColumn('Glucose_Miss_Rate', round(col('TotalMissed_Glucose') / col('TotalSupposed_Glucose'), 2)) \
    .withColumn('Temp_Miss_Rate', round(col('TotalMissed_Temp') / col('TotalSupposed_Temp'), 2)) \
    .withColumn('Weight_Miss_Rate', round(col('TotalMissed_Weight') / col('TotalSupposed_Weight'), 2))

display(capd_monthly_missed_updated)



# COMMAND ----------



# COMMAND ----------

# daily (missed rate by day)

capd_daily_missed_updated = capd_daily_missed.withColumn('BP_Miss_Rate', round(col('TotalMissed_BP') / col('TotalSupposed_BP'), 2)) \
    .withColumn('Pulse_Miss_Rate', round(col('TotalMissed_Pulse') / col('TotalSupposed_Pulse'), 2)) \
    .withColumn('Glucose_Miss_Rate', round(col('TotalMissed_Glucose') / col('TotalSupposed_Glucose'), 2)) \
    .withColumn('Temp_Miss_Rate', round(col('TotalMissed_Temp') / col('TotalSupposed_Temp'), 2)) \
    .withColumn('Weight_Miss_Rate', round(col('TotalMissed_Weight') / col('TotalSupposed_Weight'), 2))


display(capd_daily_missed_updated)



# COMMAND ----------


# When supposed = taken for all vitals then 'Correctly recorded all vitals'
# when some or all supposed > 0, but the actual taken does not match to supposed to be taken, only some actual taken matches to supposed taken then 'Only recorded some vitals'
# when total taken > supposed taken then 'Incorrectly taken vitals, took more than needed'
# when supposed taken are > 0 but all actual taken = 0 then 'did not record at all'


from pyspark.sql.functions import col, when

capd_daily_missed_updated = capd_daily_missed_updated.withColumn('Status',
    when(
        (col('TotalSupposed_BP') == col('TotalTaken_BP')) &
        (col('TotalSupposed_Pulse') == col('TotalTaken_Pulse')) &
        (col('TotalSupposed_Glucose') == col('TotalTaken_Glucose')) &
        (col('TotalSupposed_Temp') == col('TotalTaken_Temp')) &
        (col('TotalSupposed_Weight') == col('TotalTaken_Weight')),
        'Correctly Recorded All Vitals'
    ).when(
        (col('TotalSupposed_BP') < col('TotalTaken_BP')) |
        (col('TotalSupposed_Pulse') < col('TotalTaken_Pulse')) |
        (col('TotalSupposed_Glucose') < col('TotalTaken_Glucose')) |
        (col('TotalSupposed_Temp') < col('TotalTaken_Temp')) |
        (col('TotalSupposed_Weight') < col('TotalTaken_Weight')),
        'Incorrectly Taken Vitals, Took more than needed'
    ).when(
        ((col('TotalTaken_BP') > 0) |
        (col('TotalTaken_Pulse') > 0) |
        (col('TotalTaken_Glucose') > 0) |
        (col('TotalTaken_Temp') > 0) |
        (col('TotalTaken_Weight') > 0)) &
        ~(
            (col('TotalSupposed_BP') == col('TotalTaken_BP')) &
            (col('TotalSupposed_Pulse') == col('TotalTaken_Pulse')) &
            (col('TotalSupposed_Glucose') == col('TotalTaken_Glucose')) &
            (col('TotalSupposed_Temp') == col('TotalTaken_Temp')) &
            (col('TotalSupposed_Weight') == col('TotalTaken_Weight'))
        ),
        'Only Recorded Some Vitals'
    ).when(
        ((col('TotalSupposed_BP') > 0) |
        (col('TotalSupposed_Pulse') > 0) |
        (col('TotalSupposed_Glucose') > 0) |
        (col('TotalSupposed_Temp') > 0) |
        (col('TotalSupposed_Weight') > 0)) &
        (col('TotalTaken_BP') == 0) &
        (col('TotalTaken_Pulse') == 0) &
        (col('TotalTaken_Glucose') == 0) &
        (col('TotalTaken_Temp') == 0) &
        (col('TotalTaken_Weight') == 0),
        'Did not Record Vital At All'
    ).otherwise('Unknown')
)



# add day of the week 
capd_daily_missed_updated = capd_daily_missed_updated.withColumn(
    "day_of_week", F.date_format(F.col("vitaldate"), "EEEE")
)



#--------------------------------------------- running compliance rate

# Define a window specification to partition and order the rows
windowSpec = Window.partitionBy("sourceId").orderBy("Vitaldate")

# Calculate the cumulative count of 'Correctly Recorded All Vitals' status
correct_count = F.sum(F.when(F.col("Status") == "Correctly Recorded All Vitals", 1).otherwise(0)).over(windowSpec)

# Calculate the cumulative count of all rows
total_count = F.count("*").over(windowSpec)

# Calculate the running compliance rate
running_compliance_rate = round(((correct_count.cast("double") / total_count.cast("double")) * 100),2)

# Add the 'running_compliance_rate' column to the DataFrame
capd_daily_missed_updated = capd_daily_missed_updated.withColumn("running_compliance_rate", running_compliance_rate)

# Display the resulting DataFrame
display(capd_daily_missed_updated)

# Due to large data results, not all results are displayed (Databricks warning)

# COMMAND ----------



# COMMAND ----------

# Only recorded some vitals

filtered_only_recorded_some_vitals = capd_daily_missed_updated.filter(col('Status') == 'Only Recorded Some Vitals')
display(filtered_only_recorded_some_vitals)


# COMMAND ----------



# Filter by region

filtered_only_LA = capd_daily_missed_updated.filter(col('Region') == 'Latin America')
display(filtered_only_LA)

filtered_only_NA = capd_daily_missed_updated.filter(col('Region') == 'US, Canada')
display(filtered_only_NA)

# COMMAND ----------


# incorrect vitals taken only
# 
#  
filtered_only_incorrect = capd_daily_missed_updated.filter(col('Status') == 'Incorrectly Taken Vitals, Took more than needed')
display(filtered_only_incorrect)

# COMMAND ----------



# COMMAND ----------


# Active is when Consecutive days missed >= 7



# Define the window partitioned by sourceId and ordered by VitalDate in ascending order
window = Window.partitionBy('sourceId').orderBy(F.asc('VitalDate'))

# Calculate the lagged status to compare with the current status
capd_daily_missed_status = capd_daily_missed_updated.withColumn(
    'prev_status',
    F.lag('Status').over(window)
)

# Reorder the DataFrame by VitalDate within each sourceId group
capd_daily_missed_status = capd_daily_missed_status.orderBy('sourceId', 'VitalDate')

# Calculate the date difference in days between the current and previous VitalDate
capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'date_diff',
    F.datediff(capd_daily_missed_status['VitalDate'], F.lag('VitalDate').over(window))
)

# Calculate the consecutive days missed for each sourceId
capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'consecutive_days_missed',
    F.when(capd_daily_missed_status['Status'] == 'Did not Record Vital At All', capd_daily_missed_status['date_diff'])
    .otherwise(0)
)

# Calculate the running sum of consecutive days missed within each sourceId
capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'consecutive_days_missed',
    F.sum('consecutive_days_missed').over(window.rowsBetween(Window.unboundedPreceding, 0))
)



#----------------------------------------------------------------

# isactive and ischurned needs to define windowspec
windowSpec = Window.partitionBy("sourceId").orderBy(F.asc('VitalDate'))

#  Create a column to track the streak reset flag
status_reset_flag = F.when((F.lag(F.col("Status")).over(windowSpec) != "Did not Record Vital At All") & (F.col("Status") == "Did not Record Vital At All"), 1).otherwise(0)

cumulative_status_reset_count = F.sum(status_reset_flag).over(windowSpec)

adjustedActiveWindowSpec = Window.partitionBy("sourceId", cumulative_status_reset_count).orderBy(F.asc('VitalDate'))

capd_daily_missed_status = capd_daily_missed_status.withColumn("continuous_inactive_days",
                       F.when(F.col("Status") != "Did not Record Vital At All", 0)
                       .otherwise(F.row_number().over(adjustedActiveWindowSpec)))



#----------------------------------------------------------------


# Label rows as 'Active' or 'Inactive' based on the defined conditions
capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsActive',
    F.when(
        (capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        (capd_daily_missed_status['continuous_inactive_days'] <= 7) |
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All'),
        'Active'
    ).otherwise('Inactive')
)

# ----------------- 'IsChurned'  30 days + inactivty means churned -------------------

capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsChurned',
    F.when(
        (capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        (capd_daily_missed_status['continuous_inactive_days'] <= 30) |
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All'),
        'Not Churned'
    ).otherwise('Churned')
)


# Show the resulting DataFrame
display(capd_daily_missed_status)


# COMMAND ----------

# test

#testing = capd_daily_missed_status.filter(col('sourceId') == '00u7nnb90gT91UbM4417_0oaswb00iAZUz63zh416')
#display(testing)

# COMMAND ----------

connection_type_df = capd_df.groupBy('sourceId', 'VitalDate').agg(mode('ConnectionType').alias('ConnectionType'))

# Join connection_type_df with capd_daily_missed on sourceId and VitalDate
capd_daily_missed_updated_connectiontypes = capd_daily_missed_status.join(
    connection_type_df,
    on=['sourceId', 'VitalDate'],
    how='inner'
)
# cleaned null

capd_daily_missed_updated_connectiontypes = capd_daily_missed_updated_connectiontypes.withColumn(
    'ConnectionType',
    when(col('Status') == 'Did not Record Vital At All', None).otherwise(col('ConnectionType'))
)


# Display the updated DataFrame with ConnectionType
display(capd_daily_missed_updated_connectiontypes)

# COMMAND ----------


#testfilter = capd_daily_missed_updated_connectiontypes.filter(col('Status') == 'Did not Record Vital At All')
#display(testfilter)

# COMMAND ----------

# Excluded churned (Inactive for more than 14 days) 

active_df = capd_daily_missed_updated_connectiontypes.filter(capd_daily_missed_updated_connectiontypes['IsActive'] == 'Active')
display(active_df)


# COMMAND ----------

NA_active_df = active_df.filter(active_df['Region'] == 'US, Canada')
display(NA_active_df)

# COMMAND ----------

LA_active_df = active_df.filter(active_df['Region'] == 'Latin America')
display(LA_active_df)

# COMMAND ----------

# capd_daily_missed_updated_connectiontypes 
# create categories of users


# COMMAND ----------

# How many vitals are missed in a week? out of 7 days, how many do they miss?
# instead of this, calculate the total number of vitals they supposed to take in a month, and the number of vitals they actually took in that month. 
# The rate will be the rate of correctly adhering to prescription and taking vitals when they are supposed to, by region, by type, by patient, by month/week. (use daily count for compliance)
# 

# COMMAND ----------

# completion rate = num patient submitted vitals / num patient total 

# COMMAND ----------

# 00u19nk9yjhEJT2jq417_0oaswb00iAZUz63zh416 (strange) (might be a test user?)
# 00u55bylxehgEk37c417_0oasw31ld2g4UQsqR416 (LA)
# 00u19nhvvadW7Srdh417_0oaswb00iAZUz63zh416
# 00u1xy0j6prx5slN0417_0oaswb00iAZUz63zh416
# 00u3dncs7y88Mr7wN417_0oaswb00iAZUz63zh416 (started, left, then come back, left again)
# 00ujc06y9sPu7RCVk416_0oaswb00iAZUz63zh416 (very active user, from 2021 to present with very little downtime)
# 00ujc06y9sPu7RCVk416_0oaswb00iAZUz63zh416 new


#testtwo = pivoted_capd_vitals_prescription.filter(col('sourceId') == '00u19nk9yjhEJT2jq417_0oaswb00iAZUz63zh416')
#display(testtwo)

# COMMAND ----------

# Create a separate dataframe
# User pattern categories: active user, inactive, returned (now active), returned (now inactive)
# longest active period (longest streak of being active, before going inactive in days)

# sourceId, region, num_bluetooth, num_manual, (type of user which one use more? bt or manual), categories, active period in days ()
# most used type, (break down categories:)
# 25% BT VS 75% M = heavy manual user


# --------------
# bt/manual only user (100%)
# heavy bt/manual user (90%)
# mostly manual user, mostly bt user (75% vitals recorded using that method)
# about equal (55% to 45% )
# ----------------
# number of vitals taken category (?)
# 

# COMMAND ----------


# streaks and active days 
# using capd_daily_missed_updated_connectiontypes

# Define the window partitioned by sourceId and ordered by vitaldate
#window_spec = Window.partitionBy("sourceId").orderBy(F.asc('VitalDate'))

# Create the Streak column based on the conditions
capd_daily_missed_updated_connectiontypes_streak = capd_daily_missed_updated_connectiontypes.withColumn(
    "Streak",
    F.when(
        (F.col("Status") == "Correctly Recorded All Vitals") & (F.col("prev_status") == "Correctly Recorded All Vitals"),
        "Streak"
    ).otherwise("No Streak")
)

#--------------------------------------------- streaks change ---------------------------------------------------------

# Define a window specification to order the rows by Vitaldate and partition by sourceId
windowSpec = Window.partitionBy("sourceId").orderBy(F.asc('VitalDate'))

# Determine the change in streak status
status_change = F.when(F.col("Streak") == "No Streak", 1).otherwise(0)

# Create a column to track the streak reset flag
streak_reset_flag = F.when((F.lag(F.col("Streak")).over(windowSpec) == "No Streak") & (F.col("Streak") == "Streak"), 1).otherwise(0)

# Calculate the cumulative streak reset count
cumulative_reset_count = F.sum(streak_reset_flag).over(windowSpec)

# Create a new window specification starting from the streak reset point
adjustedWindowSpec = Window.partitionBy("sourceId", cumulative_reset_count).orderBy(F.asc('VitalDate'))

# Calculate the continuous_days column
capd_daily_missed_updated_connectiontypes_streak = capd_daily_missed_updated_connectiontypes_streak.withColumn("continuous_streak_days",
                       F.when(F.col("Streak") == "No Streak", 0)
                       .otherwise(F.row_number().over(adjustedWindowSpec)))


#------------------------------------------------------------ active change----------------------------------------------------------------------------
# Determine the change in active status
active_change = F.when(F.col("IsActive") == "Inactive", 1).otherwise(0)

# Create a column to track the streak reset flag
active_reset_flag = F.when((F.lag(F.col("IsActive")).over(windowSpec) == "Inactive") & (F.col("IsActive") == "Active"), 1).otherwise(0)

# Calculate the cumulative active reset count
cumulative_active_reset_count = F.sum(active_reset_flag).over(windowSpec)

# Create a new window specification starting from the streak reset point
adjustedActiveWindowSpec = Window.partitionBy("sourceId", cumulative_active_reset_count).orderBy(F.asc('VitalDate'))

# Calculate the continuous_days column
capd_daily_missed_updated_connectiontypes_streak = capd_daily_missed_updated_connectiontypes_streak.withColumn("continuous_active_days",
                       F.when(F.col("IsActive") == "Inactive", 0)
                       .otherwise(F.row_number().over(adjustedActiveWindowSpec)))


#---------------------------------------------------Join dataframes---------------------------------------------------------------------------------------------



# Display the resulting DataFrame
display(capd_daily_missed_updated_connectiontypes_streak)
        
capd_streak = capd_daily_missed_updated_connectiontypes_streak.groupBy("sourceId").agg(
    F.max("continuous_streak_days").alias("max_streak"),
    F.round(F.avg("continuous_streak_days"),2).alias("avg_streak"),
)


capd_active = capd_daily_missed_updated_connectiontypes_streak.groupBy("sourceId").agg(
    F.max("continuous_active_days").alias("max_active_days"),
    F.round(F.avg("continuous_active_days"),2).alias("avg_active_days"),
)

capd_streak_result = capd_streak.join(capd_active, on = 'sourceId')

# Display the resulting DataFrame
display(capd_streak_result)



# COMMAND ----------

# Filter capd_streak_result for region = 'Latin America'
capd_streak_result_latin_america = capd_daily_missed_updated_connectiontypes_streak.filter(capd_daily_missed_updated_connectiontypes_streak['Region'] == 'Latin America')

# Display the filtered DataFrame
display(capd_streak_result_latin_america)


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


# 40 / 69 patients have logs
# AND sourceId IN {format(tuple(cohort_sourceIds))}
testt = spark.sql(f"""
    SELECT distinct sourceId
    FROM prodeu.mypd_btlog_cleaned c
    WHERE sourceId IN {format(tuple(cohort_sourceIds))}
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
    AND sourceId IN {format(tuple(cohort_sourceIds))}
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
    AND sourceId IN {format(tuple(cohort_sourceIds))}

""")
display(btlog_pairing_error)

# COMMAND ----------

# reading error 
readingerror = spark.sql(f"""
    SELECT 
    count(distinct sourceId),
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_error') as reading_error,
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_initiated') as reading_initiated,
    round(COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_error')  / COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_initiated'),2) as reading_error_rate
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
    AND sourceId IN {format(tuple(cohort_sourceIds))}

""")
display(btlog_reading_success)

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
    AND sourceId IN {format(tuple(cohort_sourceIds))}
    GROUP BY
        b.sourceId, appOSType,  appVersion, appOSVersion
""")

display(btlog_ratio)


# COMMAND ----------

# filtered btlog to cohort



cohort = spark.sql(f"""
    SELECT DISTINCT sourceId
    FROM mypd.capd_cohort
""").collect()

sourceIds = [row.sourceId for row in cohort]

cohort_btcounts = spark.sql(f"""
    SELECT 
    sourceId, 
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_error') as reading_error,
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_successful') as reading_successful,
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_initiated') as reading_initiated,
    round(COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_error')/COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_initiated'),2) as reading_error_rate
    FROM prodeu.mypd_btlog_cleaned
    WHERE
    sourceId IN {format(tuple(cohort_sourceIds))}

    GROUP BY sourceId
""")
display(cohort_btcounts)

# COMMAND ----------


# capd_cohort categories
#capd_cohort.drop('bluetooth_count')
# capd_daily_missed_updated_connectiontypes

capd_cohort = capd_daily_missed_updated_connectiontypes.select('sourceId', 'region').distinct()
#capd_cohort.drop('bluetooth_count')


# count num_bluetooth
capd_cohort = capd_cohort.join(
    capd_daily_missed_updated_connectiontypes
        .filter(col('ConnectionType') == 'Bluetooth')
        .groupBy('sourceId')
        .agg(count('*').alias('num_bluetooth')),
    on='sourceId',
    how='left'
).fillna(0, subset=['num_bluetooth'])

# count num_manual
capd_cohort = capd_cohort.join(
    capd_daily_missed_updated_connectiontypes
        .filter(col('ConnectionType') == 'Manual')
        .groupBy('sourceId')
        .agg(count('*').alias('num_manual')),
    on='sourceId',
    how='left'
).fillna(0, subset=['num_manual'])

# total num_connection
capd_cohort = capd_cohort.withColumn('total_num_connection', col('num_bluetooth') + col('num_manual'))

# % bt/ total 
# % manual / total

capd_cohort = capd_cohort.withColumn('percentage_bt', round(col('num_bluetooth') / col('total_num_connection'),2))
capd_cohort = capd_cohort.withColumn('percentage_manual', round(col('num_manual') / col('total_num_connection'),2))

# most used method when entering vitals
capd_cohort = capd_cohort.withColumn('most_used', 
                                     when(col('num_bluetooth') > col('num_manual'), 'Bluetooth')
                                     .when(col('num_bluetooth') < col('num_manual'), 'Manual')
                                     .otherwise('Equally used'))



#capd_cohort.drop('entry_pattern')

capd_cohort = capd_cohort.withColumn('entry_pattern', 
                                     when(col('percentage_bt') == 1, 'Bluetooth only')
                                     .when((col('percentage_bt') >= 0.9) & (col('percentage_bt') < 1), 'Mostly Bluetooth')
                                     .when((col('percentage_bt') >= 0.75) & (col('percentage_bt') < 0.9), 'Heavy Bluetooth')
                                     .when((col('percentage_bt') >= 0.6) & (col('percentage_bt') < 0.75), 'Medium Bluetooth')
                                     .when((col('percentage_bt') > 0.5) & (col('percentage_bt') < 0.6), 'Light Bluetooth')
                                     .when(col('percentage_manual') == 1, 'Manual only')
                                     .when((col('percentage_manual') >= 0.9) & (col('percentage_manual') < 1), 'Mostly Manual')
                                     .when((col('percentage_manual') >= 0.75) & (col('percentage_manual') < 0.9), 'Heavy Manual')
                                     .when((col('percentage_manual') >= 0.6) & (col('percentage_manual') < 0.75), 'Medium Manual')
                                     .when((col('percentage_manual') > 0.5) & (col('percentage_manual') < 0.6), 'Light Manual')
                                     .when(col('percentage_manual') == col('percentage_bt'), 'Equally used')
                                     .otherwise('Other'))


# Display the updated capd_cohort DataFrame

# Join the streak count
capd_cohort = capd_cohort.join(capd_streak_result, on="sourceId")


#---------------------------------------------------------------------------------------------- day of week with most 'Did not record vitals at all'
# Calculate the count of occurrences of 'Did not Record Vital At All' for each sourceId and day of the week
result = capd_daily_missed_updated_connectiontypes.groupBy("sourceId", "day_of_week") \
    .agg(F.sum(F.when(F.col("Status") == "Did not Record Vital At All", 1).otherwise(0)).alias("count"))

# Define a window specification to partition by sourceId and order by the count in descending order
windowSpec = Window.partitionBy("sourceId").orderBy(F.desc("count"))

# Get the row with the highest count for each sourceId
max_count_row = result.withColumn("max_count", F.first("count").over(windowSpec))

# Filter the rows to get only the rows with the highest count for each sourceId
capd_most_missed_dow = max_count_row.filter(F.col("count") == F.col("max_count")) \
    .groupBy("sourceId").agg(F.first("day_of_week").alias("Most_missed_day_of_week"))

# Display the resulting DataFrame
#display(capd_most_missed_dow)

capd_cohort = capd_cohort.join(capd_most_missed_dow, on="sourceId")


# ---------------------------------------------------------------------- currently active vs inactive

# Define a window specification to partition by sourceId and order by VitalDate in descending order
windowSpec = Window.partitionBy("sourceId").orderBy(F.desc("VitalDate"))

# Get the most recent row of data for each sourceId
most_recent_row = capd_daily_missed_updated_connectiontypes_streak.withColumn("row_number", F.row_number().over(windowSpec)) \
    .filter(F.col("row_number") == 1)

# Create the 'active_pattern' column based on the conditions
active_pattern = F.when(
    (F.col("IsActive") == "Active") & (F.col("row_number") == 1),
    "Active"
).when(
    (F.col("IsActive") == "Inactive") & (F.col("row_number") == 1),
    "Inactive"
).otherwise(None)

# 'churned_pattern'

churned_pattern = F.when(
    (F.col("IsChurned") == "Not Churned") & (F.col("row_number") == 1),
    "Not Churned"
).when(
    (F.col("IsChurned") == "Churned") & (F.col("row_number") == 1),
    "Churned"
).otherwise(None)



# Select the sourceId and active_pattern columns
active_pattern = most_recent_row.select("sourceId", active_pattern.alias("Active_Status"))
churned_pattern = most_recent_row.select("sourceId", churned_pattern.alias("Churned_Status"))

#active_pattern = active_pattern.join(churned_pattern, on = 'sourceId')
churned_pattern = churned_pattern.dropDuplicates(["sourceId"])
# Remove duplicate rows (if any) and retain only unique sourceId
active_pattern = active_pattern.dropDuplicates(["sourceId"])


# Display the resulting DataFrame
#display(capd_active_pattern)
# -----------------------------------------------


capd_cohort = capd_cohort.join(active_pattern, on= 'sourceId')
capd_cohort = capd_cohort.join(churned_pattern, on = 'sourceId')

# left join
capd_cohort = capd_cohort.join(cohort_btcounts, on = 'sourceId', how = 'left')

display(capd_cohort)



# COMMAND ----------

display(capd_cohort)

# COMMAND ----------

churned_pattern = F.when(
    (F.col("IsChurned") == "Not Churned") & (F.col("row_number") == 1),
    "Not Churned"
).when(
    (F.col("IsChurned") == "Churned") & (F.col("row_number") == 1),
    "Churned"
).otherwise(None)
churned_pattern = most_recent_row.select("sourceId", churned_pattern.alias("Churned_Status"))
churned_pattern = churned_pattern.dropDuplicates(["sourceId"])
display(churned_pattern)


# COMMAND ----------



# COMMAND ----------



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
