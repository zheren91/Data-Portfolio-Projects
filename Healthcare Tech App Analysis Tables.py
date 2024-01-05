# Databricks notebook source
from pyspark.sql.functions import from_json, col, round, when, sum, expr, date_sub, current_date, lit, first, mode, count,date_trunc,lead, lag, to_date, mode, from_utc_timestamp, to_utc_timestamp, hour
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

capd_vitals_prescription = spark.sql(f"""
    SELECT
        date(tmp.vital_date) as VitalDate,
        tmp.treatmentsourceId AS sourceId,
        tmp.eventsourceId AS events_sourceId,
        z.patientTimeZone as patientTimeZone,
        e.timestamp as event_timestamp,
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
            ROW_NUMBER() OVER (PARTITION BY t.sourceId, e.payload.vital.id ORDER BY e.payload.vital.timestamp DESC) AS row_number
        FROM prodeu.mypd_treatment t
        LEFT JOIN prodeu.events e ON e.payload.sourceId = t.sourceId AND DATE(e.payload.vital.timestamp) = DATE(t.treatmentTime)
    ) tmp
    LEFT JOIN prodeu.events e on e.payload.sourceId = tmp.treatmentsourceId and e.payload.vital.id = tmp.vitalId
    LEFT JOIN prodeu.mypd_prescription prescription ON prescription.treatmentId = tmp.treatmentId AND prescription.sourceId = tmp.treatmentsourceId 
    LEFT JOIN prodeu.mypd_timezone z on e.payload.sourceId = z.sourceId
    WHERE tmp.row_number = 1
    AND e.type = 'vital-recorded'
    AND tmp.treatmentsourceId NOT IN {format(tuple(NotIn))}
    AND tmp.eventsourceId NOT IN {format(tuple(NotIn))}
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') != 'PRD-3'
    GROUP BY tmp.treatmentsourceId, tmp.vitalId, tmp.treatmentId, tmp.treatmentsourceId, tmp.eventsourceId, patientTimeZone, tmp.type, tmp.vital_date, tmp.treatmentTime, event_timestamp, tmp.treatmentDevice,
             prescription.dayOfTheWeek, prescription.BPCollectionOrder, PulseOrder, GlucoseOrder,
             TempOrder, WeightOrder, prescription.urineOutput, e.type, region, e.payload.vital.source.type
    ORDER BY vital_date DESC
""")


# drop duplicates (duplicate vitalid, and duplicate eventtimestamp), do it separately so it removes all duplicates
capd_vitals_prescription = capd_vitals_prescription.dropDuplicates(["sourceId", "vitalId"])
capd_vitals_prescription = capd_vitals_prescription.dropDuplicates(["sourceId", "event_timestamp"])
#display(capd_vitals_prescription)


#---- convert patient time zone

capd_vitals_prescription = capd_vitals_prescription.withColumn(
    "patient_event_timestamp",
    from_utc_timestamp(col("event_timestamp"), "UTC")
).withColumn(
    "patient_event_timestamp",
    from_utc_timestamp(col("patient_event_timestamp"), col("patientTimeZone"))
)


# Create a new column for patient_event_timestamp 


capd_vitals_prescription = capd_vitals_prescription.withColumn(
    "patient_event_timestamp",
    from_utc_timestamp(col("event_timestamp"), col("patientTimeZone"))
)
# ------




capd_vitals_prescription = capd_vitals_prescription.withColumn(
    "PatientVitalDate",
    expr("date_sub(patient_event_timestamp, if(hour(patient_event_timestamp) < 3, 1, 0))")
)

# Format the 'PatientVitalDate' column to 'yyyy-mm-dd'
capd_vitals_prescription = capd_vitals_prescription.withColumn(
    "PatientVitalDate",
    col("PatientVitalDate").cast("date").cast("string")
)
# drop existing vitaldate
capd_vitals_prescription = capd_vitals_prescription.drop("VitalDate")
# create new vitaldate based on corrected patient timezone
capd_vitals_prescription = capd_vitals_prescription.withColumn(
    "VitalDate",
    col("PatientVitalDate")
)

# Display the updated DataFrame
# this already fixed the timestamp by adding a local patient timestamp
display(capd_vitals_prescription)


#----

from pyspark.sql import functions as F

# Pivot the VitalType column
pivoted_capd_vitals_prescription = capd_vitals_prescription.groupBy(
    'VitalDate', 'treatmentTime', 'event_timestamp','sourceId', 'events_sourceId', 'patientTimeZone', 'type', 'region','ConnectionType','vital_recorded', 'treatmentDevice',
    'vitalId', 'treatmentId', 'dayOfTheWeek', 'BPCollectionOrder', 'PulseOrder', 'GlucoseOrder', 'TempOrder', 'WeightOrder', 'urineOutput'
).pivot('VitalType').agg(F.first('VitalType'))  # Modify the aggregation function based on your requirements

# Display the pivoted DataFrame
display(pivoted_capd_vitals_prescription)

capd_vitals_prescription.write.mode("overwrite").saveAsTable('prodeu.mypd_capd')
pivoted_capd_vitals_prescription.write.mode("overwrite").saveAsTable('prodeu.mypd_pivot_capd')

# COMMAND ----------

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
#display(capd_daily_missed)

# Display the average missed vitals on a weekly basis
#display(capd_weekly_missed)

# Display the average missed vitals on a monthly basis
#display(capd_monthly_missed)


# COMMAND ----------

capd_weekly_missed_updated = capd_weekly_missed.withColumn('BP_Miss_Rate', round(col('TotalMissed_BP') / col('TotalSupposed_BP'), 2)) \
    .withColumn('Pulse_Miss_Rate', round(col('TotalMissed_Pulse') / col('TotalSupposed_Pulse'), 2)) \
    .withColumn('Glucose_Miss_Rate', round(col('TotalMissed_Glucose') / col('TotalSupposed_Glucose'), 2)) \
    .withColumn('Temp_Miss_Rate', round(col('TotalMissed_Temp') / col('TotalSupposed_Temp'), 2)) \
    .withColumn('Weight_Miss_Rate', round(col('TotalMissed_Weight') / col('TotalSupposed_Weight'), 2))


capd_monthly_missed_updated = capd_monthly_missed.withColumn('BP_Miss_Rate', round(col('TotalMissed_BP') / col('TotalSupposed_BP'), 2)) \
    .withColumn('Pulse_Miss_Rate', round(col('TotalMissed_Pulse') / col('TotalSupposed_Pulse'), 2)) \
    .withColumn('Glucose_Miss_Rate', round(col('TotalMissed_Glucose') / col('TotalSupposed_Glucose'), 2)) \
    .withColumn('Temp_Miss_Rate', round(col('TotalMissed_Temp') / col('TotalSupposed_Temp'), 2)) \
    .withColumn('Weight_Miss_Rate', round(col('TotalMissed_Weight') / col('TotalSupposed_Weight'), 2))

capd_daily_missed_updated = capd_daily_missed.withColumn('BP_Miss_Rate', round(col('TotalMissed_BP') / col('TotalSupposed_BP'), 2)) \
    .withColumn('Pulse_Miss_Rate', round(col('TotalMissed_Pulse') / col('TotalSupposed_Pulse'), 2)) \
    .withColumn('Glucose_Miss_Rate', round(col('TotalMissed_Glucose') / col('TotalSupposed_Glucose'), 2)) \
    .withColumn('Temp_Miss_Rate', round(col('TotalMissed_Temp') / col('TotalSupposed_Temp'), 2)) \
    .withColumn('Weight_Miss_Rate', round(col('TotalMissed_Weight') / col('TotalSupposed_Weight'), 2))




# COMMAND ----------

capd_weekly_missed_updated.write.mode("overwrite").saveAsTable('mypd.capd_weekly_missed_updated')
capd_monthly_missed_updated.write.mode("overwrite").saveAsTable('mypd.capd_monthly_missed_updated')
capd_df.write.mode("overwrite").saveAsTable('mypd.capd_df')

# COMMAND ----------


# When supposed = taken for all vitals then 'Correctly recorded all vitals'
# when some or all supposed > 0, but the actual taken does not match to supposed to be taken, only some actual taken matches to supposed taken then 'Only recorded some vitals'
# when total taken > supposed taken then 'Incorrectly taken vitals, took more than needed'
# when supposed taken are > 0 but all actual taken = 0 then 'did not record at all'


capd_daily_missed_updated = capd_daily_missed_updated.withColumn('Status',
    when(
        (col('TotalSupposed_BP') == col('TotalTaken_BP')) | (col('TotalSupposed_BP') < col('TotalTaken_BP')) &
        (col('TotalSupposed_Pulse') == col('TotalTaken_Pulse')) | (col('TotalSupposed_Pulse') < col('TotalTaken_Pulse')) &
        (col('TotalSupposed_Glucose') == col('TotalTaken_Glucose')) | (col('TotalSupposed_Glucose') < col('TotalTaken_Glucose')) &
        (col('TotalSupposed_Temp') == col('TotalTaken_Temp')) | (col('TotalSupposed_Temp') < col('TotalTaken_Temp')) &
        (col('TotalSupposed_Weight') == col('TotalTaken_Weight')) | (col('TotalSupposed_Weight') < col('TotalTaken_Weight')),
        'Correctly Recorded All Vitals'
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
#display(capd_daily_missed_updated)

# Due to large data results, not all results are displayed (Databricks warning)
#capd_daily_missed_updated.write.mode("overwrite").saveAsTable('mypd.capd_daily_missed_updated')

# COMMAND ----------


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


capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsActive',
    F.when(
        (capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        (capd_daily_missed_status['continuous_inactive_days'] <= 7) | 
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All'),
        'Active'
    ).otherwise('Inactive')
)

# Label rows as 'Active' or 'Inactive' based on the defined conditions
#capd_daily_missed_status = capd_daily_missed_status.withColumn(
 #   'IsActiveAtLeastTwo',
  #  F.when(
   #     (capd_daily_missed_status['continuous_inactive_days'].isNull()) |
    #    ((capd_daily_missed_status['continuous_inactive_days'] <= 5) &
     #   (capd_daily_missed_status['Status'] != 'Did not Record Vital At All')),
      #  'Active'
    #).otherwise('Inactive')
#)

# ----------------- at least 2 days a week

# Define a window specification partitioned by 'sourceId' and ordered by 'VitalDate' in descending order
windowSpec = Window.partitionBy('sourceId').orderBy('VitalDate').rowsBetween(-6, 0)

# Add a new column 'DidNotRecordDays' that counts the number of days where 'Status' is 'Did not Record Vital At All'
capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'DidNotRecordDays',
    F.sum(F.when(col('Status') == 'Did not Record Vital At All', 1).otherwise(0)).over(windowSpec)
)

# Label rows as 'Active' or 'Inactive' based on the modified conditions
capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsActiveAtLeastTwo',
    F.when(
        #(capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        ((capd_daily_missed_status['continuous_inactive_days'] <= 5) &
        (capd_daily_missed_status['DidNotRecordDays'] <= 5) &
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All')),
        'Active'
    ).otherwise('Inactive')
)

capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsActiveAtLeastThree',
    F.when(
        #(capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        ((capd_daily_missed_status['continuous_inactive_days'] <= 4) &
        (capd_daily_missed_status['DidNotRecordDays'] <= 4) &
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All')),
        'Active'
    ).otherwise('Inactive')
)

capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsActiveAtLeastFour',
    F.when(
        #(capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        ((capd_daily_missed_status['continuous_inactive_days'] <= 3) &
        (capd_daily_missed_status['DidNotRecordDays'] <= 3) &
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All')),
        'Active'
    ).otherwise('Inactive')
)


capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsActiveAtLeastFive',
    F.when(
        #(capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        ((capd_daily_missed_status['continuous_inactive_days'] <= 2) &
        (capd_daily_missed_status['DidNotRecordDays'] <= 2) &
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All')),
        'Active'
    ).otherwise('Inactive')
)

capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsActiveAtLeastSix',
    F.when(
        #(capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        ((capd_daily_missed_status['continuous_inactive_days'] <= 1) &
        (capd_daily_missed_status['DidNotRecordDays'] <= 1) &
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All')),
        'Active'
    ).otherwise('Inactive')
)

capd_daily_missed_status = capd_daily_missed_status.withColumn(
    'IsActiveAtLeastSeven',
    F.when(
        #(capd_daily_missed_status['continuous_inactive_days'].isNull()) |
        ((capd_daily_missed_status['continuous_inactive_days'] == 0) &
        (capd_daily_missed_status['DidNotRecordDays'] == 0) &
        (capd_daily_missed_status['Status'] != 'Did not Record Vital At All')),
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

#display(capd_daily_missed_updated_connectiontypes)

# COMMAND ----------

# Calculate mode of ConnectionType while excluding rows with temperature is null
connection_type_df = capd_df.filter(col('temperature').isNull()) \
    .groupBy('sourceId', 'VitalDate') \
    .agg(mode(col('ConnectionType')).alias('ConnectionType'))

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
#display(capd_daily_missed_updated_connectiontypes)

# COMMAND ----------


# streaks and active days 
# capd_daily_missed_updated_connectiontypes
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
#display(capd_streak_result)



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
                                     when(col('percentage_manual') >= 0.85, 'Heavy Manual')
                                     .when((col('percentage_manual') >= 0.56) & (col('percentage_manual') < 0.85), 'Regular Manual')
                                     .when((col('percentage_manual') >= 0.45) & (col('percentage_manual') <= 0.55), 'Approximately Equally Used')
                                     .when((col('percentage_bt') >= 0.56) & (col('percentage_bt') < 0.85), 'Regular Bluetooth')
                                     .when(col('percentage_bt') >= 0.85, 'Heavy Bluetooth')
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

display(capd_cohort)



# COMMAND ----------

# Group by 'sourceId' and calculate the counts for 'Active' and 'Inactive'
count_by_status = capd_daily_missed_updated_connectiontypes.groupBy('sourceId').agg(
    F.sum(F.when(F.col('IsActiveAtLeastTwo') == 'Active', 1).otherwise(0)).alias('ActiveTwoDaysCount'),
    F.sum(F.when(F.col('IsActiveAtLeastTwo') == 'Inactive', 1).otherwise(0)).alias('InactiveTwoDaysCount'),
    F.sum(F.when(F.col('IsActiveAtLeastSeven') == 'Active', 1).otherwise(0)).alias('ActiveSevenDaysCount'),
    F.sum(F.when(F.col('IsActiveAtLeastSeven') == 'Inactive', 1).otherwise(0)).alias('InactiveSevenDaysCount'),
    F.sum(F.when(F.col('Status') == 'Correctly Recorded All Vitals', 1).otherwise(0)).alias('Compliance'),
    F.sum(F.when(F.col('Status') != 'Correctly Recorded All Vitals', 1).otherwise(0)).alias('NonCompliance')
)

# Calculate active percentages
count_by_status = count_by_status.withColumn(
    'ActiveTwoDaysPercentage',
    F.round((F.col('ActiveTwoDaysCount') / (F.col('ActiveTwoDaysCount') + F.col('InactiveTwoDaysCount'))) * 100, 2)
)

count_by_status = count_by_status.withColumn(
    'ActiveSevenDaysPercentage',
    F.round((F.col('ActiveSevenDaysCount') / (F.col('ActiveSevenDaysCount') + F.col('InactiveSevenDaysCount'))) * 100, 2)
)

count_by_status = count_by_status.withColumn(
    'ComplianceRate',
    F.round((F.col('Compliance') / (F.col('Compliance') + F.col('NonCompliance'))) * 100, 2)
)


count_by_status = count_by_status.select('sourceId',
                                         'ActiveTwoDaysPercentage',
                                         'ActiveSevenDaysPercentage',
                                         'ComplianceRate')
# Display the result
#display(count_by_status)

capd_cohort = capd_cohort.join(count_by_status, on = 'sourceId')






display(capd_cohort)

# compliance rate = days correctly recorded all vitals / total number of days

# COMMAND ----------

# capd table
mypd_capd = spark.sql(f""" 
select 
sourceId,
event_timestamp,
ConnectionType,
VitalType,
Region
from prodeu.mypd_capd
""")

# ------ defined cohort

cohort_sourceIds = capd_daily_missed_updated_connectiontypes.select('sourceId').distinct()
cohort_sourceIds_list = [row['sourceId'] for row in cohort_sourceIds.collect()]

#----- btlogs of cohort patients
mypd_btlog = spark.sql(f""" 
select  
sourceId,
timestamp as event_timestamp,
name as ConnectionType,
null as VitalType,
null as Region
from prodeu.mypd_btlog_cleaned
where sourceId IN {format(tuple(cohort_sourceIds_list))}
AND name IN (
    'bluetooth_reading_initiated',
    'bluetooth_reading_error',
    'bluetooth_reading_successful',
    'bluetooth_pairing_initiated',
    'bluetooth_pairing_error',
    'bluetooth_pairing_successful'
)
""")
#display(mypd_capd)
#display(mypd_btlog)

capd_union = mypd_capd.union(mypd_btlog)
display(capd_union)

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

#----
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



#----

from pyspark.sql.functions import avg, round

# of the days that they encountered errors, how many tries before they can successfully record vitals USING BT??
# Calculate average count per sourceId and round to 2 decimal places
average_count_per_sourceId = count_per_date.groupBy("sourceId").agg(round(avg("count"), 2).alias("avg_bt_tries_count"))

# Display the result
display(average_count_per_sourceId)


# COMMAND ----------

cohort_btcounts = spark.sql(f"""
    SELECT 
    DISTINCT sourceId, 
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_error') as reading_error,
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_successful') as reading_successful,
    COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_initiated') as reading_initiated,
    round(COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_error')/COUNT(sourceId) FILTER(WHERE name = 'bluetooth_reading_initiated'),2) as reading_error_rate
    FROM prodeu.mypd_btlog_cleaned
    WHERE
    sourceId IN {format(tuple(cohort_sourceIds_list))}

    GROUP BY sourceId
""")
display(cohort_btcounts)

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
#display(capd_bt_next_row)


# Group by sourceId and find the mode of next_ConnectionType
capd_bt_next_row = capd_bt_next_row.groupBy("sourceId").agg(mode(col("next_ConnectionType")).alias("choice_after_reading_error"))

# Display the result
display(capd_bt_next_row)

# COMMAND ----------

display(cohort_btcounts)
display(average_count_per_sourceId)
display(capd_bt_next_row)
display(capd_cohort)

# COMMAND ----------

capd_test_cohort = capd_cohort.join(cohort_btcounts, on = 'sourceId', how = 'left')
display(capd_test_cohort)
capd_test_cohort = capd_test_cohort.join(average_count_per_sourceId, on = 'sourceId', how = 'left')
display(capd_test_cohort)
capd_test_cohort = capd_test_cohort.join(capd_bt_next_row, on = 'sourceId', how = 'left')
display(capd_test_cohort)

# COMMAND ----------



# COMMAND ----------

# store as table
# capd_test_cohort
capd_test_cohort.write.mode("overwrite").saveAsTable('mypd.capd_cohort')
capd_daily_missed_updated_connectiontypes_streak.write.mode("overwrite").saveAsTable('mypd.capd_daily_missed_updated_connectiontypes_streak')

# COMMAND ----------



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
# cumulative churned number?

# COMMAND ----------

# capd table
mypd_capd = spark.sql(f""" 
select 
sourceId,
event_timestamp,
ConnectionType,
VitalType,
Region
from prodeu.mypd_capd
""")

# ------ defined cohort

cohort_sourceIds = capd_daily_missed_updated_connectiontypes.select('sourceId').distinct().collect()
sourceIds_list = [row.sourceId for row in cohort_sourceIds]
#----- btlogs of cohort patients
mypd_btlog = spark.sql(f""" 
    select  
    sourceId,
    timestamp as event_timestamp,
    name as ConnectionType,
    null as VitalType,
    null as Region
from prodeu.mypd_btlog_cleaned
where sourceId IN  {format(tuple(sourceIds_list))}
AND name IN (
    'bluetooth_reading_initiated',
    'bluetooth_reading_error',
    'bluetooth_reading_successful',
    'bluetooth_pairing_initiated',
    'bluetooth_pairing_error',
    'bluetooth_pairing_successful'
)
""")
#display(mypd_capd)
#display(mypd_btlog)

capd_union = mypd_capd.union(mypd_btlog)
#display(capd_union)


# COMMAND ----------

capd_union.write.mode("overwrite").saveAsTable('mypd.capd_union')

# COMMAND ----------

# churned and activated count 
churned_and_reactivated_capd = spark.sql(f"""
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
        END) AS num_churned_capd_LatinAmerica,

        COUNT(DISTINCT CASE
            WHEN t1.patientRegion = 'PRD-4' AND NOT EXISTS (
                SELECT 1 FROM prodeu.mypd_treatment t2
                WHERE t2.sourceId = t1.sourceId
                    AND t2.treatmentTime > t1.treatmentTime
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 30)
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
                    AND t2.treatmentTime <= DATE_ADD(t1.treatmentTime, 30)
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
    AND t1.sourceId IN {format(tuple(cohort_sourceIds_list))}
    GROUP BY activity_month
    HAVING DATEDIFF(CURRENT_DATE(), MAX(t1.treatmentTime)) >= 30
""")

display(churned_and_reactivated_capd)

# COMMAND ----------

churned_and_reactivated_capd.write.mode("overwrite").saveAsTable('mypd.churned_and_reactivated_capd')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct sourceId)
# MAGIC from prodeu.mypd_pivot_capd

# COMMAND ----------

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
    AND get_json_object(e.payload.vital.sourceMetadata, '$.patientRegion') != 'PRD-3' -- Exclude PRD-3
    GROUP BY tmp.vitalId, tmp.treatmentId, tmp.treatmentsourceId, tmp.eventsourceId, tmp.type, tmp.vital_date, tmp.treatmentTime, tmp.treatmentDevice,
             prescription.dayOfTheWeek, prescription.BPCollectionOrder, PulseOrder, GlucoseOrder,
             TempOrder, WeightOrder, prescription.urineOutput, e.type, region, e.payload.vital.source.type
    ORDER BY vital_date DESC
""")
#display(capd_vitals_prescription)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct e.payload.sourceId
# MAGIC from prodeu.events e
# MAGIC where get_json_object(e.payload.vital.sourceMetadata, '$.treatmentDevice') = 'capd'

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from prodeu.mypd_prescription

# COMMAND ----------

test = capd_vitals_prescription.select('sourceId').distinct()
display(test)

# COMMAND ----------


