# Databricks notebook source
from pyspark.sql.functions import from_json, col, round, when, sum, expr, date_sub, current_date, lit, first, mode, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType, MapType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql.functions import from_json, schema_of_json, to_utc_timestamp, explode
from pyspark.sql.types import StringType, MapType
import json

parsed_json = spark.sql("""
    SELECT
        e.payload.sourceId,
        from_json(e.payload.value, 'map<string, string>') AS mypd_map,
        get_json_object(e.payload.value, '$.storeDataString') as storeDataString,
        get_json_object(get_json_object(e.payload.value, '$.storeDataString'), '$.prescription') as prescription,
        e.timestamp as timestamp
    FROM prodeu.events e
    WHERE e.tenant = '2' AND e.type = 'capd-treatment-recorded'
""")

# create a placeholder array to hold the parsed data (dataArray)
dataArray = []
prescriptionArray = []
settingsArray = []

# execute the above sql query and loop over the result data.
for r in parsed_json.collect():
    # parse the storeDataString into a pyhton dictionary (this will do a deep parse)
    storedata = json.loads(r.storeDataString)
    storedata["sourceId"] = r.sourceId
    storedata["treatmentId"] = r.mypd_map["patientId"]
    storedata["treatmentId"] = r.mypd_map["treatmentId"]
    storedata["treatmentTime"] = r.mypd_map["treatmentTime"]
    storedata["patientRegion"] = r.mypd_map["patientRegion"]
    storedata["patientTimeZone"] = r.mypd_map["patientTimeZone"]
    storedata["deviceSoftwareVersion"] = r.mypd_map["deviceSoftwareVersion"]
    storedata["timestamp"] = r.timestamp
    storedata["prescription"]["sourceId"] = r.sourceId
    storedata["prescription"]["treatmentId"] = r.mypd_map["treatmentId"]
    storedata["prescription"]["patientSettings"]["sourceId"] = r.sourceId
    storedata["prescription"]["patientSettings"]["treatmentId"] = r.mypd_map["treatmentId"]
    # re-create the object in parsed_json
    dataArray.append(
        storedata
    )
    prescription = storedata.pop("prescription")
    prescriptionArray.append(
        prescription
    )
    settingsArray.append(
        prescription.pop("patientSettings")
    )

# create a new pyspark data frame from the dataArray 
df = spark.createDataFrame(dataArray)
df2 = spark.createDataFrame(settingsArray)
df3 = spark.createDataFrame(prescriptionArray)

display(df)
display(df2)
display(df3)

exploded_json = df.join(df2,["sourceId"]).join(df3,["sourceId"])

display(exploded_json)


# COMMAND ----------

df.write.mode("overwrite").saveAsTable('prodeu.mypd_treatment')
df2.write.mode("overwrite").saveAsTable('prodeu.mypd_prescription')
df3.write.mode("overwrite").saveAsTable('prodeu.mypd_setting')
#exploded_json.write.mode("overwrite").saveAsTable('prodeu.mypd_data')

# COMMAND ----------

# 
mypd_btlog_cleaned = spark.sql(f"""
SELECT
  SUBSTRING(eventClientId, 3) AS sourceId,
  *
 
FROM
  prodeu.btlogs;

""")
display(mypd_btlog_cleaned)

mypd_btlog_cleaned.write.mode("overwrite").saveAsTable('prodeu.mypd_btlog_cleaned')

# COMMAND ----------

# MyPD User timezone table
mypd_timezone = spark.sql(f"""
SELECT 

distinct
e.payload.sourceId,
first(e.payload.vital.patientTimeZone) as patientTimeZone
FROM prodeu.events e
WHERE e.type = 'vital-recorded'
group by sourceId
""")
display(mypd_timezone)
mypd_timezone.write.mode("overwrite").saveAsTable('prodeu.mypd_timezone')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct patientTimeZone from prodeu.mypd_timezone

# COMMAND ----------


