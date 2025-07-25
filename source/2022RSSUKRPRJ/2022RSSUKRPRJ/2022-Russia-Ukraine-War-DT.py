# Databricks notebook source
# DBTITLE 1,Accessing the data and displaying
# Define your ADLS Gen2 path
raw_path = "abfss://2022-russia-ukraine-war@2022rssukrproject.dfs.core.windows.net/raw/"
display(dbutils.fs.ls(raw_path))

# COMMAND ----------

# DBTITLE 1,Data Analysis 1
rss_equipmentLosses_df = spark.read.format("csv").option("header", "true").load(raw_path + "russia_losses_equipment.csv")
display(rss_equipmentLosses_df)
rss_equipmentLosses_df.printSchema() # datatypes are wrong
# Date column is not in the correct format - should be date type
# All columns except "greatest losses direction" are not in the right format - should be int type
# Decimals dont make sense to these columns - should be int type

# COMMAND ----------

# DBTITLE 1,Data Transformation 1
from pyspark.sql.functions import *
rss_equipmentLosses_clean_df = rss_equipmentLosses_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("day", col("day").cast("int")) \
    .withColumn("aircraft", col("aircraft").cast("int")) \
    .withColumn("helicopter", col("helicopter").cast("int")) \
    .withColumn("tank", col("tank").cast("int")) \
    .withColumn("APC", col("APC").cast("int")) \
    .withColumn("field artillery", col("field artillery").cast("int")) \
    .withColumn("MRL", col("MRL").cast("int")) \
    .withColumn("military auto", col("military auto").cast("int")) \
    .withColumn("fuel tank", col("fuel tank").cast("int")) \
    .withColumn("drone", col("drone").cast("int")) \
    .withColumn("naval ship", col("naval ship").cast("int")) \
    .withColumn("anti-aircraft warfare", col("anti-aircraft warfare").cast("int")) \
    .withColumn("special equipment", col("special equipment").cast("int")) \
    .withColumn("mobile SRBM system", col("mobile SRBM system").cast("int")) \
    .withColumn("greatest losses direction", col("greatest losses direction").cast("string")) \
    .withColumn("vehicles and fuel tanks", col("vehicles and fuel tanks").cast("int")) \
    .withColumn("cruise missiles", col("cruise missiles").cast("int")) \
    .withColumn("submarines", col("submarines").cast("int"))
display(rss_equipmentLosses_clean_df)

# COMMAND ----------

# DBTITLE 1,Data Analysis 2
rss_equipmentLossCorrection_df = spark.read.format("csv").option("header", "true").load(raw_path + "russia_losses_equipment_correction.csv")
display(rss_equipmentLossCorrection_df)
rss_equipmentLossCorrection_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Data Transformation 2
from pyspark.sql.functions import *

rss_equipmentLossCorrection_clean_df = rss_equipmentLossCorrection_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("day", col("day").cast("int")) \
    .withColumn("aircraft", col("aircraft").cast("int")) \
    .withColumn("helicopter", col("helicopter").cast("int")) \
    .withColumn("tank", col("tank").cast("int")) \
    .withColumn("APC", col("APC").cast("int")) \
    .withColumn("field artillery", col("field artillery").cast("int")) \
    .withColumn("MRL", col("MRL").cast("int")) \
    .withColumn("drone", col("drone").cast("int")) \
    .withColumn("naval ship", col("naval ship").cast("int")) \
    .withColumn("submarines", col("submarines").cast("int")) \
    .withColumn("anti-aircraft warfare", col("anti-aircraft warfare").cast("int")) \
    .withColumn("special equipment", col("special equipment").cast("int")) \
    .withColumn("vehicles and fuel tanks", col("vehicles and fuel tanks").cast("int")) \
    .withColumn("cruise missiles", col("cruise missiles").cast("int")) \
    .withColumn("personnel", col("personnel").cast("int")) \
    .withColumnRenamed("date", "Date") \
    .withColumnRenamed("day", "Day") \
    .withColumnRenamed("aircraft", "Aircraft") \
    .withColumnRenamed("helicopter", "Helicopter") \
    .withColumnRenamed("tank", "Tank") \
    .withColumnRenamed("APC", "Armored Personnel Carrier") \
    .withColumnRenamed("field artillery", "Field Artillery") \
    .withColumnRenamed("MRL", "Multiple Rocket Launcher") \
    .withColumnRenamed("drone", "Drone") \
    .withColumnRenamed("naval ship", "Naval Ship") \
    .withColumnRenamed("submarines", "Submarine") \
    .withColumnRenamed("anti-aircraft warfare", "Anti-Aircraft Warfare") \
    .withColumnRenamed("special equipment", "Special Equipment") \
    .withColumnRenamed("vehicles and fuel tanks", "Vehicles and Fuel Tanks") \
    .withColumnRenamed("cruise missiles", "Cruise Missiles") \
    .withColumnRenamed("personnel", "Personnel")
# display(rss_equipmentLossCorrection_clean_df)

"""
Essentially what we are doing here is adding a column for each of the columns in the dataframe, and then setting the value of the column to True if the value is less than 0, False if the value is greater than 0, and None if the value is null
"""

list_of_columns = ["Day","Aircraft","Helicopter","Tank","Armored Personnel Carrier","Field Artillery","Multiple Rocket Launcher","Drone","Naval Ship","Submarine","Anti-Aircraft Warfare","Special Equipment","Vehicles and Fuel Tanks","Cruise Missiles","Personnel"]

for colName in list_of_columns:
    newColName = f"Invalid {colName}"
    rss_equipmentLossCorrection_clean_df = rss_equipmentLossCorrection_clean_df.withColumn(
        newColName, when(col(colName) < 0, True) \
            .when(col(colName) > 0, False) \
            .otherwise(None)
    )

display(rss_equipmentLossCorrection_clean_df)

# COMMAND ----------

# DBTITLE 1,Data Analysis 3
rss_personnelLosses_df = spark.read.format("csv").option("header", "true").load(raw_path + "russia_losses_personnel.csv")
display(rss_personnelLosses_df)
rss_personnelLosses_df.printSchema()

# COMMAND ----------

