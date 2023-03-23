# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, MtdDays_0

df_0=spark.sql("""
    SELECT
        DayDt AS DayDt,
        MtdDayDt AS MtdDayDt,
        LyrMtdDayDt AS LyrMtdDayDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MtdDays""")

df_0.createOrReplaceTempView("MtdDays_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_MtdDays_1

df_1=spark.sql("""
    SELECT
        mtd.DayDt,
        mtd.MtdDayDt,
        mtd.LyrMtdDayDt 
    FROM
        MtdDays mtd,
        Days d 
    WHERE
        mtd.DayDt = d.DayDt 
        AND d.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_MtdDays_1")

# COMMAND ----------

# DBTITLE 1, MTD_DAYS

spark.sql("""INSERT INTO MTD_DAYS SELECT DayDt AS DAY_DT,
MtdDayDt AS MTD_DAY_DT,
LyrMtdDayDt AS LYR_MTD_DAY_DT FROM SQ_Shortcut_to_MtdDays_1""")

# COMMAND ----------

# DBTITLE 1, MTD_DAYS

spark.sql("""INSERT INTO MTD_DAYS SELECT DayDt AS DAY_DT,
MtdDayDt AS MTD_DAY_DT,
LyrMtdDayDt AS LYR_MTD_DAY_DT FROM SQ_Shortcut_to_MtdDays_1""")
