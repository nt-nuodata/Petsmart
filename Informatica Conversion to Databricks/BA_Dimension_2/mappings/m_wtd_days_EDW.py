# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, WtdDays_0


df_0=spark.sql("""
    SELECT
        DayDt AS DayDt,
        WtdDayDt AS WtdDayDt,
        LyrWtdDayDt AS LyrWtdDayDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        WtdDays""")

df_0.createOrReplaceTempView("WtdDays_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WtdDays_1


df_1=spark.sql("""
    SELECT
        wtd.DayDt,
        wtd.WtdDayDt,
        wtd.LyrWtdDayDt 
    FROM
        WtdDays wtd,
        Days d 
    WHERE
        wtd.DayDt = d.DayDt 
        AND d.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_WtdDays_1")

# COMMAND ----------
# DBTITLE 1, WTD_DAYS


spark.sql("""INSERT INTO WTD_DAYS SELECT DayDt AS DAY_DT,
WtdDayDt AS WTD_DAY_DT,
LyrWtdDayDt AS LYR_WTD_DAY_DT FROM SQ_Shortcut_to_WtdDays_1""")