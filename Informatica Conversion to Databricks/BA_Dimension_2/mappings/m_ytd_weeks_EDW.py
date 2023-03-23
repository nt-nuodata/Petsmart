# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, YtdWeeks_0


df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        YtdWeekDt AS YtdWeekDt,
        LyrYtdWeekDt AS LyrYtdWeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        YtdWeeks""")

df_0.createOrReplaceTempView("YtdWeeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_YtdWeeks_1


df_1=spark.sql("""
    SELECT
        ytd.WeekDt,
        ytd.YtdWeekDt,
        ytd.LyrYtdWeekDt 
    FROM
        YtdWeeks ytd,
        Weeks w 
    WHERE
        ytd.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_YtdWeeks_1")

# COMMAND ----------
# DBTITLE 1, YTD_WEEKS


spark.sql("""INSERT INTO YTD_WEEKS SELECT WeekDt AS WEEK_DT,
YtdWeekDt AS YTD_WEEK_DT,
LyrYtdWeekDt AS LYR_YTD_WEEK_DT FROM SQ_Shortcut_to_YtdWeeks_1""")