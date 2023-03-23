# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, LyrMtdWeeks_0


df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        LyrMtdWeekDt AS LyrMtdWeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LyrMtdWeeks""")

df_0.createOrReplaceTempView("LyrMtdWeeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LyrMtdWeeks_1


df_1=spark.sql("""
    SELECT
        lyrm.WeekDt,
        lyrm.LyrMtdWeekDt 
    FROM
        LyrMtdWeeks lyrm,
        Weeks w 
    WHERE
        lyrm.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_LyrMtdWeeks_1")

# COMMAND ----------
# DBTITLE 1, LYR_MTD_WEEKS


spark.sql("""INSERT INTO LYR_MTD_WEEKS SELECT WeekDt AS WEEK_DT,
LyrMtdWeekDt AS LYR_MTD_WEEK_DT FROM SQ_Shortcut_to_LyrMtdWeeks_1""")

# COMMAND ----------
# DBTITLE 1, LYR_MTD_WEEKS


spark.sql("""INSERT INTO LYR_MTD_WEEKS SELECT WeekDt AS WEEK_DT,
LyrMtdWeekDt AS LYR_MTD_WEEK_DT FROM SQ_Shortcut_to_LyrMtdWeeks_1""")