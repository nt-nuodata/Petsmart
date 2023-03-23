# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MtdWeeks_0


df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        MtdWeekDt AS MtdWeekDt,
        LyrMtdWeekDt AS LyrMtdWeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MtdWeeks""")

df_0.createOrReplaceTempView("MtdWeeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MtdWeeks_1


df_1=spark.sql("""
    SELECT
        mtd.WeekDt,
        mtd.MtdWeekDt,
        mtd.LyrMtdWeekDt 
    FROM
        MtdWeeks mtd,
        Weeks w 
    WHERE
        mtd.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_MtdWeeks_1")

# COMMAND ----------
# DBTITLE 1, MTD_WEEKS


spark.sql("""INSERT INTO MTD_WEEKS SELECT WeekDt AS WEEK_DT,
MtdWeekDt AS MTD_WEEK_DT,
LyrMtdWeekDt AS LYR_MTD_WEEK_DT FROM SQ_Shortcut_to_MtdWeeks_1""")

# COMMAND ----------
# DBTITLE 1, MTD_WEEKS


spark.sql("""INSERT INTO MTD_WEEKS SELECT WeekDt AS WEEK_DT,
MtdWeekDt AS MTD_WEEK_DT,
LyrMtdWeekDt AS LYR_MTD_WEEK_DT FROM SQ_Shortcut_to_MtdWeeks_1""")