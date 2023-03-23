# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Wk13Weeks_0


df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        Wk13WeekDt AS Wk13WeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Wk13Weeks""")

df_0.createOrReplaceTempView("Wk13Weeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Wk13Weeks_1


df_1=spark.sql("""
    SELECT
        wk13.WeekDt,
        wk13.Wk13WeekDt 
    FROM
        Wk13Weeks wk13,
        Weeks w 
    WHERE
        wk13.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Wk13Weeks_1")

# COMMAND ----------
# DBTITLE 1, WK13_WEEKS


spark.sql("""INSERT INTO WK13_WEEKS SELECT WeekDt AS WEEK_DT,
Wk13WeekDt AS WK13_WEEK_DT FROM SQ_Shortcut_to_Wk13Weeks_1""")