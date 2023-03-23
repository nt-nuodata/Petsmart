# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Wk8Weeks_0


df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        Wk8WeekDt AS Wk8WeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Wk8Weeks""")

df_0.createOrReplaceTempView("Wk8Weeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Wk8Weeks_1


df_1=spark.sql("""
    SELECT
        wk8.WeekDt,
        wk8.Wk8WeekDt 
    FROM
        Wk8Weeks wk8,
        Weeks w 
    WHERE
        wk8.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Wk8Weeks_1")

# COMMAND ----------
# DBTITLE 1, WK8_WEEKS


spark.sql("""INSERT INTO WK8_WEEKS SELECT WeekDt AS WEEK_DT,
Wk8WeekDt AS WK8_WEEK_DT FROM SQ_Shortcut_to_Wk8Weeks_1""")