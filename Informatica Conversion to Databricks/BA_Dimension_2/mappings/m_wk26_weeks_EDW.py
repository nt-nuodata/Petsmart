# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Wk26Weeks_0


df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        Wk26WeekDt AS Wk26WeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Wk26Weeks""")

df_0.createOrReplaceTempView("Wk26Weeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Wk26Weeks_1


df_1=spark.sql("""
    SELECT
        wk26.WeekDt,
        wk26.Wk26WeekDt 
    FROM
        Wk26Weeks wk26,
        Weeks w 
    WHERE
        Wk26.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Wk26Weeks_1")

# COMMAND ----------
# DBTITLE 1, WK26_WEEKS


spark.sql("""INSERT INTO WK26_WEEKS SELECT WeekDt AS WEEK_DT,
Wk26WeekDt AS WK26_WEEK_DT FROM SQ_Shortcut_to_Wk26Weeks_1""")