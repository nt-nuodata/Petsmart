# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Wk52Weeks_0


df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        Wk52WeekDt AS Wk52WeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Wk52Weeks""")

df_0.createOrReplaceTempView("Wk52Weeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Wk52Weeks_1


df_1=spark.sql("""
    SELECT
        wk52.WeekDt,
        wk52.Wk52WeekDt 
    FROM
        Wk52Weeks wk52,
        Weeks w 
    WHERE
        wk52.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Wk52Weeks_1")

# COMMAND ----------
# DBTITLE 1, WK52_WEEKS


spark.sql("""INSERT INTO WK52_WEEKS SELECT WeekDt AS WEEK_DT,
Wk52WeekDt AS WK52_WEEK_DT FROM SQ_Shortcut_to_Wk52Weeks_1""")