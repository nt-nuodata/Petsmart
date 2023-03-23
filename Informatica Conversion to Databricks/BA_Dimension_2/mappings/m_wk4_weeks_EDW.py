# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Wk4Weeks_0


df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        Wk4WeekDt AS Wk4WeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Wk4Weeks""")

df_0.createOrReplaceTempView("Wk4Weeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Wk4Weeks_1


df_1=spark.sql("""
    SELECT
        wk4.WeekDt,
        wk4.Wk4WeekDt 
    FROM
        Wk4Weeks wk4,
        Weeks w 
    WHERE
        wk4.WeekDt = w.WeekDt 
        AND W.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Wk4Weeks_1")

# COMMAND ----------
# DBTITLE 1, WK4_WEEKS


spark.sql("""INSERT INTO WK4_WEEKS SELECT WeekDt AS WEEK_DT,
Wk4WeekDt AS WK4_WEEK_DT FROM SQ_Shortcut_to_Wk4Weeks_1""")