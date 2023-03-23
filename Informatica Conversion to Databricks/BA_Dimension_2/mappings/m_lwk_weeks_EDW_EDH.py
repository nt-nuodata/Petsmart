# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, LwkWeeks_0


df_0=spark.sql("""
    SELECT
        LwkWeekDt AS LwkWeekDt,
        WeekDt AS WeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LwkWeeks""")

df_0.createOrReplaceTempView("LwkWeeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LwkWeeks_1


df_1=spark.sql("""
    SELECT
        lw.LwkWeekDt,
        lw.WeekDt 
    FROM
        LwkWeeks lw,
        Weeks w 
    WHERE
        lw.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_LwkWeeks_1")

# COMMAND ----------
# DBTITLE 1, LWK_WEEKS


spark.sql("""INSERT INTO LWK_WEEKS SELECT LwkWeekDt AS LWK_WEEK_DT,
WeekDt AS WEEK_DT FROM SQ_Shortcut_to_LwkWeeks_1""")

# COMMAND ----------
# DBTITLE 1, LWK_WEEKS


spark.sql("""INSERT INTO LWK_WEEKS SELECT LwkWeekDt AS LWK_WEEK_DT,
WeekDt AS WEEK_DT FROM SQ_Shortcut_to_LwkWeeks_1""")