# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, LqtrWeeks_0

df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        LqtrWeekDt AS LqtrWeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LqtrWeeks""")

df_0.createOrReplaceTempView("LqtrWeeks_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_LqtrWeeks_1

df_1=spark.sql("""
    SELECT
        lqtr.WeekDt,
        lqtr.LqtrWeekDt 
    FROM
        LqtrWeeks lqtr,
        Weeks w 
    WHERE
        lqtr.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_LqtrWeeks_1")

# COMMAND ----------

# DBTITLE 1, LQTR_WEEKS

spark.sql("""INSERT INTO LQTR_WEEKS SELECT WeekDt AS WEEK_DT,
LqtrWeekDt AS LQTR_WEEK_DT FROM SQ_Shortcut_to_LqtrWeeks_1""")

# COMMAND ----------

# DBTITLE 1, LQTR_WEEKS

spark.sql("""INSERT INTO LQTR_WEEKS SELECT WeekDt AS WEEK_DT,
LqtrWeekDt AS LQTR_WEEK_DT FROM SQ_Shortcut_to_LqtrWeeks_1""")
