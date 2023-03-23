# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, Wk16Weeks_0

df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        Wk16WeekDt AS Wk16WeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Wk16Weeks""")

df_0.createOrReplaceTempView("Wk16Weeks_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_Wk16Weeks_1

df_1=spark.sql("""
    SELECT
        wk16.WeekDt,
        wk16.Wk16WeekDt 
    FROM
        Wk16Weeks wk16,
        Weeks w 
    WHERE
        Wk16.WeekDt = w.WeekDt 
        AND W.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Wk16Weeks_1")

# COMMAND ----------

# DBTITLE 1, WK16_WEEKS

spark.sql("""INSERT INTO WK16_WEEKS SELECT WeekDt AS WEEK_DT,
Wk16WeekDt AS WK16_WEEK_DT FROM SQ_Shortcut_to_Wk16Weeks_1""")
