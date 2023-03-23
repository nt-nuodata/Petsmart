# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, LwkDays_0

df_0=spark.sql("""
    SELECT
        LwkDayDt AS LwkDayDt,
        DayDt AS DayDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LwkDays""")

df_0.createOrReplaceTempView("LwkDays_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_LwkDays_1

df_1=spark.sql("""
    SELECT
        ld.LwkDayDt,
        ld.DayDt 
    FROM
        LwkDays ld,
        Days d 
    WHERE
        ld.DayDt = d.DayDt 
        AND d.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_LwkDays_1")

# COMMAND ----------

# DBTITLE 1, LWK_DAYS

spark.sql("""INSERT INTO LWK_DAYS SELECT LwkDayDt AS LWK_DAY_DT,
DayDt AS DAY_DT FROM SQ_Shortcut_to_LwkDays_1""")

# COMMAND ----------

# DBTITLE 1, LWK_DAYS

spark.sql("""INSERT INTO LWK_DAYS SELECT LwkDayDt AS LWK_DAY_DT,
DayDt AS DAY_DT FROM SQ_Shortcut_to_LwkDays_1""")
