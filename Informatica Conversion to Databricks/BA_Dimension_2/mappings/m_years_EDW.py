# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Years_0


df_0=spark.sql("""
    SELECT
        FiscalYr AS FiscalYr,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Years""")

df_0.createOrReplaceTempView("Years_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Years_1


df_1=spark.sql("""
    SELECT
        FiscalYr 
    FROM
        Years 
    WHERE
        FiscalYr = DATEPART(YY, GETDATE()) + 2""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Years_1")

# COMMAND ----------
# DBTITLE 1, YEARS


spark.sql("""INSERT INTO YEARS SELECT FISCAL_YR AS FISCAL_YR FROM SQ_Shortcut_to_Years_1""")