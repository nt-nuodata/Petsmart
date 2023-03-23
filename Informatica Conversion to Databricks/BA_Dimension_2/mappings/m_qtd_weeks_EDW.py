# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, QtdWeeks_0

df_0=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        QtdWeekDt AS QtdWeekDt,
        LyrQtdWeekDt AS LyrQtdWeekDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        QtdWeeks""")

df_0.createOrReplaceTempView("QtdWeeks_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_QtdWeeks_1

df_1=spark.sql("""
    SELECT
        qtd.WeekDt,
        qtd.QtdWeekDt,
        qtd.LyrQtdWeekDt 
    FROM
        QtdWeeks qtd,
        Weeks w 
    WHERE
        qtd.WeekDt = w.WeekDt 
        AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_QtdWeeks_1")

# COMMAND ----------

# DBTITLE 1, QTD_WEEKS

spark.sql("""INSERT INTO QTD_WEEKS SELECT WeekDt AS WEEK_DT,
QtdWeekDt AS QTD_WEEK_DT,
LyrQtdWeekDt AS LYR_QTD_WEEK_DT FROM SQ_Shortcut_to_QtdWeeks_1""")
