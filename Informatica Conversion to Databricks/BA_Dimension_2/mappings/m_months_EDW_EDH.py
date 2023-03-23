# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Months_0


df_0=spark.sql("""
    SELECT
        FiscalMo AS FiscalMo,
        FiscalHalf AS FiscalHalf,
        FiscalMoName AS FiscalMoName,
        FiscalMoNameAbbr AS FiscalMoNameAbbr,
        FiscalMoNbr AS FiscalMoNbr,
        FiscalQtr AS FiscalQtr,
        FiscalQtrNbr AS FiscalQtrNbr,
        FiscalYr AS FiscalYr,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Months""")

df_0.createOrReplaceTempView("Months_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Months_1


df_1=spark.sql("""
    SELECT
        FiscalMo,
        FiscalHalf,
        FiscalMoName,
        FiscalMoNameAbbr,
        FiscalMoNbr,
        FiscalQtr,
        FiscalQtrNbr,
        FiscalYr 
    FROM
        Months 
    WHERE
        FiscalYr = DATEPART(YY, GETDATE()) + 2""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Months_1")

# COMMAND ----------
# DBTITLE 1, MONTHS


spark.sql("""INSERT INTO MONTHS SELECT FISCAL_MO AS FISCAL_MO,
FISCAL_HALF AS FISCAL_HALF,
FISCAL_MO_NAME AS FISCAL_MO_NAME,
FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
FISCAL_MO_NBR AS FISCAL_MO_NBR,
FISCAL_QTR AS FISCAL_QTR,
FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
FISCAL_YR AS FISCAL_YR FROM SQ_Shortcut_to_Months_1""")

# COMMAND ----------
# DBTITLE 1, MONTHS


spark.sql("""INSERT INTO MONTHS SELECT FiscalMo AS FISCAL_MO,
FiscalHalf AS FISCAL_HALF,
FiscalMoName AS FISCAL_MO_NAME,
FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
FiscalMoNbr AS FISCAL_MO_NBR,
FiscalQtr AS FISCAL_QTR,
FiscalQtrNbr AS FISCAL_QTR_NBR,
FiscalYr AS FISCAL_YR FROM SQ_Shortcut_to_Months_1""")