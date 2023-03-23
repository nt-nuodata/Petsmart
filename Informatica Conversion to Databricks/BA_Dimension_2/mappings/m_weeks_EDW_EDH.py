# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, WEEKS_0

df_0=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        CAL_WK AS CAL_WK,
        CAL_WK_NBR AS CAL_WK_NBR,
        CAL_MO AS CAL_MO,
        CAL_MO_NBR AS CAL_MO_NBR,
        CAL_MO_NAME AS CAL_MO_NAME,
        CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
        CAL_QTR AS CAL_QTR,
        CAL_QTR_NBR AS CAL_QTR_NBR,
        CAL_HALF AS CAL_HALF,
        CAL_YR AS CAL_YR,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_WK_NBR AS FISCAL_WK_NBR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        FISCAL_QTR AS FISCAL_QTR,
        FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
        FISCAL_HALF AS FISCAL_HALF,
        FISCAL_YR AS FISCAL_YR,
        LYR_WEEK_DT AS LYR_WEEK_DT,
        LWK_WEEK_DT AS LWK_WEEK_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        WEEKS""")

df_0.createOrReplaceTempView("WEEKS_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_Weeks_1

df_1=spark.sql("""
    SELECT
        WeekDt,
        CalWk,
        CalWkNbr,
        CalMo,
        CalMoNbr,
        CalMoName,
        CalMoNameAbbr,
        CalQtr,
        CalQtrNbr,
        CalHalf,
        CalYr,
        FiscalWk,
        FiscalWkNbr,
        FiscalMo,
        FiscalMoNbr,
        FiscalMoName,
        FiscalMoNameAbbr,
        FiscalQtr,
        FiscalQtrNbr,
        FiscalHalf,
        FiscalYr,
        LyrWeekDt,
        LwkWeekDt 
    FROM
        Weeks 
    WHERE
        FiscalYr = DATEPART(YY, GETDATE()) + 2""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Weeks_1")

# COMMAND ----------

# DBTITLE 1, WEEKS

spark.sql("""INSERT INTO WEEKS SELECT WeekDt AS WEEK_DT,
CalWk AS CAL_WK,
CalWkNbr AS CAL_WK_NBR,
CalMo AS CAL_MO,
CalMoNbr AS CAL_MO_NBR,
CalMoName AS CAL_MO_NAME,
CalMoNameAbbr AS CAL_MO_NAME_ABBR,
CalQtr AS CAL_QTR,
CalQtrNbr AS CAL_QTR_NBR,
CalHalf AS CAL_HALF,
CalYr AS CAL_YR,
FiscalWk AS FISCAL_WK,
FiscalWkNbr AS FISCAL_WK_NBR,
FiscalMo AS FISCAL_MO,
FiscalMoNbr AS FISCAL_MO_NBR,
FiscalMoName AS FISCAL_MO_NAME,
FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
FiscalQtr AS FISCAL_QTR,
FiscalQtrNbr AS FISCAL_QTR_NBR,
FiscalHalf AS FISCAL_HALF,
FiscalYr AS FISCAL_YR,
LyrWeekDt AS LYR_WEEK_DT,
LwkWeekDt AS LWK_WEEK_DT FROM SQ_Shortcut_to_Weeks_1""")

# COMMAND ----------

# DBTITLE 1, WEEKS

spark.sql("""INSERT INTO WEEKS SELECT WEEK_DT AS WEEK_DT,
CAL_WK AS CAL_WK,
CAL_WK_NBR AS CAL_WK_NBR,
CAL_MO AS CAL_MO,
CAL_MO_NBR AS CAL_MO_NBR,
CAL_MO_NAME AS CAL_MO_NAME,
CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
CAL_QTR AS CAL_QTR,
CAL_QTR_NBR AS CAL_QTR_NBR,
CAL_HALF AS CAL_HALF,
CAL_YR AS CAL_YR,
FISCAL_WK AS FISCAL_WK,
FISCAL_WK_NBR AS FISCAL_WK_NBR,
FISCAL_MO AS FISCAL_MO,
FISCAL_MO_NBR AS FISCAL_MO_NBR,
FISCAL_MO_NAME AS FISCAL_MO_NAME,
FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
FISCAL_QTR AS FISCAL_QTR,
FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
FISCAL_HALF AS FISCAL_HALF,
FISCAL_YR AS FISCAL_YR,
LYR_WEEK_DT AS LYR_WEEK_DT,
LWK_WEEK_DT AS LWK_WEEK_DT FROM SQ_Shortcut_to_Weeks_1""")
