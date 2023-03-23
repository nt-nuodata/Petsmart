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
        DISTINCT FiscalMo,
        FiscalHalf,
        FiscalMoName,
        FiscalMoNameAbbr,
        FiscalMoNbr,
        FiscalQtr,
        FiscalQtrNbr,
        FiscalYr 
    FROM
        Weeks""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Weeks_1")

# COMMAND ----------

# DBTITLE 1, EXPTRANS_2

df_2=spark.sql("""
    SELECT
        FiscalMo AS FiscalMo,
        FiscalHalf AS FiscalHalf,
        FiscalMoName AS FiscalMoName,
        FiscalMoNameAbbr AS FiscalMoNameAbbr,
        FiscalMoNbr AS FiscalMoNbr,
        FiscalQtr AS FiscalQtr,
        FiscalQtrNbr AS FiscalQtrNbr,
        FiscalYr AS FiscalYr,
        to_char(ADD_TO_DATE(current_timestamp,
        'YY',
        $$year),
        'YYYY') AS O_FiscalYr,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_Weeks_1""")

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------

# DBTITLE 1, FILTRANS_3

df_3=spark.sql("""
    SELECT
        FiscalMo AS FiscalMo,
        FiscalHalf AS FiscalHalf,
        FiscalMoName AS FiscalMoName,
        FiscalMoNameAbbr AS FiscalMoNameAbbr,
        FiscalMoNbr AS FiscalMoNbr,
        FiscalQtr AS FiscalQtr,
        FiscalQtrNbr AS FiscalQtrNbr,
        FiscalYr AS FiscalYr,
        O_FiscalYr AS O_FiscalYr,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_2 
    WHERE
        FiscalYr = O_FiscalYr""")

df_3.createOrReplaceTempView("FILTRANS_3")

# COMMAND ----------

# DBTITLE 1, MONTHS

spark.sql("""INSERT INTO MONTHS SELECT FISCAL_MO AS FISCAL_MO,
FISCAL_HALF AS FISCAL_HALF,
FISCAL_MO_NAME AS FISCAL_MO_NAME,
FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
FISCAL_MO_NBR AS FISCAL_MO_NBR,
FISCAL_QTR AS FISCAL_QTR,
FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
FISCAL_YR AS FISCAL_YR FROM FILTRANS_3""")
