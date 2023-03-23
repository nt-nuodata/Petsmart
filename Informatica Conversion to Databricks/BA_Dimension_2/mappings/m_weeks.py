# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, DAYS_0

df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
        HOLIDAY_FLAG AS HOLIDAY_FLAG,
        DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
        DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
        DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
        CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
        CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
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
        FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
        FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
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
        WEEK_DT AS WEEK_DT,
        EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
        EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
        ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
        ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
        CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
        CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
        CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
        CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
        MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
        MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
        MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
        MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
        PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
        PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DAYS""")

df_0.createOrReplaceTempView("DAYS_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_Days_1

df_1=spark.sql("""
    SELECT
        DISTINCT WeekDt,
        CalWk,
        CalWkNbr,
        CAST(DATEPART(YY,
        WeekDt) AS VARCHAR (4)) + REPLICATE('0',
        2 - LEN(DATEPART(MM,
        WeekDt))) + CAST(DATEPART(MM,
        WeekDt) AS VARCHAR (2)) AS CAL_MO,
        DATEPART(MM,
        WeekDt) AS CAL_MO_NBR,
        DATENAME(MM,
        WeekDt) AS CAL_MO_NAME,
        SUBSTRING(DATENAME(MM,
        WeekDt),
        1,
        3) AS CAL_MO_NAME_ABBR,
        CAST(DATEPART(YY,
        WeekDt) AS VARCHAR (4)) + REPLICATE('0',
        2 - LEN(DATEPART(QUARTER,
        WeekDt))) + CAST(DATEPART(QUARTER,
        WeekDt) AS VARCHAR (2)) AS CAL_QTR,
        DATEPART(QUARTER,
        WeekDt) AS CAL_QTR_NBR,
        CAST(DATEPART(YY,
        WeekDt) AS VARCHAR (4)) + (CASE 
            WHEN DATEPART(QUARTER,
            WeekDt) <= 2 THEN '01' 
            ELSE '02' 
        END) AS CAL_HALF,
        DATEPART(YY,
        WeekDt) AS CAL_YR,
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
        DAYS""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_Days_1")

# COMMAND ----------

# DBTITLE 1, EXPTRANS_2

df_2=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        CalWk AS CalWk,
        CalWkNbr AS CalWkNbr,
        CalMo AS CalMo,
        CalMoNbr AS CalMoNbr,
        CalMoName AS CalMoName,
        CalMoNameAbbr AS CalMoNameAbbr,
        CalQtr AS CalQtr,
        CalQtrNbr AS CalQtrNbr,
        CalHalf AS CalHalf,
        CalYr AS CalYr,
        FiscalWk AS FiscalWk,
        FiscalWkNbr AS FiscalWkNbr,
        FiscalMo AS FiscalMo,
        FiscalMoNbr AS FiscalMoNbr,
        FiscalMoName AS FiscalMoName,
        FiscalMoNameAbbr AS FiscalMoNameAbbr,
        FiscalQtr AS FiscalQtr,
        FiscalQtrNbr AS FiscalQtrNbr,
        FiscalHalf AS FiscalHalf,
        FiscalYr AS FiscalYr,
        to_char(ADD_TO_DATE(current_timestamp,
        'YY',
        $$year),
        'YYYY') AS O_FiscalYr,
        LyrWeekDt AS LyrWeekDt,
        LwkWeekDt AS LwkWeekDt,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_Days_1""")

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------

# DBTITLE 1, FILTRANS_3

df_3=spark.sql("""
    SELECT
        WeekDt AS WeekDt,
        CalWk AS CalWk,
        CalWkNbr AS CalWkNbr,
        CalMo AS CalMo,
        CalMoNbr AS CalMoNbr,
        CalMoName AS CalMoName,
        CalMoNameAbbr AS CalMoNameAbbr,
        CalQtr AS CalQtr,
        CalQtrNbr AS CalQtrNbr,
        CalHalf AS CalHalf,
        CalYr AS CalYr,
        FiscalWk AS FiscalWk,
        FiscalWkNbr AS FiscalWkNbr,
        FiscalMo AS FiscalMo,
        FiscalMoNbr AS FiscalMoNbr,
        FiscalMoName AS FiscalMoName,
        FiscalMoNameAbbr AS FiscalMoNameAbbr,
        FiscalQtr AS FiscalQtr,
        FiscalQtrNbr AS FiscalQtrNbr,
        FiscalHalf AS FiscalHalf,
        FiscalYr AS FiscalYr,
        O_FiscalYr AS O_FiscalYr,
        LyrWeekDt AS LyrWeekDt,
        LwkWeekDt AS LwkWeekDt,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_2 
    WHERE
        FiscalYr = O_FiscalYr""")

df_3.createOrReplaceTempView("FILTRANS_3")

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
LWK_WEEK_DT AS LWK_WEEK_DT FROM FILTRANS_3""")
