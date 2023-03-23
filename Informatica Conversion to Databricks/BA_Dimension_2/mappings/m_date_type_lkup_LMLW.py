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

# DBTITLE 1, SQ_Shortcut_To_DAYS_1

df_1=spark.sql("""SELECT 1 AS DATE_TYPE_ID, 'DAY' AS DATE_TYPE_DESC, 49 AS DATE_TYPE_SORT_ID, 'DAY' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 2 AS DATE_TYPE_ID, 'WTD' AS DATE_TYPE_DESC, 50 AS DATE_TYPE_SORT_ID, 'WTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 3 AS DATE_TYPE_ID, 'LW' AS DATE_TYPE_DESC, 51 AS DATE_TYPE_SORT_ID, 'LW' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 4 AS DATE_TYPE_ID, 'PTD' AS DATE_TYPE_DESC, 52 AS DATE_TYPE_SORT_ID, 'PTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 5 AS DATE_TYPE_ID, 'LMth' AS DATE_TYPE_DESC, 53 AS DATE_TYPE_SORT_ID, 'LMth' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 6 AS DATE_TYPE_ID, 'QTD' AS DATE_TYPE_DESC, 54 AS DATE_TYPE_SORT_ID, 'QTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 7 AS DATE_TYPE_ID, 'LQtr' AS DATE_TYPE_DESC, 71 AS DATE_TYPE_SORT_ID, 'LQtr' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 8 AS DATE_TYPE_ID, 'YTD' AS DATE_TYPE_DESC, 56 AS DATE_TYPE_SORT_ID, 'YTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 9 AS DATE_TYPE_ID, '4Wk' AS DATE_TYPE_DESC, 57 AS DATE_TYPE_SORT_ID, '4Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 10 AS DATE_TYPE_ID, '8Wk' AS DATE_TYPE_DESC, 58 AS DATE_TYPE_SORT_ID, '8Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 11 AS DATE_TYPE_ID, '13Wk' AS DATE_TYPE_DESC, 59 AS DATE_TYPE_SORT_ID, '13Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 12 AS DATE_TYPE_ID, 'LYTD' AS DATE_TYPE_DESC, 60 AS DATE_TYPE_SORT_ID, 'LYTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 13 AS DATE_TYPE_ID, 'LYr' AS DATE_TYPE_DESC, 61 AS DATE_TYPE_SORT_ID, 'LYr' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 14 AS DATE_TYPE_ID, '16Wk' AS DATE_TYPE_DESC, 62 AS DATE_TYPE_SORT_ID, '16Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 15 AS DATE_TYPE_ID, 'LYQTD' AS DATE_TYPE_DESC, 63 AS DATE_TYPE_SORT_ID, 'LYQTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 16 AS DATE_TYPE_ID, '2WksAgo' AS DATE_TYPE_DESC, 64 AS DATE_TYPE_SORT_ID, '2WksAgo' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 17 AS DATE_TYPE_ID, '26Wk' AS DATE_TYPE_DESC, 65 AS DATE_TYPE_SORT_ID, '26Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 18 AS DATE_TYPE_ID, '52Wk' AS DATE_TYPE_DESC, 66 AS DATE_TYPE_SORT_ID, '52Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 19 AS DATE_TYPE_ID, 'LYPTD' AS DATE_TYPE_DESC, 67 AS DATE_TYPE_SORT_ID, 'LYPTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 20 AS DATE_TYPE_ID, 'LYLMth' AS DATE_TYPE_DESC, 68 AS DATE_TYPE_SORT_ID, 'LYLMth' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 21 AS DATE_TYPE_ID, '2Wk' AS DATE_TYPE_DESC, 69 AS DATE_TYPE_SORT_ID, '2Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 22 AS DATE_TYPE_ID, 'Q3Q4TD' AS DATE_TYPE_DESC, 70 AS DATE_TYPE_SORT_ID, 'Q3Q4TD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 23 AS DATE_TYPE_ID, 'STD' AS DATE_TYPE_DESC, 55 AS DATE_TYPE_SORT_ID, 'STD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG
UNION
SELECT 24 AS DATE_TYPE_ID, 'Day 1' AS DATE_TYPE_DESC, 9 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Mon' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 25 AS DATE_TYPE_ID, 'Day 2' AS DATE_TYPE_DESC, 10 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Tue' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 26 AS DATE_TYPE_ID, 'Day 3' AS DATE_TYPE_DESC, 11 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Wed' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 27 AS DATE_TYPE_ID, 'Day 4' AS DATE_TYPE_DESC, 12 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Thu' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 28 AS DATE_TYPE_ID, 'Day 5' AS DATE_TYPE_DESC, 13 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Fri' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 29 AS DATE_TYPE_ID, 'Day 6' AS DATE_TYPE_DESC, 14 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sat' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 30 AS DATE_TYPE_ID, 'Day 7' AS DATE_TYPE_DESC, 15 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sun' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 31 AS DATE_TYPE_ID, 'Day 8' AS DATE_TYPE_DESC, 17 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Mon' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 32 AS DATE_TYPE_ID, 'Day 9' AS DATE_TYPE_DESC, 18 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Tue' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 33 AS DATE_TYPE_ID, 'Day 10' AS DATE_TYPE_DESC, 19 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Wed' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 34 AS DATE_TYPE_ID, 'Day 11' AS DATE_TYPE_DESC, 20 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Thu' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 35 AS DATE_TYPE_ID, 'Day 12' AS DATE_TYPE_DESC, 21 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Fri' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 36 AS DATE_TYPE_ID, 'Day 13' AS DATE_TYPE_DESC, 22 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sat' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 37 AS DATE_TYPE_ID, 'Day 14' AS DATE_TYPE_DESC, 23 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sun' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 38 AS DATE_TYPE_ID, 'Day 15' AS DATE_TYPE_DESC, 25 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Mon' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 39 AS DATE_TYPE_ID, 'Day 16' AS DATE_TYPE_DESC, 26 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Tue' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 40 AS DATE_TYPE_ID, 'Day 17' AS DATE_TYPE_DESC, 27 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Wed' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 41 AS DATE_TYPE_ID, 'Day 18' AS DATE_TYPE_DESC, 28 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Thu' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 42 AS DATE_TYPE_ID, 'Day 19' AS DATE_TYPE_DESC, 29 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Fri' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 43 AS DATE_TYPE_ID, 'Day 20' AS DATE_TYPE_DESC, 30 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sat' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 44 AS DATE_TYPE_ID, 'Day 21' AS DATE_TYPE_DESC, 31 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sun' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 45 AS DATE_TYPE_ID, 'Day 22' AS DATE_TYPE_DESC, 33 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Mon' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 46 AS DATE_TYPE_ID, 'Day 23' AS DATE_TYPE_DESC, 34 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Tue' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 47 AS DATE_TYPE_ID, 'Day 24' AS DATE_TYPE_DESC, 35 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Wed' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 48 AS DATE_TYPE_ID, 'Day 25' AS DATE_TYPE_DESC, 36 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Thu' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 49 AS DATE_TYPE_ID, 'Day 26' AS DATE_TYPE_DESC, 37 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Fri' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 50 AS DATE_TYPE_ID, 'Day 27' AS DATE_TYPE_DESC, 38 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sat' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 51 AS DATE_TYPE_ID, 'Day 28' AS DATE_TYPE_DESC, 39 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sun' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG 
UNION
SELECT 52 AS DATE_TYPE_ID, 'Day 29' AS DATE_TYPE_DESC, 41 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 53 AS DATE_TYPE_ID, 'Day 30' AS DATE_TYPE_DESC, 42 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 54 AS DATE_TYPE_ID, 'Day 31' AS DATE_TYPE_DESC, 43 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 55 AS DATE_TYPE_ID, 'Day 32' AS DATE_TYPE_DESC, 44 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 56 AS DATE_TYPE_ID, 'Day 33' AS DATE_TYPE_DESC, 45 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 57 AS DATE_TYPE_ID, 'Day 34' AS DATE_TYPE_DESC, 46 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 58 AS DATE_TYPE_ID, 'Day 35' AS DATE_TYPE_DESC, 47 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG
UNION
SELECT 59 AS DATE_TYPE_ID, 'Wk 1' AS DATE_TYPE_DESC, 16 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 60 AS DATE_TYPE_ID, 'Wk 2' AS DATE_TYPE_DESC, 24 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 61 AS DATE_TYPE_ID, 'Wk 3' AS DATE_TYPE_DESC, 32 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 62 AS DATE_TYPE_ID, 'Wk 4' AS DATE_TYPE_DESC, 40 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 63 AS DATE_TYPE_ID, 'Wk 5' AS DATE_TYPE_DESC, 48 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG
UNION
SELECT 64 AS DATE_TYPE_ID, 'Day 1 LW' AS DATE_TYPE_DESC, 1 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 65 AS DATE_TYPE_ID, 'Day 2 LW' AS DATE_TYPE_DESC, 2 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 66 AS DATE_TYPE_ID, 'Day 3 LW' AS DATE_TYPE_DESC, 3 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 67 AS DATE_TYPE_ID, 'Day 4 LW' AS DATE_TYPE_DESC, 4 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 68 AS DATE_TYPE_ID, 'Day 5 LW' AS DATE_TYPE_DESC, 5 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 69 AS DATE_TYPE_ID, 'Day 6 LW' AS DATE_TYPE_DESC, 6 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 70 AS DATE_TYPE_ID, 'Day 7 LW' AS DATE_TYPE_DESC, 7 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 71 AS DATE_TYPE_ID, 'LMLW' AS DATE_TYPE_DESC, 8 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG
UNION
SELECT 72 AS DATE_TYPE_ID, 'LYLW' AS DATE_TYPE_DESC, 72 AS DATE_TYPE_SORT_ID, 'LYLW' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG
UNION
SELECT 73 AS DATE_TYPE_ID, 'LYSTD' AS DATE_TYPE_DESC, 73 AS DATE_TYPE_SORT_ID, 'LYSTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG
UNION
SELECT 80 DATE_TYPE_ID, 'Cal YTD' DATE_TYPE_DESC, 80 DATE_TYPE_SORT_ID, 'CYTD' DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 81 DATE_TYPE_ID, 'Cal QTD' DATE_TYPE_DESC, 81 DATE_TYPE_SORT_ID, 'CQTD' DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION
SELECT 82 DATE_TYPE_ID, 'Cal MTD' DATE_TYPE_DESC, 82 DATE_TYPE_SORT_ID, 'CMTD' DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_DAYS_1")

# COMMAND ----------

# DBTITLE 1, EXP_DATE_TYPE_LKUP_2

df_2=spark.sql("""
    SELECT
        DATE_TYPE_ID AS DATE_TYPE_ID,
        DATE_TYPE_DESC AS DATE_TYPE_DESC,
        DATE_TYPE_SORT_ID AS DATE_TYPE_SORT_ID,
        DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
        DATE_TYPE_DESC3 AS DATE_TYPE_DESC3,
        DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
        TW_LW_FLAG AS TW_LW_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_DAYS_1""")

df_2.createOrReplaceTempView("EXP_DATE_TYPE_LKUP_2")

# COMMAND ----------

# DBTITLE 1, DATE_TYPE_LKUP

spark.sql("""INSERT INTO DATE_TYPE_LKUP SELECT DATE_TYPE_ID AS DATE_TYPE_ID,
DATE_TYPE_DESC AS DATE_TYPE_DESC,
DATE_TYPE_SORT_ID AS DATE_TYPE_SORT_ID,
DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
DATE_TYPE_DESC3 AS DATE_TYPE_DESC3,
DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
TW_LW_FLAG AS TW_LW_FLAG FROM EXP_DATE_TYPE_LKUP_2""")
