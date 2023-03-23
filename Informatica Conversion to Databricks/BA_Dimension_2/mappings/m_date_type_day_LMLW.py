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


df_1=spark.sql("""--DAY
SELECT 1 date_type_id,
       CURRENT_DATE - 1 day_dt
UNION
--LW
SELECT  3 date_type_id,
        d2.day_dt DAY_DT
FROM    days d1
        INNER JOIN (
                   SELECT       Fiscal_Yr, max(fiscal_wk) as LastYearMaxFiscal_wk
                   FROM         days
                   GROUP BY     Fiscal_Yr
                   order by fiscal_yr
                   ) d3 ON (d1.fiscal_yr - 1) = d3.fiscal_yr
        INNER JOIN days d2 ON d1.day_dt = CURRENT_DATE
           AND CASE d1.FISCAL_WK_NBR WHEN 1 THEN d3.LastYearMaxFiscal_wk  ELSE (d1.fiscal_wk - 1) END = d2.fiscal_wk
UNION
--4Wk
SELECT 9 DATE_TYPE_ID,
       D.DAY_DT
  FROM DAYS D
  CROSS JOIN (SELECT DAY_OF_WK_NBR 
                FROM DAYS 
                        WHERE DAY_DT = CURRENT_DATE
                     ) P
  WHERE D.DAY_DT BETWEEN CURRENT_DATE -27 - P.DAY_OF_WK_NBR AND CURRENT_DATE - P.DAY_OF_WK_NBR
UNION
--WTD
SELECT 2 date_type_id,
       d2.day_dt
  FROM days d1
  JOIN days d2
    ON d1.day_dt = CURRENT_DATE - 1
   AND d1.fiscal_wk = d2.fiscal_wk
   AND d1.day_dt >= d2.day_dt
UNION
--PTD
SELECT 4 date_type_id,
       d2.day_dt
  FROM days d1
  JOIN days d2
    ON d1.day_dt = CURRENT_DATE - 1
   AND d1.fiscal_mo = d2.fiscal_mo
   AND d1.day_dt >= d2.day_dt
UNION
--QTD
SELECT 6 date_type_id,
       d2.day_dt
  FROM days d1
  JOIN days d2
    ON d1.day_dt = CURRENT_DATE - 1
   AND d1.fiscal_qtr = d2.fiscal_qtr
   AND d1.day_dt >= d2.day_dt
UNION
--YTD
SELECT 8 date_type_id,
       d2.day_dt
  FROM days d1
  JOIN days d2
    ON d1.day_dt = CURRENT_DATE - 1
   AND d1.fiscal_yr = d2.fiscal_yr
   AND d1.day_dt >= d2.day_dt
UNION
--Q3Q4TD
SELECT 22 date_type_id,
       d2.day_dt
  FROM days d1
  JOIN days d2
    ON d1.day_dt = CURRENT_DATE - 1
   AND d1.fiscal_yr = d2.fiscal_yr
  where substr(d2.FISCAL_QTR,5,2) in (03,04)
  and d2.day_dt < current_Date
   AND d1.day_dt >= d2.day_dt
UNION
--STD
SELECT  23 DATE_TYPE_ID,
		d1.DAY_DT
		FROM DAYS d1
		WHERE DAY_DT < CURRENT_DATE
		AND FISCAL_HALF=(SELECT FISCAL_HALF FROM DAYS d2
                 		WHERE d2.DAY_DT=(SELECT MAX(d3.DAY_DT) FROM DAYS d3 WHERE d3.DAY_DT < CURRENT_DATE))
UNION
--DAYS OF FISCAL MONTH
SELECT (23 + FISCAL_DAY_OF_MO_NBR) DAY_TYPE_ID, DAY_DT
FROM DAYS 
WHERE 
FISCAL_MO = (SELECT MAX(FISCAL_MO) FROM DAYS WHERE DAY_DT < CURRENT_DATE)
UNION
--WEEKS OF FISCAL MONTH
SELECT
CASE WHEN W.TYPE_ID=1 THEN 59
WHEN W.TYPE_ID=2 THEN 60
WHEN W.TYPE_ID=3 THEN 61
WHEN W.TYPE_ID=4 THEN 62
WHEN W.TYPE_ID=5 THEN 63
END DATE_TYPE_ID,
D.DAY_DT
FROM DAYS D
JOIN
( 
SELECT ROW_NUMBER() OVER (ORDER BY WEEK_DT) AS TYPE_ID, WEEK_DT 
FROM WEEKS WHERE FISCAL_MO = (SELECT MAX(FISCAL_MO) FROM DAYS WHERE DAY_DT < CURRENT_DATE)
) W
ON D.WEEK_DT=W.WEEK_DT
UNION
--DAYS OF LAST WEEK PREVIOUS FISCAL MONTH
SELECT 
CASE WHEN W.TYPE_ID=1 THEN 64
WHEN W.TYPE_ID=2 THEN 65
WHEN W.TYPE_ID=3 THEN 66
WHEN W.TYPE_ID=4 THEN 67
WHEN W.TYPE_ID=5 THEN 68
WHEN W.TYPE_ID=6 THEN 69
WHEN W.TYPE_ID=7 THEN 70
END DATE_TYPE_ID,
D.DAY_DT
FROM 
DAYS D
JOIN
( 
SELECT ROW_NUMBER() OVER (ORDER BY DAY_DT) AS TYPE_ID, DAY_DT 
FROM DAYS 
WHERE FISCAL_WK = (
			SELECT MAX(FISCAL_WK) FROM DAYS 
			WHERE 
			FISCAL_MO = (SELECT MAX(FISCAL_MO) -1 FROM DAYS WHERE DAY_DT  < CURRENT_DATE)
			)
) W
ON D.DAY_DT = W.DAY_DT
UNION
--LAST WEEK PREVIOUS FISCAL MONTH
SELECT
71 DATE_TYPE_ID,
D.DAY_DT
FROM DAYS D
JOIN
( 
SELECT MAX(WEEK_DT) AS WEEK_DT
FROM WEEKS 
WHERE FISCAL_MO = (SELECT MAX(FISCAL_MO)-1 FROM DAYS WHERE DAY_DT < CURRENT_DATE)
) W
ON D.WEEK_DT=W.WEEK_DT
UNION
-- CALENDAR YEAR,QUARTER,MONTH
SELECT A.DATE_TYPE_ID,
       D.DAY_DT
  FROM (
       SELECT 80 DATE_TYPE_ID, CAL_YR, NULL CAL_QTR, NULL CAL_MO FROM DAYS WHERE DAY_DT = CURRENT_DATE - 1 UNION ALL
       SELECT 81 DATE_TYPE_ID, NULL CAL_YR, CAL_QTR, NULL CAL_MO FROM DAYS WHERE DAY_DT = CURRENT_DATE - 1 UNION ALL
       SELECT 82 DATE_TYPE_ID, NULL CAL_YR, NULL CAL_QTR, CAL_MO FROM DAYS WHERE DAY_DT = CURRENT_DATE - 1
       ) A
  JOIN DAYS D
    ON NVL(A.CAL_YR, D.CAL_YR)  = D.CAL_YR
   AND NVL(A.CAL_QTR,D.CAL_QTR) = D.CAL_QTR
   AND NVL(A.CAL_MO, D.CAL_MO)  = D.CAL_MO
   AND D.DAY_DT <= CURRENT_DATE - 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_DAYS_1")

# COMMAND ----------
# DBTITLE 1, DATE_TYPE_DAY


spark.sql("""INSERT INTO DATE_TYPE_DAY SELECT DATE_TYPE_ID AS DATE_TYPE_ID,
DAY_DT AS DAY_DT FROM SQ_Shortcut_To_DAYS_1""")