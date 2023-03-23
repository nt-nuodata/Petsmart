# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SEQ_DATE_TYPE_ID


spark.sql("""CREATE TABLE SEQ_DATE_TYPE_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int);""")

spark.sql("""INSERT INTO SEQ_DATE_TYPE_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int) VALUES(60, 59, 1)""")

# COMMAND ----------
# DBTITLE 1, WEEKS_1


df_1=spark.sql("""
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

df_1.createOrReplaceTempView("WEEKS_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_WEEKS_2


df_2=spark.sql("""
    SELECT
        FISCAL_WK_NBR AS DATE_TYPE_DESC2,
        CASE 
            WHEN WEEK_DT = (SELECT
                WEEK_DT 
            FROM
                DAYS 
            WHERE
                DAY_DT = CURRENT_DATE - 1) THEN '1' 
            WHEN WEEK_DT = (SELECT
                LWK_WEEK_DT 
            FROM
                DAYS 
            WHERE
                DAY_DT = CURRENT_DATE - 1) THEN '1' 
            ELSE '0' 
        END TW_LW_FLAG 
    FROM
        WEEKS 
    WHERE
        FISCAL_MO = (
            SELECT
                MAX(FISCAL_MO) 
            FROM
                DAYS 
            WHERE
                DAY_DT < CURRENT_DATE
        ) 
    ORDER BY
        FISCAL_WK_NBR""")

df_2.createOrReplaceTempView("SQ_Shortcut_To_WEEKS_2")

# COMMAND ----------
# DBTITLE 1, EXP_WEEKS_3


df_3=spark.sql("""
    SELECT
        (ROW_NUMBER() OVER (
    ORDER BY
        (SELECT
            NULL)) - 1) * (SELECT
            Increment_By 
        FROM
            SEQ_DATE_TYPE_ID) + (SELECT
            NEXTVAL 
        FROM
            SEQ_DATE_TYPE_ID) AS DATE_TYPE_ID,
        SEQ_DATE_TYPE_ID.NEXTVAL AS DATE_TYPE_ID,
        'FW' || ' ' || DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
        'Active' AS DATE_TYPE_5WK_STATUS,
        (CAST(TW_LW_FLAG AS DECIMAL (38,
        0))) AS TW_LW_FLAG,
        SEQ_DATE_TYPE_ID.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SEQ_DATE_TYPE_ID 
    INNER JOIN
        SQ_Shortcut_To_WEEKS_2 
            ON SEQ_DATE_TYPE_ID.Monotonically_Increasing_Id = SQ_Shortcut_To_WEEKS_2.Monotonically_Increasing_Id""")

df_3.createOrReplaceTempView("EXP_WEEKS_3")

spark.sql("""UPDATE SEQ_DATE_TYPE_ID SET CURRVAL = (SELECT MAX(DATE_TYPE_ID) FROM EXP_WEEKS_3) , NEXTVAL = (SELECT MAX(DATE_TYPE_ID) FROM EXP_WEEKS_3) + (SELECT Increment_By FROM EXP_WEEKS_3)""")

# COMMAND ----------
# DBTITLE 1, DAYS_4


df_4=spark.sql("""
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

df_4.createOrReplaceTempView("DAYS_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_DAYS_5


df_5=spark.sql("""SELECT 
(23 + FISCAL_DAY_OF_MO_NBR) DAY_TYPE_ID, 
TO_CHAR(DAY_DT,'MM/DD') AS DATE_TYPE_DESC2, 
DAY_OF_WK_NAME_ABBR AS DATE_TYPE_DESC3,
CASE WHEN WEEK_DT = (SELECT WEEK_DT FROM DAYS WHERE DAY_DT = CURRENT_DATE - 1) THEN 1
	 WHEN WEEK_DT = (SELECT LWK_WEEK_DT FROM DAYS WHERE DAY_DT = CURRENT_DATE - 1) THEN 1
ELSE 0
END TW_LW_FLAG
FROM DAYS 
WHERE 
FISCAL_MO = (SELECT MAX(FISCAL_MO) FROM DAYS WHERE DAY_DT < CURRENT_DATE)

UNION

SELECT 
CASE WHEN W.TYPE_ID=1 THEN 64
WHEN W.TYPE_ID=2 THEN 65
WHEN W.TYPE_ID=3 THEN 66
WHEN W.TYPE_ID=4 THEN 67
WHEN W.TYPE_ID=5 THEN 68
WHEN W.TYPE_ID=6 THEN 69
WHEN W.TYPE_ID=7 THEN 70
END DATE_TYPE_ID,
TO_CHAR(D.DAY_DT,'MM/DD') AS DATE_TYPE_DESC2, 
D.DAY_OF_WK_NAME_ABBR AS DATE_TYPE_DESC3,
1 AS TW_LW_FLAG
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
ON D.DAY_DT = W.DAY_DT""")

df_5.createOrReplaceTempView("SQ_Shortcut_To_DAYS_5")

# COMMAND ----------
# DBTITLE 1, EXP_DAYS_6


df_6=spark.sql("""
    SELECT
        DATE_TYPE_ID AS DATE_TYPE_ID,
        DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
        DATE_TYPE_DESC3 AS DATE_TYPE_DESC3,
        'Active' AS DATE_TYPE_5WK_STATUS,
        TW_LW_FLAG AS TW_LW_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_DAYS_5""")

df_6.createOrReplaceTempView("EXP_DAYS_6")

# COMMAND ----------
# DBTITLE 1, UPD_DATE_TYPE_ID_DAYS_7


df_7=spark.sql("""
    SELECT
        DATE_TYPE_ID AS DATE_TYPE_ID,
        DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
        DATE_TYPE_DESC3 AS DATE_TYPE_DESC3,
        DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
        TW_LW_FLAG AS TW_LW_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DAYS_6""")

df_7.createOrReplaceTempView("UPD_DATE_TYPE_ID_DAYS_7")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_DAYS_LMLW_8


df_8=spark.sql("""
    SELECT
        DISTINCT 71 DATE_TYPE_ID,
        'FW' || ' ' || D.FISCAL_WK_NBR AS DATE_TYPE_DESC2,
        'Active' AS DATE_TYPE_5WK_STATUS,
        1 AS TW_LW_FLAG 
    FROM
        DAYS D 
    JOIN
        (
            SELECT
                MAX(WEEK_DT) AS WEEK_DT 
            FROM
                WEEKS 
            WHERE
                FISCAL_MO = (
                    SELECT
                        MAX(FISCAL_MO) - 1 
                    FROM
                        DAYS 
                    WHERE
                        DAY_DT < CURRENT_DATE
                )
            ) W 
                ON D.WEEK_DT = W.WEEK_DT""")

df_8.createOrReplaceTempView("SQ_Shortcut_To_DAYS_LMLW_8")

# COMMAND ----------
# DBTITLE 1, EXP_LW_9


df_9=spark.sql("""
    SELECT
        DATE_TYPE_ID AS DATE_TYPE_ID,
        DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
        DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
        TW_LW_FLAG AS TW_LW_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_DAYS_LMLW_8""")

df_9.createOrReplaceTempView("EXP_LW_9")

# COMMAND ----------
# DBTITLE 1, UNI_WEEKS_10


df_10=spark.sql("""SELECT DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
DATE_TYPE_ID AS DATE_TYPE_ID,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
TW_LW_FLAG AS TW_LW_FLAG FROM EXP_LW_9 UNION ALL SELECT DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
DATE_TYPE_ID AS DATE_TYPE_ID,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
TW_LW_FLAG AS TW_LW_FLAG FROM EXP_WEEKS_3""")

df_10.createOrReplaceTempView("UNI_WEEKS_10")

# COMMAND ----------
# DBTITLE 1, UPD_DATE_TYPE_ID_WEEKS_11


df_11=spark.sql("""
    SELECT
        DATE_TYPE_ID AS DATE_TYPE_ID,
        DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
        DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
        TW_LW_FLAG AS TW_LW_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UNI_WEEKS_10""")

df_11.createOrReplaceTempView("UPD_DATE_TYPE_ID_WEEKS_11")

# COMMAND ----------
# DBTITLE 1, DATE_TYPE_LKUP


spark.sql("""INSERT INTO DATE_TYPE_LKUP SELECT DATE_TYPE_ID AS DATE_TYPE_ID,
DATE_TYPE_DESC AS DATE_TYPE_DESC,
DATE_TYPE_SORT_ID AS DATE_TYPE_SORT_ID,
DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
DATE_TYPE_DESC3 AS DATE_TYPE_DESC3,
DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
TW_LW_FLAG AS TW_LW_FLAG FROM UPD_DATE_TYPE_ID_DAYS_7""")

# COMMAND ----------
# DBTITLE 1, DATE_TYPE_LKUP


spark.sql("""INSERT INTO DATE_TYPE_LKUP SELECT DATE_TYPE_ID AS DATE_TYPE_ID,
DATE_TYPE_DESC AS DATE_TYPE_DESC,
DATE_TYPE_SORT_ID AS DATE_TYPE_SORT_ID,
DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
DATE_TYPE_DESC3 AS DATE_TYPE_DESC3,
DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
TW_LW_FLAG AS TW_LW_FLAG FROM UPD_DATE_TYPE_ID_WEEKS_11""")