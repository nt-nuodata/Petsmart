# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SEQ_MA_EVENT_ID


spark.sql("""CREATE TABLE SEQ_MA_EVENT_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int);""")

spark.sql("""INSERT INTO SEQ_MA_EVENT_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int) VALUES(2, 1, 1)""")

# COMMAND ----------
# DBTITLE 1, SERVICES_MARGIN_RATE_1


df_1=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        MARGIN_RATE AS MARGIN_RATE,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SERVICES_MARGIN_RATE""")

df_1.createOrReplaceTempView("SERVICES_MARGIN_RATE_1")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_2


df_2=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        OFFER_ID AS OFFER_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        COMPANY_ID AS COMPANY_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        EM_COMMENT AS EM_COMMENT,
        EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
        EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        EM_VENDOR_ID AS EM_VENDOR_ID,
        EM_VENDOR_NAME AS EM_VENDOR_NAME,
        EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT""")

df_2.createOrReplaceTempView("MA_EVENT_2")

# COMMAND ----------
# DBTITLE 1, DAYS_3


df_3=spark.sql("""
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

df_3.createOrReplaceTempView("DAYS_3")

# COMMAND ----------
# DBTITLE 1, SAP_DEPT_4


df_4=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        MERCH_DIVISIONAL_ID AS MERCH_DIVISIONAL_ID,
        BUYER_ID AS BUYER_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_DEPT""")

df_4.createOrReplaceTempView("SAP_DEPT_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5


df_5=spark.sql("""
    SELECT
        NULL AS OFFER_ID,
        S.SAP_DEPT_ID,
        NULL AS PRODUCT_ID,
        NULL AS COUNTRY_CD,
        W.START_DT,
        W.END_DT,
        30 AS MA_EVENT_TYPE_ID,
        7 AS MA_EVENT_SOURCE_ID,
        S.LOCATION_ID,
        'Services Margin - ' || TRIM(SAP_DEPT_DESC) || ' - Week=' || TO_CHAR(S.WEEK_DT,
        'MM/DD/YYYY') || ' - Location ID=' || S.LOCATION_ID AS MA_EVENT_DESC,
        NULL AS EM_VENDOR_FUNDING_ID,
        NULL AS EM_COMMENT,
        NULL AS EM_BILL_ALT_VENDOR_FLAG,
        NULL AS EM_ALT_VENDOR_ID,
        NULL AS EM_ALT_VENDOR_NAME,
        NULL AS EM_ALT_VENDOR_COUNTRY_CD,
        NULL AS EM_VENDOR_ID,
        NULL AS EM_VENDOR_NAME,
        NULL AS EM_VENDOR_COUNTRY_CD,
        NULL AS VENDOR_NAME_TXT,
        1 AS MA_PCT_IND,
        S.MARGIN_RATE * 100 * -1 AS MA_AMT,
        NULL AS MA_MAX_AMT,
        'I' AS LOAD_FLAG 
    FROM
        SERVICES_MARGIN_RATE S 
    JOIN
        SAP_DEPT D 
            ON S.SAP_DEPT_ID = D.SAP_DEPT_ID 
    JOIN
        (
            SELECT
                WEEK_DT,
                MIN(DAY_DT) AS START_DT,
                MAX(DAY_DT) AS END_DT 
            FROM
                DAYS 
            WHERE
                CAL_YR >= 2014 
            GROUP BY
                WEEK_DT
        ) W 
            ON S.WEEK_DT = W.WEEK_DT 
    LEFT JOIN
        MA_EVENT M 
            ON S.LOCATION_ID = M.LOCATION_ID 
            AND S.SAP_DEPT_ID = M.SAP_DEPT_ID 
            AND W.START_DT = M.START_DT 
            AND W.END_DT = M.END_DT 
            AND M.MA_EVENT_TYPE_ID = 30 
            AND M.MA_EVENT_SOURCE_ID = 7 
    WHERE
        S.UPDATE_TSTMP > CURRENT_DATE - 2 
        AND M.MA_EVENT_ID IS NULL""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5")

# COMMAND ----------
# DBTITLE 1, EXP_INS_UPD_6


df_6=spark.sql("""
    SELECT
        (ROW_NUMBER() OVER (
    ORDER BY
        (SELECT
            NULL)) - 1) * (SELECT
            Increment_By 
        FROM
            SEQ_MA_EVENT_ID) + (SELECT
            NEXTVAL 
        FROM
            SEQ_MA_EVENT_ID) AS MA_EVENT_ID,
        SEQ_MA_EVENT_ID.NEXTVAL AS MA_EVENT_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.OFFER_ID AS OFFER_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.SAP_DEPT_ID AS SAP_DEPT_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.PRODUCT_ID AS PRODUCT_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.COUNTRY_CD AS COUNTRY_CD,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.START_DT AS START_DT,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.END_DT AS END_DT,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.LOCATION_ID AS LOCATION_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.MA_EVENT_DESC AS MA_EVENT_DESC,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_COMMENT AS EM_COMMENT,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_VENDOR_ID AS EM_VENDOR_ID,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_VENDOR_NAME AS EM_VENDOR_NAME,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.MA_PCT_IND AS MA_PCT_IND,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.MA_AMT AS MA_AMT,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.MA_MAX_AMT AS MA_MAX_AMT,
        current_timestamp AS UPDATE_DT,
        current_timestamp AS LOAD_DT,
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.LOAD_FLAG AS LOAD_FLAG,
        SEQ_MA_EVENT_ID.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SEQ_MA_EVENT_ID 
    INNER JOIN
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5 
            ON SEQ_MA_EVENT_ID.Monotonically_Increasing_Id = SQ_Shortcut_to_SERVICES_MARGIN_RATE_Ins_5.Monotonically_Increasing_Id""")

df_6.createOrReplaceTempView("EXP_INS_UPD_6")

spark.sql("""UPDATE SEQ_MA_EVENT_ID SET CURRVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_INS_UPD_6) , NEXTVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_INS_UPD_6) + (SELECT Increment_By FROM EXP_INS_UPD_6)""")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SERVICES_MARGIN_RATE_Upd_7


df_7=spark.sql("""
    SELECT
        M.MA_EVENT_ID,
        NULL AS OFFER_ID,
        M.SAP_DEPT_ID,
        NULL AS PRODUCT_ID,
        NULL AS COUNTRY_CD,
        M.START_DT,
        M.END_DT,
        M.MA_EVENT_TYPE_ID,
        M.MA_EVENT_SOURCE_ID,
        M.LOCATION_ID,
        M.MA_EVENT_DESC,
        NULL AS EM_VENDOR_FUNDING_ID,
        NULL AS EM_COMMENT,
        NULL AS EM_BILL_ALT_VENDOR_FLAG,
        NULL AS EM_ALT_VENDOR_ID,
        NULL AS EM_ALT_VENDOR_NAME,
        NULL AS EM_ALT_VENDOR_COUNTRY_CD,
        NULL AS EM_VENDOR_ID,
        NULL AS EM_VENDOR_NAME,
        NULL AS EM_VENDOR_COUNTRY_CD,
        NULL AS VENDOR_NAME_TXT,
        M.MA_PCT_IND,
        S.MARGIN_RATE * 100 * -1 AS MA_AMT,
        NULL AS MA_MAX_AMT,
        M.LOAD_DT,
        'R' AS LOAD_FLAG 
    FROM
        SERVICES_MARGIN_RATE S 
    JOIN
        SAP_DEPT D 
            ON S.SAP_DEPT_ID = D.SAP_DEPT_ID 
    JOIN
        (
            SELECT
                WEEK_DT,
                MIN(DAY_DT) AS START_DT,
                MAX(DAY_DT) AS END_DT 
            FROM
                DAYS 
            WHERE
                CAL_YR >= 2014 
            GROUP BY
                WEEK_DT
        ) W 
            ON S.WEEK_DT = W.WEEK_DT 
    JOIN
        MA_EVENT M 
            ON S.LOCATION_ID = M.LOCATION_ID 
            AND S.SAP_DEPT_ID = M.SAP_DEPT_ID 
            AND W.START_DT = M.START_DT 
            AND W.END_DT = M.END_DT 
            AND M.MA_EVENT_TYPE_ID = 30 
            AND M.MA_EVENT_SOURCE_ID = 7 
    WHERE
        S.UPDATE_TSTMP > CURRENT_DATE - 2 
        AND M.MA_AMT <> S.MARGIN_RATE * 100 * -1""")

df_7.createOrReplaceTempView("SQ_Shortcut_to_SERVICES_MARGIN_RATE_Upd_7")

# COMMAND ----------
# DBTITLE 1, EXP_UPD_8


df_8=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        OFFER_ID AS OFFER_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        EM_COMMENT AS EM_COMMENT,
        EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
        EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        EM_VENDOR_ID AS EM_VENDOR_ID,
        EM_VENDOR_NAME AS EM_VENDOR_NAME,
        EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        current_timestamp AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        LOAD_FLAG AS LOAD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SERVICES_MARGIN_RATE_Upd_7""")

df_8.createOrReplaceTempView("EXP_UPD_8")

# COMMAND ----------
# DBTITLE 1, UNI_SERVICES_MARGIN_RATE_9


df_9=spark.sql("""SELECT COUNTRY_CD AS COUNTRY_CD,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
EM_COMMENT AS EM_COMMENT,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
END_DT AS END_DT,
LOAD_DT AS LOAD_DT,
LOAD_FLAG AS LOAD_FLAG,
LOCATION_ID AS LOCATION_ID,
MA_AMT AS MA_AMT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_MAX_AMT AS MA_MAX_AMT,
MA_PCT_IND AS MA_PCT_IND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
OFFER_ID AS OFFER_ID,
PRODUCT_ID AS PRODUCT_ID,
SAP_DEPT_ID AS SAP_DEPT_ID,
START_DT AS START_DT,
UPDATE_DT AS UPDATE_DT,
VENDOR_NAME_TXT AS VENDOR_NAME_TXT FROM EXP_UPD_8 UNION ALL SELECT COUNTRY_CD AS COUNTRY_CD,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
EM_COMMENT AS EM_COMMENT,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
END_DT AS END_DT,
LOAD_DT AS LOAD_DT,
LOAD_FLAG AS LOAD_FLAG,
LOCATION_ID AS LOCATION_ID,
MA_AMT AS MA_AMT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_MAX_AMT AS MA_MAX_AMT,
MA_PCT_IND AS MA_PCT_IND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
OFFER_ID AS OFFER_ID,
PRODUCT_ID AS PRODUCT_ID,
SAP_DEPT_ID AS SAP_DEPT_ID,
START_DT AS START_DT,
UPDATE_DT AS UPDATE_DT,
VENDOR_NAME_TXT AS VENDOR_NAME_TXT FROM EXP_INS_UPD_6""")

df_9.createOrReplaceTempView("UNI_SERVICES_MARGIN_RATE_9")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_10


df_10=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        OFFER_ID AS OFFER_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        EM_COMMENT AS EM_COMMENT,
        EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
        EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        EM_VENDOR_ID AS EM_VENDOR_ID,
        EM_VENDOR_NAME AS EM_VENDOR_NAME,
        EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        LOAD_FLAG AS LOAD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UNI_SERVICES_MARGIN_RATE_9""")

df_10.createOrReplaceTempView("UPD_STRATEGY_10")

# COMMAND ----------
# DBTITLE 1, EXP_INS_11


df_11=spark.sql("""
    SELECT
        date_trunc('DAY',
        current_timestamp) AS LOAD_DT,
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        EM_COMMENT AS EM_COMMENT,
        EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
        EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        EM_VENDOR_ID AS EM_VENDOR_ID,
        EM_VENDOR_NAME AS EM_VENDOR_NAME,
        EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        IFF(LOAD_FLAG = 'R',
        'U',
        LOAD_FLAG) AS LOAD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UNI_SERVICES_MARGIN_RATE_9""")

df_11.createOrReplaceTempView("EXP_INS_11")

# COMMAND ----------
# DBTITLE 1, MA_EVENT_RESTATE_HIST


spark.sql("""INSERT INTO MA_EVENT_RESTATE_HIST SELECT LOAD_DT AS LOAD_DT,
MA_EVENT_ID AS MA_EVENT_ID,
OFFER_ID AS OFFER_ID,
SAP_DEPT_ID AS SAP_DEPT_ID,
PRODUCT_ID AS PRODUCT_ID,
COUNTRY_CD AS COUNTRY_CD,
START_DT AS START_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
LOCATION_ID AS LOCATION_ID,
MOVEMENT_ID AS MOVEMENT_ID,
VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
GL_ACCT_NBR AS GL_ACCT_NBR,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
COMPANY_ID AS COMPANY_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_COMMENT AS EM_COMMENT,
EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
MA_PCT_IND AS MA_PCT_IND,
ORIG_MA_AMT AS MA_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG FROM EXP_INS_11""")

# COMMAND ----------
# DBTITLE 1, MA_EVENT


spark.sql("""INSERT INTO MA_EVENT SELECT MA_EVENT_ID AS MA_EVENT_ID,
OFFER_ID AS OFFER_ID,
SAP_DEPT_ID AS SAP_DEPT_ID,
PRODUCT_ID AS PRODUCT_ID,
COUNTRY_CD AS COUNTRY_CD,
START_DT AS START_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
LOCATION_ID AS LOCATION_ID,
MOVEMENT_ID AS MOVEMENT_ID,
VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
GL_ACCT_NBR AS GL_ACCT_NBR,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
COMPANY_ID AS COMPANY_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_COMMENT AS EM_COMMENT,
EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
MA_PCT_IND AS MA_PCT_IND,
NEW_MA_AMT AS MA_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPD_STRATEGY_10""")