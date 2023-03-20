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
# DBTITLE 1, USR_MA_DEF_ALLOW_CTRL_1


df_1=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        DEFECTIVE_ALLOWANCE_PCT AS DEFECTIVE_ALLOWANCE_PCT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        USR_MA_DEF_ALLOW_CTRL""")

df_1.createOrReplaceTempView("USR_MA_DEF_ALLOW_CTRL_1")

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
# DBTITLE 1, SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4


df_4=spark.sql("""
    SELECT
        D.START_DT,
        D.END_DT,
        80 MA_EVENT_TYPE_ID,
        13 MA_EVENT_SOURCE_ID,
        UMDAC.FISCAL_MO,
        UMDAC.SOURCE_VENDOR_ID,
        'Defective Allowance: FiscalMo=' || UMDAC.FISCAL_MO::VARCHAR (10) || ': SourceVendor=' || UMDAC.SOURCE_VENDOR_ID::VARCHAR (20) MA_EVENT_DESC,
        1 MA_PCT_IND,
        UMDAC.DEFECTIVE_ALLOWANCE_PCT * 100 AS NEW_MA_AMT,
        NEW_MA_AMT AS ORIG_MA_AMT,
        'I' AS INS_UPD_DEL_FLAG 
    FROM
        USR_MA_DEF_ALLOW_CTRL UMDAC 
    JOIN
        (
            SELECT
                FISCAL_MO,
                MIN(DAY_DT) START_DT,
                MAX(DAY_DT) END_DT 
            FROM
                DAYS 
            GROUP BY
                FISCAL_MO
        ) D 
            ON UMDAC.FISCAL_MO = D.FISCAL_MO 
    LEFT JOIN
        MA_EVENT M 
            ON UMDAC.FISCAL_MO = M.FISCAL_MO 
            AND UMDAC.SOURCE_VENDOR_ID = M.SOURCE_VENDOR_ID 
            AND M.MA_EVENT_SOURCE_ID = 13 
    WHERE
        M.MA_EVENT_ID IS NULL 
        AND D.START_DT >= (
            SELECT
                WEEK_DT + 1 
            FROM
                DAYS 
            WHERE
                DAY_DT = CURRENT_DATE - 96
        )""")

df_4.createOrReplaceTempView("SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4")

# COMMAND ----------
# DBTITLE 1, EXP_NEW_5


df_5=spark.sql("""
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
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.START_DT AS START_DT,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.END_DT AS END_DT,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.FISCAL_MO AS FISCAL_MO,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.MA_EVENT_DESC AS MA_EVENT_DESC,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.MA_PCT_IND AS MA_PCT_IND,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.NEW_MA_AMT AS NEW_MA_AMT,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.ORIG_MA_AMT AS ORIG_MA_AMT,
        current_timestamp AS UPDATE_DT,
        current_timestamp AS LOAD_DT,
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.INS_UPD_FLAG AS INS_UPD_FLAG,
        SEQ_MA_EVENT_ID.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SEQ_MA_EVENT_ID 
    INNER JOIN
        SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4 
            ON SEQ_MA_EVENT_ID.Monotonically_Increasing_Id = SQ_USR_MA_DEF_ALLOW_CTRL_INSERTS_4.Monotonically_Increasing_Id""")

df_5.createOrReplaceTempView("EXP_NEW_5")

spark.sql("""UPDATE SEQ_MA_EVENT_ID SET CURRVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_NEW_5) , NEXTVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_NEW_5) + (SELECT Increment_By FROM EXP_NEW_5)""")

# COMMAND ----------
# DBTITLE 1, SQ_USR_MA_DEF_ALLOW_CTRL_UPDATES_6


df_6=spark.sql("""
    SELECT
        M.MA_EVENT_ID,
        M.START_DT,
        M.END_DT,
        80 MA_EVENT_TYPE_ID,
        13 MA_EVENT_SOURCE_ID,
        UMDAC.FISCAL_MO,
        UMDAC.SOURCE_VENDOR_ID,
        'Defective Allowance: FiscalMo=' || UMDAC.FISCAL_MO::VARCHAR (10) || ': SourceVendor=' || UMDAC.SOURCE_VENDOR_ID::VARCHAR (20) MA_EVENT_DESC,
        1 MA_PCT_IND,
        UMDAC.DEFECTIVE_ALLOWANCE_PCT * 100 AS NEW_MA_AMT,
        M.MA_AMT AS ORIG_MA_AMT,
        'U' AS INS_UPD_DEL_FLAG,
        M.LOAD_DT 
    FROM
        USR_MA_DEF_ALLOW_CTRL UMDAC 
    JOIN
        MA_EVENT M 
            ON UMDAC.FISCAL_MO = M.FISCAL_MO 
            AND UMDAC.SOURCE_VENDOR_ID = M.SOURCE_VENDOR_ID 
            AND M.MA_EVENT_SOURCE_ID = 13 
    WHERE
        NEW_MA_AMT <> M.MA_AMT 
        AND M.START_DT >= (
            SELECT
                WEEK_DT + 1 
            FROM
                DAYS 
            WHERE
                DAY_DT = CURRENT_DATE - 96
        )""")

df_6.createOrReplaceTempView("SQ_USR_MA_DEF_ALLOW_CTRL_UPDATES_6")

# COMMAND ----------
# DBTITLE 1, EXP_ORIG_7


df_7=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        NEW_MA_AMT AS NEW_MA_AMT,
        ORIG_MA_AMT AS ORIG_MA_AMT,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        current_timestamp AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_USR_MA_DEF_ALLOW_CTRL_UPDATES_6""")

df_7.createOrReplaceTempView("EXP_ORIG_7")

# COMMAND ----------
# DBTITLE 1, UNI_NEW_ORIG_8


df_8=spark.sql("""SELECT END_DT AS END_DT,
FISCAL_MO AS FISCAL_MO,
INS_UPD_FLAG AS INS_UPD_FLAG,
LOAD_DT AS LOAD_DT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_PCT_IND AS MA_PCT_IND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
NEW_MA_AMT AS NEW_MA_AMT,
ORIG_MA_AMT AS ORIG_MA_AMT,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
START_DT AS START_DT,
UPDATE_DT AS UPDATE_DT FROM EXP_NEW_5 UNION ALL SELECT END_DT AS END_DT,
FISCAL_MO AS FISCAL_MO,
INS_UPD_FLAG AS INS_UPD_FLAG,
LOAD_DT AS LOAD_DT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_PCT_IND AS MA_PCT_IND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
NEW_MA_AMT AS NEW_MA_AMT,
ORIG_MA_AMT AS ORIG_MA_AMT,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
START_DT AS START_DT,
UPDATE_DT AS UPDATE_DT FROM EXP_ORIG_7""")

df_8.createOrReplaceTempView("UNI_NEW_ORIG_8")

# COMMAND ----------
# DBTITLE 1, EXP_INS_UPD_9


df_9=spark.sql("""
    SELECT
        date_trunc('DAY',
        current_timestamp) AS LOAD_DT,
        MA_EVENT_ID AS MA_EVENT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        ORIG_MA_AMT AS ORIG_MA_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT1 AS LOAD_DT1,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UNI_NEW_ORIG_8""")

df_9.createOrReplaceTempView("EXP_INS_UPD_9")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_10


df_10=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        NEW_MA_AMT AS NEW_MA_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UNI_NEW_ORIG_8""")

df_10.createOrReplaceTempView("UPD_STRATEGY_10")

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
INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG FROM EXP_INS_UPD_9""")