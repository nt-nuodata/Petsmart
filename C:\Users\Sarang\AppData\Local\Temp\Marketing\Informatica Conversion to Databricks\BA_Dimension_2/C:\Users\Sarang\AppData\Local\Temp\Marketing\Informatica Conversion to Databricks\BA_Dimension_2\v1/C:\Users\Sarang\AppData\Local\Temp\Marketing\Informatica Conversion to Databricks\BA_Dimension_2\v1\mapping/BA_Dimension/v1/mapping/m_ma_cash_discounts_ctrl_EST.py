# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MA_CASH_DISCOUNT_CTRL_0


df_0=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        EST_CASH_DISCOUNT_PCT AS EST_CASH_DISCOUNT_PCT,
        ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
        ACT_CASH_DISCOUNT_PCT AS ACT_CASH_DISCOUNT_PCT,
        OVRD_CASH_DISCOUNT_PCT AS OVRD_CASH_DISCOUNT_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_CASH_DISCOUNT_CTRL""")

df_0.createOrReplaceTempView("MA_CASH_DISCOUNT_CTRL_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_1


df_1=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        EST_CASH_DISCOUNT_PCT AS EST_CASH_DISCOUNT_PCT,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MA_CASH_DISCOUNT_CTRL_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_1")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE_RPT_2


df_2=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_TYPE_DESC AS VENDOR_TYPE_DESC,
        VENDOR_NBR AS VENDOR_NBR,
        LOCATION_ID AS LOCATION_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        MGR_ID AS MGR_ID,
        MGR_DESC AS MGR_DESC,
        DVL_ID AS DVL_ID,
        DVL_DESC AS DVL_DESC,
        EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
        PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        PAYMENT_TERM_DESC AS PAYMENT_TERM_DESC,
        INCO_TERM_CD AS INCO_TERM_CD,
        INCO_TERM_DESC AS INCO_TERM_DESC,
        ADDRESS AS ADDRESS,
        CITY AS CITY,
        STATE AS STATE,
        STATE_NAME AS STATE_NAME,
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        ZIP AS ZIP,
        CONTACT AS CONTACT,
        CONTACT_PHONE AS CONTACT_PHONE,
        PHONE AS PHONE,
        PHONE_EXT AS PHONE_EXT,
        FAX AS FAX,
        RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
        RTV_TYPE_CD AS RTV_TYPE_CD,
        RTV_TYPE_DESC AS RTV_TYPE_DESC,
        RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
        INDUSTRY_CD AS INDUSTRY_CD,
        LATITUDE AS LATITUDE,
        LONGITUDE AS LONGITUDE,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        ADD_DT AS ADD_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PROFILE_RPT""")

df_2.createOrReplaceTempView("VENDOR_PROFILE_RPT_2")

# COMMAND ----------
# DBTITLE 1, SALES_DAY_SKU_STORE_RPT_3


df_3=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        WEEK_DT AS WEEK_DT,
        FISCAL_YR AS FISCAL_YR,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        OPT_SALES_TYPE_ID AS OPT_SALES_TYPE_ID,
        VENDOR_ID AS VENDOR_ID,
        PROMO_FLAG AS PROMO_FLAG,
        STATUS_ID AS STATUS_ID,
        BRAND_NAME AS BRAND_NAME,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        SKU_VEND_TXN_CNT AS SKU_VEND_TXN_CNT,
        NET_SALES_AMT AS NET_SALES_AMT,
        NET_SALES_QTY AS NET_SALES_QTY,
        NET_MARGIN_AMT AS NET_MARGIN_AMT,
        SALES_AMT AS SALES_AMT,
        SALES_COST AS SALES_COST,
        SALES_QTY AS SALES_QTY,
        RETURN_AMT AS RETURN_AMT,
        RETURN_COST AS RETURN_COST,
        RETURN_QTY AS RETURN_QTY,
        CLEARANCE_AMT AS CLEARANCE_AMT,
        CLEARANCE_QTY AS CLEARANCE_QTY,
        CLEARANCE_RETURN_AMT AS CLEARANCE_RETURN_AMT,
        CLEARANCE_RETURN_QTY AS CLEARANCE_RETURN_QTY,
        DISCOUNT_AMT AS DISCOUNT_AMT,
        DISCOUNT_QTY AS DISCOUNT_QTY,
        DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
        DISCOUNT_RETURN_QTY AS DISCOUNT_RETURN_QTY,
        POS_COUPON_AMT AS POS_COUPON_AMT,
        POS_COUPON_QTY AS POS_COUPON_QTY,
        SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
        SPECIAL_SALES_QTY AS SPECIAL_SALES_QTY,
        SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
        SPECIAL_RETURN_QTY AS SPECIAL_RETURN_QTY,
        SPECIAL_SRVC_AMT AS SPECIAL_SRVC_AMT,
        SPECIAL_SRVC_QTY AS SPECIAL_SRVC_QTY,
        MA_SALES_AMT AS MA_SALES_AMT,
        MA_SALES_QTY AS MA_SALES_QTY,
        MA_TRANS_AMT AS MA_TRANS_AMT,
        MA_TRANS_COST AS MA_TRANS_COST,
        MA_TRANS_QTY AS MA_TRANS_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_DAY_SKU_STORE_RPT""")

df_3.createOrReplaceTempView("SALES_DAY_SKU_STORE_RPT_3")

# COMMAND ----------
# DBTITLE 1, SQ_EST_CASH_DISCOUNT_PCT_4


df_4=spark.sql("""
    SELECT
        DISTINCT V.VENDOR_ID,
        SUBSTR(V.PAYMENT_TERM_DESC,
        1,
        INSTR(V.PAYMENT_TERM_DESC,
        '%') - 1) TXT_EST_CASH_DISCOUNT_PCT 
    FROM
        VENDOR_PROFILE_RPT V 
    JOIN
        SALES_DAY_SKU_STORE_RPT S 
            ON V.VENDOR_ID = S.VENDOR_ID 
    WHERE
        S.DAY_DT >= CURRENT_DATE - 36 
        AND NVL(TXT_EST_CASH_DISCOUNT_PCT, '') <> ''""")

df_4.createOrReplaceTempView("SQ_EST_CASH_DISCOUNT_PCT_4")

# COMMAND ----------
# DBTITLE 1, EXP_VENDOR_ID_5


df_5=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        IFF(IS_NUMBER(TXT_EST_CASH_DISCOUNT_PCT),
        TO_DECIMAL(TXT_EST_CASH_DISCOUNT_PCT) * .01,
        0) AS THIS_EST_CASH_DISCOUNT_PCT,
        1 AS FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_EST_CASH_DISCOUNT_PCT_4""")

df_5.createOrReplaceTempView("EXP_VENDOR_ID_5")

# COMMAND ----------
# DBTITLE 1, FTR_REMOVE_INVALID_6


df_6=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        THIS_EST_CASH_DISCOUNT_PCT AS THIS_EST_CASH_DISCOUNT_PCT,
        FLAG AS FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_VENDOR_ID_5 
    WHERE
        THIS_EST_CASH_DISCOUNT_PCT <> 0""")

df_6.createOrReplaceTempView("FTR_REMOVE_INVALID_6")

# COMMAND ----------
# DBTITLE 1, DAYS_7


df_7=spark.sql("""
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

df_7.createOrReplaceTempView("DAYS_7")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_DAYS_8


df_8=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        FISCAL_MO AS FISCAL_MO,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        DAYS_7 
    WHERE
        DAYS.DAY_DT = CURRENT_DATE - 1""")

df_8.createOrReplaceTempView("SQ_Shortcut_To_DAYS_8")

# COMMAND ----------
# DBTITLE 1, EXP_FLAG_9


df_9=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        1 AS FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_DAYS_8""")

df_9.createOrReplaceTempView("EXP_FLAG_9")

# COMMAND ----------
# DBTITLE 1, JNR_CROSS_JOIN_10


df_10=spark.sql("""
    SELECT
        MASTER.VENDOR_ID AS VENDOR_ID,
        MASTER.THIS_EST_CASH_DISCOUNT_PCT AS THIS_EST_CASH_DISCOUNT_PCT,
        MASTER.FLAG AS FLAG,
        DETAIL.FISCAL_MO AS FISCAL_MO,
        DETAIL.FLAG AS FLAG1,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FTR_REMOVE_INVALID_6 MASTER 
    INNER JOIN
        EXP_FLAG_9 DETAIL 
            ON MASTER.FLAG = DETAIL.FLAG""")

df_10.createOrReplaceTempView("JNR_CROSS_JOIN_10")

# COMMAND ----------
# DBTITLE 1, JNR_MA_CASH_DISCOUNT_CTRL_11


df_11=spark.sql("""
    SELECT
        MASTER.VENDOR_ID AS VENDOR_ID,
        MASTER.THIS_EST_CASH_DISCOUNT_PCT AS THIS_EST_CASH_DISCOUNT_PCT,
        MASTER.FISCAL_MO AS FISCAL_MO1,
        DETAIL.FISCAL_MO AS FISCAL_MO,
        DETAIL.SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        DETAIL.EST_CASH_DISCOUNT_PCT AS EST_CASH_DISCOUNT_PCT,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_CROSS_JOIN_10 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_1 DETAIL 
            ON MASTER.VENDOR_ID = SOURCE_VENDOR_ID 
            AND FISCAL_MO1 = DETAIL.FISCAL_MO""")

df_11.createOrReplaceTempView("JNR_MA_CASH_DISCOUNT_CTRL_11")

# COMMAND ----------
# DBTITLE 1, EXP_INS_UPD_FLAG_12


df_12=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        VENDOR_ID AS VENDOR_ID,
        THIS_EST_CASH_DISCOUNT_PCT AS THIS_EST_CASH_DISCOUNT_PCT,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP,
        IFF(ISNULL(FISCAL_MO),
        'I',
        IFF(THIS_EST_CASH_DISCOUNT_PCT <> v_EST_CASH_DISCOUNT_PCT,
        'U',
        'X')) AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_MA_CASH_DISCOUNT_CTRL_11""")

df_12.createOrReplaceTempView("EXP_INS_UPD_FLAG_12")

# COMMAND ----------
# DBTITLE 1, FIL_INS_UPD_FLAG_13


df_13=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        VENDOR_ID AS VENDOR_ID,
        THIS_EST_CASH_DISCOUNT_PCT AS THIS_EST_CASH_DISCOUNT_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_INS_UPD_FLAG_12 
    WHERE
        INS_UPD_FLAG <> 'X'""")

df_13.createOrReplaceTempView("FIL_INS_UPD_FLAG_13")

# COMMAND ----------
# DBTITLE 1, SRT_DISTINCT_14


df_14=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        VENDOR_ID AS VENDOR_ID,
        THIS_EST_CASH_DISCOUNT_PCT AS THIS_EST_CASH_DISCOUNT_PCT,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_INS_UPD_FLAG_13 
    ORDER BY
        FISCAL_MO ASC,
        VENDOR_ID ASC,
        THIS_EST_CASH_DISCOUNT_PCT ASC,
        INS_UPD_FLAG ASC,
        UPDATE_TSTMP ASC,
        LOAD_TSTMP ASC""")

df_14.createOrReplaceTempView("SRT_DISTINCT_14")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_15


df_15=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        VENDOR_ID AS VENDOR_ID,
        THIS_EST_CASH_DISCOUNT_PCT AS THIS_EST_CASH_DISCOUNT_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SRT_DISTINCT_14""")

df_15.createOrReplaceTempView("UPD_STRATEGY_15")

# COMMAND ----------
# DBTITLE 1, MA_CASH_DISCOUNT_CTRL


spark.sql("""INSERT INTO MA_CASH_DISCOUNT_CTRL SELECT FISCAL_MO AS FISCAL_MO,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
EST_CASH_DISCOUNT_PCT AS EST_CASH_DISCOUNT_PCT,
ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
ACT_CASH_DISCOUNT_PCT AS ACT_CASH_DISCOUNT_PCT,
OVRD_CASH_DISCOUNT_PCT1 AS OVRD_CASH_DISCOUNT_PCT,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPD_STRATEGY_15""")