# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SALES_DAY_STORE_RPT_CRCY_VW_0


df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        LOCATION_ID AS LOCATION_ID,
        TY_LY_FLAG AS TY_LY_FLAG,
        CURRENCY_TYPE_ID AS CURRENCY_TYPE_ID,
        CURRENCY_TYPE_DESC AS CURRENCY_TYPE_DESC,
        WEEK_DT AS WEEK_DT,
        FISCAL_YR AS FISCAL_YR,
        STORE_NBR AS STORE_NBR,
        COMP_FLAG AS COMP_FLAG,
        TXN_CNT AS TXN_CNT,
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
        POS_COUPON_ALLOC_AMT AS POS_COUPON_ALLOC_AMT,
        POS_COUPON_ALLOC_QTY AS POS_COUPON_ALLOC_QTY,
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
        SALES_DAY_STORE_RPT_CRCY_VW""")

df_0.createOrReplaceTempView("SALES_DAY_STORE_RPT_CRCY_VW_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SALES_DAY_STORE_RPT_CRCY_VW_1


df_1=spark.sql("""
    SELECT
        a11.DAY_DT DAY_DATE,
        sum(a11.NET_SALES_AMT * a11.EXCH_RATE_PCT) EDW_Sales 
    FROM
        SALES_DAY_STORE_RPT_CRCY_VW a11 
    JOIN
        SITE_PROFILE_RPT a12 
            ON (
                a11.LOCATION_ID = a12.LOCATION_ID
            ) 
    WHERE
        a11.DAY_DT = (
            SELECT
                current_date - 1
        ) 
        AND a11.TY_LY_FLAG = 'TY' 
        AND a12.STORE_OPEN_CLOSE_FLAG NOT IN (
            'C'
        ) 
        AND a12.LOCATION_TYPE_ID IN (
            8, 6, 15
        ) 
        AND a11.CURRENCY_TYPE_ID IN (
            2
        ) 
    GROUP BY
        a11.DAY_DT""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SALES_DAY_STORE_RPT_CRCY_VW_1")

# COMMAND ----------
# DBTITLE 1, TXN_KEY_2


df_2=spark.sql("""
    SELECT
        TXN_TSTMP AS TXN_TSTMP,
        SITE_NBR AS SITE_NBR,
        REGISTER_NBR AS REGISTER_NBR,
        TXN_NBR AS TXN_NBR,
        DS_ORDER_NBR AS DS_ORDER_NBR,
        DS_ORDER_SEQ_NBR AS DS_ORDER_SEQ_NBR,
        TXN_TYPE_ID AS TXN_TYPE_ID,
        TXN_KEY_GID AS TXN_KEY_GID,
        FIRST_TIB_ROW_GUID AS FIRST_TIB_ROW_GUID,
        TXN_YYYYMMDD AS TXN_YYYYMMDD,
        TXN_00HHMMSS AS TXN_00HHMMSS,
        TXN_DT AS TXN_DT,
        CASHIER_NBR AS CASHIER_NBR,
        VALIDATION_ID AS VALIDATION_ID,
        DS_CHANNEL AS DS_CHANNEL,
        DS_ASSIST_SITE_NBR AS DS_ASSIST_SITE_NBR,
        DS_CURRENCY_CD AS DS_CURRENCY_CD,
        PAYMENT_DEVICE_TYPE AS PAYMENT_DEVICE_TYPE,
        TXN_SEGMENT_CD AS TXN_SEGMENT_CD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TXN_KEY""")

df_2.createOrReplaceTempView("TXN_KEY_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TXN_KEY_3


df_3=spark.sql("""
    SELECT
        date_trunc('DAY',
        T.TXN_DT) AS TXN_DT,
        SUM(NVL(UPC_QTY_X_PRICE,
        0)) + SUM(NVL(LINE_ITEM_DISCOUNTS,
        0)) + SUM(NVL(PETM_CPN_AMT,
        0)) AS STX_SALES 
    FROM
        TXN_KEY T,
        TXN_STATUS S,
        (SELECT
            K.TXN_TSTMP,
            K.TXN_KEY_GID,
            SUM(CASE 
                WHEN NVL(TS.SAP_DEPT_ID,
                0) NOT IN (73,
                93) 
                AND NVL(TS.SAP_CATEGORY_ID,
                0) NOT IN (91006,
                90103) THEN CASE 
                    WHEN NVL(U.SERVICE_REDEMPTION_AMT,
                    0) > 0 THEN U.SERVICE_REDEMPTION_AMT 
                    ELSE U.UPC_QTY * U.UNIT_PRICE_AMT 
                END 
                ELSE 0 
            END) AS UPC_QTY_X_PRICE,
            SUM(CASE 
                WHEN NVL(TS.SAP_DEPT_ID,
                0) NOT IN (73,
                93) 
                AND NVL(TS.SAP_CATEGORY_ID,
                0) NOT IN (91006,
                90103) THEN U.EXTENDED_PRICE_AMT - (U.UPC_QTY * U.UNIT_PRICE_AMT) 
                ELSE 0 
            END) AS LINE_ITEM_DISCOUNTS,
            SUM(CASE 
                WHEN NVL(TS.SAP_DEPT_ID,
                0) NOT IN (73,
                93) 
                AND NVL(TS.SAP_CATEGORY_ID,
                0) NOT IN (91006,
                90103) 
                AND U.ITEM_BIT0 = 0 THEN U.UPC_QTY 
                ELSE 0 
            END) AS UPC_QTY,
            SUM(CASE 
                WHEN NVL(TS.SAP_DEPT_ID,
                0) NOT IN (73,
                93) 
                AND NVL(TS.SAP_CATEGORY_ID,
                0) NOT IN (91006,
                90103) 
                AND U.ITEM_BIT0 = 1 THEN U.UPC_QTY 
                ELSE 0 
            END) AS UPC_QTY_RETURN,
            SUM(CASE 
                WHEN NVL(TS.SAP_DEPT_ID,
                0) IN (93) THEN U.UPC_QTY * U.UNIT_PRICE_AMT 
                ELSE 0 
            END) AS CHARTIES_AMT,
            SUM(CASE 
                WHEN D.GL_ACCOUNT_NBR IN (24611) THEN U.UPC_QTY * U.UNIT_PRICE_AMT 
                ELSE 0 
            END) AS SANTA_PHOTO_AMT 
        FROM
            TXN_KEY K 
        JOIN
            TXN_STATUS S 
                ON K.TXN_TSTMP = S.TXN_TSTMP 
                AND K.TXN_KEY_GID = S.TXN_KEY_GID 
        JOIN
            TXN_UPC_SCAN U 
                ON K.TXN_TSTMP = U.TXN_TSTMP 
                AND K.TXN_KEY_GID = U.TXN_KEY_GID 
        LEFT JOIN
            TXN_UPC TU 
                ON NVL(U.UPC_SWIPED_ID,
            U.UPC_KEYED_ID) = TU.UPC_ID 
        LEFT JOIN
            TXN_SKU TS 
                ON TU.SKU_NBR = TS.SKU_NBR 
        LEFT JOIN
            (
                SELECT
                    DISTINCT SKU_NBR,
                    GL_ACCOUNT_NBR 
                FROM
                    NCAST.DM_GL_ACCT_ASSIGNMENT
            ) D 
                ON NVL(TS.SKU_NBR,
            5000002) = NVL(D.SKU_NBR,
            5000002) 
        WHERE
            K.TXN_TSTMP >= date_trunc('DAY', current_timestamp - 1) 
            AND K.TXN_TSTMP < date_trunc('DAY', current_timestamp) 
            AND S.TXN_WAS_MID_VOIDED_FLAG = 0 
            AND S.TXN_WAS_POST_VOIDED_FLAG = 0 
            AND U.ITEM_BIT2 = 0 
            AND U.ITEM_BIT7 = 0 
            AND S.TXN_SETTLED_FLAG = 1 
            AND CASE 
                WHEN NVL(U.SERVICE_REDEMPTION_AMT, 0) > 0 THEN 0 
                ELSE TS.SPECIAL_SALES_FLAG 
            END <> 1 
            AND K.TXN_TYPE_ID IN (
                2, 26, 37, 54
            ) 
        GROUP BY
            K.TXN_TSTMP,
            K.TXN_KEY_GID) T1,
            (SELECT
                K.TXN_TSTMP,
                K.TXN_KEY_GID,
                SUM(U.CPN_AMT) AS PETM_CPN_AMT 
            FROM
                TXN_STATUS S,
                TXN_COUPON_REDEEM U,
                TXN_KEY K 
            WHERE
                K.TXN_TSTMP >= date_trunc('DAY', current_timestamp - 1) 
                AND K.TXN_TSTMP < date_trunc('DAY', current_timestamp) 
                AND K.TXN_TSTMP = S.TXN_TSTMP 
                AND K.TXN_KEY_GID = S.TXN_KEY_GID 
                AND K.TXN_TSTMP = U.TXN_TSTMP 
                AND K.TXN_KEY_GID = U.TXN_KEY_GID 
                AND S.TXN_WAS_MID_VOIDED_FLAG = 0 
                AND S.TXN_WAS_POST_VOIDED_FLAG = 0 
                AND U.ITEM_BIT2 = 0 
                AND S.TXN_SETTLED_FLAG = 1 
                AND K.TXN_TYPE_ID IN (
                    2, 26, 37, 54
                ) 
                AND U.CPN_TYPE_ID NOT IN (
                    1
                ) 
            GROUP BY
                K.TXN_TSTMP,
                K.TXN_KEY_GID) T2 
        WHERE
            T.TXN_TSTMP >= date_trunc('DAY', current_timestamp - 1) 
            AND T.TXN_TSTMP < date_trunc('DAY', current_timestamp) 
            AND T.TXN_TSTMP = S.TXN_TSTMP 
            AND T.TXN_KEY_GID = S.TXN_KEY_GID 
            AND T.TXN_TSTMP = T1.TXN_TSTMP(+) 
            AND T.TXN_KEY_GID = T1.TXN_KEY_GID(+) 
            AND T.TXN_TSTMP = T2.TXN_TSTMP(+) 
            AND T.TXN_KEY_GID = T2.TXN_KEY_GID(+) 
            AND S.TXN_WAS_MID_VOIDED_FLAG = 0 
            AND S.TXN_WAS_POST_VOIDED_FLAG = 0 
            AND S.TXN_SETTLED_FLAG = 1 
            AND T.TXN_TYPE_ID IN (
                2, 26, 37, 54
            ) 
        GROUP BY
            TRUNC(T.TXN_DT)""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_TXN_KEY_3")

# COMMAND ----------
# DBTITLE 1, EXP_TXN_TSTMP_4


df_4=spark.sql("""
    SELECT
        date_trunc('DAY',
        TXN_TSTMP) AS TXN_TSTMP,
        STX_SALES_AMT AS STX_SALES_AMT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_TXN_KEY_3""")

df_4.createOrReplaceTempView("EXP_TXN_TSTMP_4")

# COMMAND ----------
# DBTITLE 1, JNR_STX_EDW_5


df_5=spark.sql("""
    SELECT
        DETAIL.DAY_DT AS DAY_DT,
        DETAIL.EDW_SALES_AMT AS EDW_SALES_AMT,
        MASTER.TXN_TSTMP AS TXN_TSTMP,
        MASTER.STX_SALES_AMT AS STX_SALES_AMT,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_TXN_TSTMP_4 MASTER 
    INNER JOIN
        SQ_Shortcut_to_SALES_DAY_STORE_RPT_CRCY_VW_1 DETAIL 
            ON MASTER.TXN_TSTMP = DETAIL.DAY_DT""")

df_5.createOrReplaceTempView("JNR_STX_EDW_5")

# COMMAND ----------
# DBTITLE 1, EXP_VARIANCE_CALC_6


df_6=spark.sql("""
    SELECT
        TXN_TSTMP AS TXN_TSTMP,
        current_timestamp AS BATCH_DATE,
        EDW_SALES_AMT AS EDW_SALES_AMT,
        STX_SALES_AMT AS STX_SALES_AMT,
        (((EDW_SALES_AMT - STX_SALES_AMT) / EDW_SALES_AMT) * 100) AS SALES_VARIANCE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_STX_EDW_5""")

df_6.createOrReplaceTempView("EXP_VARIANCE_CALC_6")

# COMMAND ----------
# DBTITLE 1, UPD_BATCH_LOAD_AUD_7


df_7=spark.sql("""
    SELECT
        TXN_TSTMP AS TXN_TSTMP,
        BATCH_DATE AS BATCH_DATE,
        EDW_SALES_AMT AS EDW_SALES_AMT,
        STX_SALES_AMT AS STX_SALES_AMT,
        SALES_VARIANCE AS SALES_VARIANCE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_VARIANCE_CALC_6""")

df_7.createOrReplaceTempView("UPD_BATCH_LOAD_AUD_7")

# COMMAND ----------
# DBTITLE 1, BATCH_LOAD_AUD_LOG


spark.sql("""INSERT INTO BATCH_LOAD_AUD_LOG SELECT DAY_DT AS DAY_DT,
BATCH_DATE AS BATCH_DATE,
AOS AS AOS,
ISPU AS ISPU,
SFS AS SFS,
STR AS STR,
WEB AS WEB,
STX_COUNT AS STX_COUNT,
EDW_COUNT AS EDW_COUNT,
EDW_SALES AS EDW_SALES,
STX_SALES AS STX_SALES,
PLAN_SALES_AMT AS PLAN_SALES,
ACTUAL_SALES_AMT AS ACTUAL_SALES,
SALES_VARIANCE AS SALES_VARIANCE,
PLAN_VARIANCE AS PLAN_VARIANCE FROM UPD_BATCH_LOAD_AUD_7""")