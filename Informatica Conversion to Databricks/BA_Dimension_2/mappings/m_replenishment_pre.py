# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_FILE_0


df_0=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        PURCH_GRP_ID AS PURCH_GRP_ID,
        PROCURE_TYPE_CD AS PROCURE_TYPE_CD,
        ROUNDNG_VALUE AS ROUNDNG_VALUE,
        REPL_TYPE_CD AS REPL_TYPE_CD,
        SAFETY_QTY AS SAFETY_QTY,
        SERVICE_LVL_RT AS SERVICE_LVL_RT,
        ABC_INDICATOR_CD AS ABC_INDICATOR_CD,
        MAX_STOCK_QTY AS MAX_STOCK_QTY,
        REORDER_POINT_QTY AS REORDER_POINT_QTY,
        PERIOD_CD AS PERIOD_CD,
        PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
        PROFIT_CENTER_ID AS PROFIT_CENTER_ID,
        MULTIPLIER_RT AS MULTIPLIER_RT,
        FDC_INDICATOR_CD AS FDC_INDICATOR_CD,
        MAX_TARGET_STOCK_QTY AS MAX_TARGET_STOCK_QTY,
        MIN_TARGET_STOCK_QTY AS MIN_TARGET_STOCK_QTY,
        TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
        TARGET_COVERAGE_CNT AS TARGET_COVERAGE_CNT,
        PRESENT_QTY AS PRESENT_QTY,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REPLENISHMENT_FILE""")

df_0.createOrReplaceTempView("REPLENISHMENT_FILE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_REPLENISHMENT_FILE_1


df_1=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        SAFETY_QTY AS SAFETY_QTY,
        SERVICE_LVL_RT AS SERVICE_LVL_RT,
        REORDER_POINT_QTY AS REORDER_POINT_QTY,
        PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
        TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
        PRESENT_QTY AS PRESENT_QTY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        REPLENISHMENT_FILE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_REPLENISHMENT_FILE_1")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_DATE_TRANS_2


df_2=spark.sql("""
    SELECT
        IFF(DELETE_DT = '00000000',
        NULL,
        TO_DATE(DELETE_DT,
        'MMDDYYYY')) AS o_MMDDYYYY_W_DEFAULT_TIME,
        IFF(DELETE_DT = '00000000',
        NULL,
        TO_DATE(DELETE_DT,
        'YYYYMMDD')) AS o_YYYYMMDD_W_DEFAULT_TIME,
        TO_DATE(('9999-12-31.' || i_TIME_ONLY),
        'YYYY-MM-DD.HH24MISS') AS o_TIME_W_DEFAULT_DATE,
        IFF(DELETE_DT = '00000000',
        NULL,
        TO_DATE((DELETE_DT || '.' || i_TIME_ONLY),
        'MMDDYYYY.HH24:MI:SS')) AS o_MMDDYYYY_W_TIME,
        IFF(DELETE_DT = '00000000',
        NULL,
        TO_DATE((DELETE_DT || '.' || i_TIME_ONLY),
        'YYYYMMDD.HH24:MI:SS')) AS o_YYYYMMDD_W_TIME,
        date_trunc('DAY',
        SESSSTARTTIME) AS o_CURRENT_DATE,
        ADD_TO_DATE(v_CURRENT_DATE,
        'DD',
        -1) AS o_CURRENT_DATE_MINUS1,
        TO_DATE('0001-01-01',
        'YYYY-MM-DD') AS o_DEFAULT_EFF_DATE,
        TO_DATE('9999-12-31',
        'YYYY-MM-DD') AS o_DEFAULT_END_DATE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_REPLENISHMENT_FILE_1""")

df_2.createOrReplaceTempView("EXP_COMMON_DATE_TRANS_2")

# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_PRE


spark.sql("""INSERT INTO REPLENISHMENT_PRE SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
DELETE_IND AS DELETE_IND,
SAFETY_QTY AS SAFETY_QTY,
SERVICE_LVL_RT AS SERVICE_LVL_RT,
REORDER_POINT_QTY AS REORDER_POINT_QTY,
PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
PRESENT_QTY AS PRESENT_QTY,
PROMO_QTY AS PROMO_QTY,
o_CURRENT_DATE AS LOAD_DT FROM EXP_COMMON_DATE_TRANS_2""")

spark.sql("""INSERT INTO REPLENISHMENT_PRE SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
DELETE_IND AS DELETE_IND,
SAFETY_QTY AS SAFETY_QTY,
SERVICE_LVL_RT AS SERVICE_LVL_RT,
REORDER_POINT_QTY AS REORDER_POINT_QTY,
PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
PRESENT_QTY AS PRESENT_QTY,
PROMO_QTY AS PROMO_QTY,
o_CURRENT_DATE AS LOAD_DT FROM SQ_Shortcut_To_REPLENISHMENT_FILE_1""")