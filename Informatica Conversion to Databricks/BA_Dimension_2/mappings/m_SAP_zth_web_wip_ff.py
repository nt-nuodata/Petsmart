# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ZTH_WEB_WIP_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        RECORD_ID AS RECORD_ID,
        MATKL AS MATKL,
        WSTAW AS WSTAW,
        POG_REPLACE AS POG_REPLACE,
        DISC_ARTICLE AS DISC_ARTICLE,
        RTV_DISP AS RTV_DISP,
        SELL_THROUGH AS SELL_THROUGH,
        PRES_MGR AS PRES_MGR,
        NOTIFY_PRICE AS NOTIFY_PRICE,
        PRICE_MGR AS PRICE_MGR,
        KHOP AS KHOP,
        COLOR AS COLOR,
        SIZE1 AS SIZE1,
        FLAVOR AS FLAVOR,
        RTV AS RTV,
        DISCIPLINE AS DISCIPLINE,
        STATUS_CODE AS STATUS_CODE,
        PLAN_GROUP AS PLAN_GROUP,
        STATELINETACK AS STATELINETACK,
        DUE_IN_STORE AS DUE_IN_STORE,
        QTY_FOR_DC4 AS QTY_FOR_DC4,
        RDPRF AS RDPRF,
        APU_QTY AS APU_QTY,
        USITEM AS USITEM,
        CAITEM AS CAITEM,
        US_CA_DIR_ITEM AS US_CA_DIR_ITEM,
        INLINE AS INLINE,
        DPR_SKU AS DPR_SKU,
        DPR_PLUS AS DPR_PLUS,
        DPR_PERC AS DPR_PERC,
        DPR_MIN_QTY AS DPR_MIN_QTY,
        OTB AS OTB,
        BUYER_SUGGEST AS BUYER_SUGGEST,
        BUYER_A AS BUYER_A,
        BUYER_B AS BUYER_B,
        BUYER_C AS BUYER_C,
        BUYER_D AS BUYER_D,
        BUYER_E AS BUYER_E,
        BUYER_F AS BUYER_F,
        DC8 AS DC8,
        DC9 AS DC9,
        DC10 AS DC10,
        DC12 AS DC12,
        S2920 AS S2920,
        ALLOC AS ALLOC,
        PRODUCT_CODE AS PRODUCT_CODE,
        BASE_NUMBER AS BASE_NUMBER,
        BASE_DESC AS BASE_DESC,
        SLT_CAT_CODE AS SLT_CAT_CODE,
        SUB_PROD_CODE AS SUB_PROD_CODE,
        SADDLE AS SADDLE,
        SIZE_CHART_NUM AS SIZE_CHART_NUM,
        CASE_PACK_QTY AS CASE_PACK_QTY,
        HAZ_MAT AS HAZ_MAT,
        AEROSOL AS AEROSOL,
        ROLL AS ROLL,
        OVER_SIZE AS OVER_SIZE,
        PSM_LABEL AS PSM_LABEL,
        OWNS AS OWNS,
        BASE_GEN AS BASE_GEN,
        DCLR AS DCLR,
        DSZE AS DSZE,
        DBRD AS DBRD,
        DFRT AS DFRT,
        STOR AS STOR,
        WEB AS WEB,
        CATL AS CATL,
        DTCD AS DTCD,
        DLBL AS DLBL,
        PROJECT_ID AS PROJECT_ID,
        CREATED_BY AS CREATED_BY,
        CREATED_ON AS CREATED_ON,
        CREATE_TIME AS CREATE_TIME,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_ON AS LAST_CHANGED_ON,
        LAST_CHANGE_TIME AS LAST_CHANGE_TIME,
        DC36 AS DC36,
        S2910 AS S2910,
        S2930 AS S2930,
        AVG_SALES AS AVG_SALES,
        SUBC AS SUBC,
        SPID AS SPID,
        DC38 AS DC38,
        AVG_CATWEB AS AVG_CATWEB,
        INIT_BUY AS INIT_BUY,
        FORC_ARTICLE AS FORC_ARTICLE,
        FORC_PCT AS FORC_PCT,
        ALL_STORES AS ALL_STORES,
        LIM_STORES AS LIM_STORES,
        WEB_STYLE AS WEB_STYLE,
        WEB_STYLE_TEXT AS WEB_STYLE_TEXT,
        WEB_HAZD AS WEB_HAZD,
        DC41 AS DC41,
        DC14 AS DC14,
        DC16 AS DC16,
        DC18 AS DC18,
        EXP_DAYS AS EXP_DAYS,
        MHDRZ AS MHDRZ,
        DC39 AS DC39,
        DC40 AS DC40,
        DC42 AS DC42,
        FORC_SIGN AS FORC_SIGN,
        PRITEM AS PRITEM,
        DC43 AS DC43,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTH_WEB_WIP""")

df_0.createOrReplaceTempView("ZTH_WEB_WIP_0")

# COMMAND ----------
# DBTITLE 1, FF_ZTH_WEB_WIP1
