# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, NATIONAL_COST_FLAT_0


df_0=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        COND_TYPE_CD AS COND_TYPE_CD,
        VENDOR_ID AS VENDOR_ID,
        SKU_NBR AS SKU_NBR,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        PURCH_INFO_CD AS PURCH_INFO_CD,
        COND_END_DT AS COND_END_DT,
        COND_EFF_DT AS COND_EFF_DT,
        COND_RECORD_NBR AS COND_RECORD_NBR,
        COND_AMT AS COND_AMT,
        PROMOTION_CD AS PROMOTION_CD,
        COND_RT_UNIT AS COND_RT_UNIT,
        COND_PRICE_UNIT AS COND_PRICE_UNIT,
        COND_UNIT AS COND_UNIT,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        NATIONAL_COST_FLAT""")

df_0.createOrReplaceTempView("NATIONAL_COST_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_NATIONAL_COST_FLAT_1


df_1=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        COND_TYPE_CD AS COND_TYPE_CD,
        VENDOR_ID AS VENDOR_ID,
        SKU_NBR AS SKU_NBR,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        PURCH_INFO_CD AS PURCH_INFO_CD,
        COND_END_DT AS COND_END_DT,
        COND_EFF_DT AS COND_EFF_DT,
        COND_RECORD_NBR AS COND_RECORD_NBR,
        COND_AMT AS COND_AMT,
        PROMOTION_CD AS PROMOTION_CD,
        COND_RT_UNIT AS COND_RT_UNIT,
        COND_PRICE_UNIT AS COND_PRICE_UNIT,
        COND_UNIT AS COND_UNIT,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        NATIONAL_COST_FLAT_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_NATIONAL_COST_FLAT_1")

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
        SQ_Shortcut_To_NATIONAL_COST_FLAT_1""")

df_2.createOrReplaceTempView("EXP_COMMON_DATE_TRANS_2")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_VENDOR_TRANS_3


df_3=spark.sql("""
    SELECT
        IFF(SUBSTR(VENDOR_ID,
        1,
        1) = 'V',
        TO_FLOAT(SUBSTR(VENDOR_ID,
        2,
        4)) + 900000,
        TO_FLOAT(VENDOR_ID)) AS VENDOR_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_NATIONAL_COST_FLAT_1""")

df_3.createOrReplaceTempView("EXP_COMMON_VENDOR_TRANS_3")

# COMMAND ----------
# DBTITLE 1, FIL_Invalid_Records_4


df_4=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        COND_TYPE_CD AS COND_TYPE_CD,
        VENDOR_ID AS VENDOR_ID,
        SKU_NBR AS SKU_NBR,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        PURCH_INFO_CD AS PURCH_INFO_CD,
        COND_END_DT AS COND_END_DT,
        COND_EFF_DT AS COND_EFF_DT,
        COND_RECORD_NBR AS COND_RECORD_NBR,
        COND_AMT AS COND_AMT,
        PROMOTION_CD AS PROMOTION_CD,
        COND_RT_UNIT AS COND_RT_UNIT,
        COND_PRICE_UNIT AS COND_PRICE_UNIT,
        COND_UNIT AS COND_UNIT,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_COMMON_VENDOR_TRANS_3 
    WHERE
        PURCH_INFO_CD = '0'""")

df_4.createOrReplaceTempView("FIL_Invalid_Records_4")

df_4=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        COND_TYPE_CD AS COND_TYPE_CD,
        VENDOR_ID AS VENDOR_ID,
        SKU_NBR AS SKU_NBR,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        PURCH_INFO_CD AS PURCH_INFO_CD,
        COND_END_DT AS COND_END_DT,
        COND_EFF_DT AS COND_EFF_DT,
        COND_RECORD_NBR AS COND_RECORD_NBR,
        COND_AMT AS COND_AMT,
        PROMOTION_CD AS PROMOTION_CD,
        COND_RT_UNIT AS COND_RT_UNIT,
        COND_PRICE_UNIT AS COND_PRICE_UNIT,
        COND_UNIT AS COND_UNIT,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_COMMON_DATE_TRANS_2 
    WHERE
        PURCH_INFO_CD = '0'""")

df_4.createOrReplaceTempView("FIL_Invalid_Records_4")

df_4=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        COND_TYPE_CD AS COND_TYPE_CD,
        VENDOR_ID AS VENDOR_ID,
        SKU_NBR AS SKU_NBR,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        PURCH_INFO_CD AS PURCH_INFO_CD,
        COND_END_DT AS COND_END_DT,
        COND_EFF_DT AS COND_EFF_DT,
        COND_RECORD_NBR AS COND_RECORD_NBR,
        COND_AMT AS COND_AMT,
        PROMOTION_CD AS PROMOTION_CD,
        COND_RT_UNIT AS COND_RT_UNIT,
        COND_PRICE_UNIT AS COND_PRICE_UNIT,
        COND_UNIT AS COND_UNIT,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_NATIONAL_COST_FLAT_1 
    WHERE
        PURCH_INFO_CD = '0'""")

df_4.createOrReplaceTempView("FIL_Invalid_Records_4")

df_4=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        COND_TYPE_CD AS COND_TYPE_CD,
        VENDOR_ID AS VENDOR_ID,
        SKU_NBR AS SKU_NBR,
        PURCH_ORG_CD AS PURCH_ORG_CD,
        PURCH_INFO_CD AS PURCH_INFO_CD,
        COND_END_DT AS COND_END_DT,
        COND_EFF_DT AS COND_EFF_DT,
        COND_RECORD_NBR AS COND_RECORD_NBR,
        COND_AMT AS COND_AMT,
        PROMOTION_CD AS PROMOTION_CD,
        COND_RT_UNIT AS COND_RT_UNIT,
        COND_PRICE_UNIT AS COND_PRICE_UNIT,
        COND_UNIT AS COND_UNIT,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_COMMON_DATE_TRANS_2 
    WHERE
        PURCH_INFO_CD = '0'""")

df_4.createOrReplaceTempView("FIL_Invalid_Records_4")

# COMMAND ----------
# DBTITLE 1, NATIONAL_COST_PRE


spark.sql("""INSERT INTO NATIONAL_COST_PRE SELECT SKU_NBR AS SKU_NBR,
VENDOR_ID AS VENDOR_ID,
PURCH_ORG_CD AS PURCH_ORG_CD,
COND_TYPE_CD AS COND_TYPE_CD,
PURCH_INFO_CD AS PURCH_INFO_CD,
COND_END_DT AS COND_END_DT,
DELETE_IND AS DELETE_IND,
COND_EFF_DT AS COND_EFF_DT,
COND_RECORD_NBR AS COND_RECORD_NBR,
COND_AMT AS COND_AMT,
PROMOTION_CD AS PROMOTION_CD,
COND_RT_UNIT AS COND_RT_UNIT,
COND_PRICE_UNIT AS COND_PRICE_UNIT,
COND_UNIT AS COND_UNIT,
UNIT_NUMERATOR AS UNIT_NUMERATOR,
UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
LOAD_DT AS LOAD_DT FROM FIL_Invalid_Records_4""")