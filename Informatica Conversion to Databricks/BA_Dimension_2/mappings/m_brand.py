# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, BRAND_PRE_0

df_0=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        BRAND_NAME AS BRAND_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BRAND_PRE""")

df_0.createOrReplaceTempView("BRAND_PRE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_BRAND_PRE_1

df_1=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        BRAND_NAME AS BRAND_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        BRAND_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_BRAND_PRE_1")

# COMMAND ----------

# DBTITLE 1, Brand_2

df_2=spark.sql("""
    SELECT
        BrandCd AS BrandCd,
        BrandName AS BrandName,
        BrandTypeCd AS BrandTypeCd,
        LoadTstmp AS LoadTstmp,
        UpdateTstmp AS UpdateTstmp,
        BrandClassificationCd AS BrandClassificationCd,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        Brand""")

df_2.createOrReplaceTempView("Brand_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_BRAND_3

df_3=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        BRAND_NAME AS BRAND_NAME,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Brand_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_BRAND_3")

# COMMAND ----------

# DBTITLE 1, JNR_BRAND_4

df_4=spark.sql("""
    SELECT
        DETAIL.BRAND_CD AS BRAND_CD,
        DETAIL.BRAND_NAME AS BRAND_NAME,
        MASTER.BRAND_CD AS BRAND_CD_pre,
        MASTER.BRAND_NAME AS BRAND_NAME_pre,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_BRAND_PRE_1 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_BRAND_3 DETAIL 
            ON MASTER.BRAND_CD = DETAIL.BRAND_CD""")

df_4.createOrReplaceTempView("JNR_BRAND_4")

# COMMAND ----------

# DBTITLE 1, FIL_BRAND_5

df_5=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        BRAND_NAME AS BRAND_NAME,
        BRAND_CD_pre AS BRAND_CD_pre,
        BRAND_NAME_pre AS BRAND_NAME_pre,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_BRAND_4 
    WHERE
        NOT ISNULL(BRAND_CD) 
        AND NOT ISNULL(BRAND_CD_pre) 
        AND BRAND_NAME != BRAND_NAME_pre 
        OR NOT ISNULL(BRAND_CD) 
        AND ISNULL(BRAND_CD_pre) 
        OR ISNULL(BRAND_CD) 
        AND NOT ISNULL(BRAND_CD_pre)""")

df_5.createOrReplaceTempView("FIL_BRAND_5")

# COMMAND ----------

# DBTITLE 1, EXP_LOAD_STRATEGY_6

df_6=spark.sql("""
    SELECT
        DECODE(v_LOAD_STRATEGY_FLAG,
        'I',
        BRAND_CD_pre,
        'U',
        BRAND_CD_pre,
        'D',
        BRAND_CD) AS BRAND_CD,
        DECODE(v_LOAD_STRATEGY_FLAG,
        'I',
        BRAND_NAME_pre,
        'U',
        BRAND_NAME_pre,
        'D',
        BRAND_NAME) AS BRAND_NAME,
        IFF(v_LOAD_STRATEGY_FLAG = 'D',
        1,
        0) AS DELETE_FLAG,
        current_timestamp AS UPDATE_DT,
        current_timestamp AS LOAD_DT,
        v_LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_BRAND_5""")

df_6.createOrReplaceTempView("EXP_LOAD_STRATEGY_6")

# COMMAND ----------

# DBTITLE 1, UPS_BRAND_7

df_7=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        BRAND_NAME AS BRAND_NAME,
        DELETE_FLAG AS DELETE_FLAG,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        LOAD_STRATEGY_FLAG AS LOAD_STRATEGY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_LOAD_STRATEGY_6""")

df_7.createOrReplaceTempView("UPS_BRAND_7")

# COMMAND ----------

# DBTITLE 1, BRAND

spark.sql("""INSERT INTO BRAND SELECT BRAND_CD AS BRAND_CD,
BRAND_NAME AS BRAND_NAME,
DELETE_FLAG AS DELETE_FLAG,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPS_BRAND_7""")
