# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, LISTING_PRE_0

df_0=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        STORE_NBR AS STORE_NBR,
        SKU_NBR AS SKU_NBR,
        LISTING_END_DT AS LISTING_END_DT,
        LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
        LISTING_EFF_DT AS LISTING_EFF_DT,
        LISTING_MODULE_ID AS LISTING_MODULE_ID,
        LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
        NEGATE_FLAG AS NEGATE_FLAG,
        STRUCT_COMP_FLAG AS STRUCT_COMP_FLAG,
        STRUCT_ARTICLE_FLAG AS STRUCT_ARTICLE_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LISTING_PRE""")

df_0.createOrReplaceTempView("LISTING_PRE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_To_LISTING_PRE_1

df_1=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        SKU_NBR AS SKU_NBR,
        LISTING_END_DT AS LISTING_END_DT,
        LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
        LISTING_EFF_DT AS LISTING_EFF_DT,
        LISTING_MODULE_ID AS LISTING_MODULE_ID,
        LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
        NEGATE_FLAG AS NEGATE_FLAG,
        STRUCT_COMP_FLAG AS STRUCT_COMP_FLAG,
        STRUCT_ARTICLE_FLAG AS STRUCT_ARTICLE_FLAG,
        DELETE_IND AS DELETE_IND,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LISTING_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_LISTING_PRE_1")

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
        SQ_Shortcut_To_LISTING_PRE_1""")

df_2.createOrReplaceTempView("EXP_COMMON_DATE_TRANS_2")

# COMMAND ----------

# DBTITLE 1, LISTING_HST

spark.sql("""INSERT INTO LISTING_HST SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
LISTING_END_DT AS LISTING_END_DT,
LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
LISTING_EFF_DT AS LISTING_EFF_DT,
LISTING_MODULE_ID AS LISTING_MODULE_ID,
LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
NEGATE_FLAG AS NEGATE_FLAG,
STRUCT_COMP_FLAG AS STRUCT_COMP_CD,
STRUCT_ARTICLE_FLAG AS STRUCT_ARTICLE_NBR,
DELETE_IND AS DELETE_IND,
LOAD_DT AS LOAD_DT FROM SQ_Shortcut_To_LISTING_PRE_1""")

spark.sql("""INSERT INTO LISTING_HST SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
LISTING_END_DT AS LISTING_END_DT,
LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
LISTING_EFF_DT AS LISTING_EFF_DT,
LISTING_MODULE_ID AS LISTING_MODULE_ID,
LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
NEGATE_FLAG AS NEGATE_FLAG,
STRUCT_COMP_FLAG AS STRUCT_COMP_CD,
STRUCT_ARTICLE_FLAG AS STRUCT_ARTICLE_NBR,
DELETE_IND AS DELETE_IND,
LOAD_DT AS LOAD_DT FROM EXP_COMMON_DATE_TRANS_2""")

spark.sql("""INSERT INTO LISTING_HST SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
LISTING_END_DT AS LISTING_END_DT,
LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
LISTING_EFF_DT AS LISTING_EFF_DT,
LISTING_MODULE_ID AS LISTING_MODULE_ID,
LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
NEGATE_FLAG AS NEGATE_FLAG,
STRUCT_COMP_FLAG AS STRUCT_COMP_CD,
STRUCT_ARTICLE_FLAG AS STRUCT_ARTICLE_NBR,
DELETE_IND AS DELETE_IND,
LOAD_DT AS LOAD_DT FROM EXP_COMMON_DATE_TRANS_2""")
