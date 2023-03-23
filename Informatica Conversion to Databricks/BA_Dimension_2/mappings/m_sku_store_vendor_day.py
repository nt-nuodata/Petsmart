# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SKU_STORE_VENDOR_PRE_0

df_0=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        DELETE_IND AS DELETE_IND,
        SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
        SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCURE_SITE_ID AS PROCURE_SITE_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_STORE_VENDOR_PRE""")

df_0.createOrReplaceTempView("SKU_STORE_VENDOR_PRE_0")

# COMMAND ----------

# DBTITLE 1, ASQ_SHORTCUT_TO_SKU_STORE_VENDOR_PRE_1

df_1=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        DELETE_IND AS DELETE_IND,
        SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
        SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCURE_SITE_ID AS PROCURE_SITE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_STORE_VENDOR_PRE_0""")

df_1.createOrReplaceTempView("ASQ_SHORTCUT_TO_SKU_STORE_VENDOR_PRE_1")

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
        ASQ_SHORTCUT_TO_SKU_STORE_VENDOR_PRE_1""")

df_2.createOrReplaceTempView("EXP_COMMON_DATE_TRANS_2")

# COMMAND ----------

# DBTITLE 1, SKU_STORE_VENDOR_DAY

spark.sql("""INSERT INTO SKU_STORE_VENDOR_DAY SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
VENDOR_ID AS VENDOR_ID,
DELETE_IND AS DELETE_IND,
SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
PROCURE_SITE_ID AS PROCURE_STORE_NBR,
LOAD_DT AS LOAD_DT FROM ASQ_SHORTCUT_TO_SKU_STORE_VENDOR_PRE_1""")

spark.sql("""INSERT INTO SKU_STORE_VENDOR_DAY SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
VENDOR_ID AS VENDOR_ID,
DELETE_IND AS DELETE_IND,
SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
PROCURE_SITE_ID AS PROCURE_STORE_NBR,
LOAD_DT AS LOAD_DT FROM EXP_COMMON_DATE_TRANS_2""")