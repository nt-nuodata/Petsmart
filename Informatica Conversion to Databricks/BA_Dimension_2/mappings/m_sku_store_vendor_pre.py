# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ARTICLE_SITE_VENDOR_0

df_0=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        ARTICLE_ID AS ARTICLE_ID,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
        SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCUR_SITE_ID AS PROCUR_SITE_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ARTICLE_SITE_VENDOR""")

df_0.createOrReplaceTempView("ARTICLE_SITE_VENDOR_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1

df_1=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
        SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCUR_SITE_ID AS PROCUR_SITE_ID,
        DELETE_IND AS DELETE_IND,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ARTICLE_SITE_VENDOR_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1")

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
        SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1""")

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
        SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1""")

df_3.createOrReplaceTempView("EXP_COMMON_VENDOR_TRANS_3")

# COMMAND ----------

# DBTITLE 1, AGGTRANS_4

df_4=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        MAX(i_From_Date) AS o_FROM_DATE,
        o_To_Date AS o_To_Date,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCUR_SITE_ID AS PROCUR_SITE_ID,
        DELETE_IND AS DELETE_IND 
    FROM
        SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1 
    GROUP BY
        ARTICLE_ID,
        STORE_NBR,
        VENDOR_ID""")

df_4.createOrReplaceTempView("AGGTRANS_4")

df_4=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        MAX(i_From_Date) AS o_FROM_DATE,
        o_To_Date AS o_To_Date,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCUR_SITE_ID AS PROCUR_SITE_ID,
        DELETE_IND AS DELETE_IND 
    FROM
        EXP_COMMON_VENDOR_TRANS_3 
    GROUP BY
        ARTICLE_ID,
        STORE_NBR,
        VENDOR_ID""")

df_4.createOrReplaceTempView("AGGTRANS_4")

df_4=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        MAX(i_From_Date) AS o_FROM_DATE,
        o_To_Date AS o_To_Date,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCUR_SITE_ID AS PROCUR_SITE_ID,
        DELETE_IND AS DELETE_IND 
    FROM
        EXP_COMMON_DATE_TRANS_2 
    GROUP BY
        ARTICLE_ID,
        STORE_NBR,
        VENDOR_ID""")

df_4.createOrReplaceTempView("AGGTRANS_4")

df_4=spark.sql("""
    SELECT
        ARTICLE_ID AS ARTICLE_ID,
        STORE_NBR AS STORE_NBR,
        VENDOR_ID AS VENDOR_ID,
        MAX(i_From_Date) AS o_FROM_DATE,
        o_To_Date AS o_To_Date,
        FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
        PROCUR_SITE_ID AS PROCUR_SITE_ID,
        DELETE_IND AS DELETE_IND 
    FROM
        EXP_COMMON_DATE_TRANS_2 
    GROUP BY
        ARTICLE_ID,
        STORE_NBR,
        VENDOR_ID""")

df_4.createOrReplaceTempView("AGGTRANS_4")

# COMMAND ----------

# DBTITLE 1, SKU_STORE_VENDOR_PRE

spark.sql("""INSERT INTO SKU_STORE_VENDOR_PRE SELECT ARTICLE_ID AS SKU_NBR,
STORE_NBR AS STORE_NBR,
VENDOR_ID AS VENDOR_ID,
DELETE_IND AS DELETE_IND,
o_FROM_DATE AS SOURCE_LIST_EFF_DT,
o_To_Date AS SOURCE_LIST_END_DT,
FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
PROCUR_SITE_ID AS PROCURE_SITE_ID FROM AGGTRANS_4""")
