# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ZPROMO_0


df_0=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        ARTICLE AS ARTICLE,
        SITE AS SITE,
        POGID AS POGID,
        REPL_START_DATE AS REPL_START_DATE,
        REPL_END_DATE AS REPL_END_DATE,
        LIST_START_DATE AS LIST_START_DATE,
        LIST_END_DATE AS LIST_END_DATE,
        PROMO_QTY AS PROMO_QTY,
        LAST_CNG_DATE AS LAST_CNG_DATE,
        STATUS AS STATUS,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZPROMO""")

df_0.createOrReplaceTempView("ZPROMO_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_ZPROMO_1


df_1=spark.sql("""
    SELECT
        DELETE_IND AS DELETE_IND,
        ARTICLE AS ARTICLE,
        SITE AS SITE,
        POGID AS POGID,
        REPL_START_DATE AS REPL_START_DATE,
        REPL_END_DATE AS REPL_END_DATE,
        LIST_START_DATE AS LIST_START_DATE,
        LIST_END_DATE AS LIST_END_DATE,
        PROMO_QTY AS PROMO_QTY,
        LAST_CNG_DATE AS LAST_CNG_DATE,
        STATUS AS STATUS,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ZPROMO_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_ZPROMO_1")

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
        SQ_Shortcut_To_ZPROMO_1""")

df_2.createOrReplaceTempView("EXP_COMMON_DATE_TRANS_2")

# COMMAND ----------
# DBTITLE 1, EXP_INSERT_SYSDATE_3


df_3=spark.sql("""
    SELECT
        date_trunc('DAY',
        current_timestamp) AS INSERT_SYSDATE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_ZPROMO_1""")

df_3.createOrReplaceTempView("EXP_INSERT_SYSDATE_3")

# COMMAND ----------
# DBTITLE 1, ZTB_PROMO_PRE


spark.sql("""INSERT INTO ZTB_PROMO_PRE SELECT ARTICLE AS SKU_NBR,
SITE AS STORE_NBR,
POGID AS POG_NBR,
REPL_START_DT AS REPL_START_DT,
REPL_END_DT AS REPL_END_DT,
LIST_START_DT AS LIST_START_DT,
LIST_END_DT AS LIST_END_DT,
PROMO_QTY AS PROMO_QTY,
LAST_CHNG_DT AS LAST_CHNG_DT,
STATUS AS POG_STATUS,
DELETE_IND AS DELETE_IND,
DATE_ADDED AS DATE_ADDED,
DATE_REFRESHED AS DATE_REFRESHED,
DATE_DELETED AS DATE_DELETED FROM SQ_Shortcut_To_ZPROMO_1""")

spark.sql("""INSERT INTO ZTB_PROMO_PRE SELECT ARTICLE AS SKU_NBR,
SITE AS STORE_NBR,
POGID AS POG_NBR,
REPL_START_DT AS REPL_START_DT,
REPL_END_DT AS REPL_END_DT,
LIST_START_DT AS LIST_START_DT,
LIST_END_DT AS LIST_END_DT,
PROMO_QTY AS PROMO_QTY,
LAST_CHNG_DT AS LAST_CHNG_DT,
STATUS AS POG_STATUS,
DELETE_IND AS DELETE_IND,
DATE_ADDED AS DATE_ADDED,
DATE_REFRESHED AS DATE_REFRESHED,
DATE_DELETED AS DATE_DELETED FROM EXP_INSERT_SYSDATE_3""")

spark.sql("""INSERT INTO ZTB_PROMO_PRE SELECT ARTICLE AS SKU_NBR,
SITE AS STORE_NBR,
POGID AS POG_NBR,
REPL_START_DT AS REPL_START_DT,
REPL_END_DT AS REPL_END_DT,
LIST_START_DT AS LIST_START_DT,
LIST_END_DT AS LIST_END_DT,
PROMO_QTY AS PROMO_QTY,
LAST_CHNG_DT AS LAST_CHNG_DT,
STATUS AS POG_STATUS,
DELETE_IND AS DELETE_IND,
DATE_ADDED AS DATE_ADDED,
DATE_REFRESHED AS DATE_REFRESHED,
DATE_DELETED AS DATE_DELETED FROM EXP_COMMON_DATE_TRANS_2""")

spark.sql("""INSERT INTO ZTB_PROMO_PRE SELECT ARTICLE AS SKU_NBR,
SITE AS STORE_NBR,
POGID AS POG_NBR,
REPL_START_DT AS REPL_START_DT,
REPL_END_DT AS REPL_END_DT,
LIST_START_DT AS LIST_START_DT,
LIST_END_DT AS LIST_END_DT,
PROMO_QTY AS PROMO_QTY,
LAST_CHNG_DT AS LAST_CHNG_DT,
STATUS AS POG_STATUS,
DELETE_IND AS DELETE_IND,
DATE_ADDED AS DATE_ADDED,
DATE_REFRESHED AS DATE_REFRESHED,
DATE_DELETED AS DATE_DELETED FROM EXP_COMMON_DATE_TRANS_2""")

spark.sql("""INSERT INTO ZTB_PROMO_PRE SELECT ARTICLE AS SKU_NBR,
SITE AS STORE_NBR,
POGID AS POG_NBR,
REPL_START_DT AS REPL_START_DT,
REPL_END_DT AS REPL_END_DT,
LIST_START_DT AS LIST_START_DT,
LIST_END_DT AS LIST_END_DT,
PROMO_QTY AS PROMO_QTY,
LAST_CHNG_DT AS LAST_CHNG_DT,
STATUS AS POG_STATUS,
DELETE_IND AS DELETE_IND,
DATE_ADDED AS DATE_ADDED,
DATE_REFRESHED AS DATE_REFRESHED,
DATE_DELETED AS DATE_DELETED FROM EXP_COMMON_DATE_TRANS_2""")

spark.sql("""INSERT INTO ZTB_PROMO_PRE SELECT ARTICLE AS SKU_NBR,
SITE AS STORE_NBR,
POGID AS POG_NBR,
REPL_START_DT AS REPL_START_DT,
REPL_END_DT AS REPL_END_DT,
LIST_START_DT AS LIST_START_DT,
LIST_END_DT AS LIST_END_DT,
PROMO_QTY AS PROMO_QTY,
LAST_CHNG_DT AS LAST_CHNG_DT,
STATUS AS POG_STATUS,
DELETE_IND AS DELETE_IND,
DATE_ADDED AS DATE_ADDED,
DATE_REFRESHED AS DATE_REFRESHED,
DATE_DELETED AS DATE_DELETED FROM EXP_COMMON_DATE_TRANS_2""")

spark.sql("""INSERT INTO ZTB_PROMO_PRE SELECT ARTICLE AS SKU_NBR,
SITE AS STORE_NBR,
POGID AS POG_NBR,
REPL_START_DT AS REPL_START_DT,
REPL_END_DT AS REPL_END_DT,
LIST_START_DT AS LIST_START_DT,
LIST_END_DT AS LIST_END_DT,
PROMO_QTY AS PROMO_QTY,
LAST_CHNG_DT AS LAST_CHNG_DT,
STATUS AS POG_STATUS,
DELETE_IND AS DELETE_IND,
DATE_ADDED AS DATE_ADDED,
DATE_REFRESHED AS DATE_REFRESHED,
DATE_DELETED AS DATE_DELETED FROM EXP_COMMON_DATE_TRANS_2""")