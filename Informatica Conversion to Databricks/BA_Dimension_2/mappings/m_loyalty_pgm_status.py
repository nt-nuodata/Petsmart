# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, LOYALTY_PRE_0


df_0=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        STORE_DESC AS STORE_DESC,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LOYALTY_PRE""")

df_0.createOrReplaceTempView("LOYALTY_PRE_0")

# COMMAND ----------
# DBTITLE 1, CURRENCY_1


df_1=spark.sql("""
    SELECT
        CURRENCY_ID AS CURRENCY_ID,
        DATE_RATE_START AS DATE_RATE_START,
        CURRENCY_TYPE AS CURRENCY_TYPE,
        DATE_RATE_ENDED AS DATE_RATE_ENDED,
        EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
        RATIO_TO AS RATIO_TO,
        RATIO_FROM AS RATIO_FROM,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        CURRENCY_NBR AS CURRENCY_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        CURRENCY""")

df_1.createOrReplaceTempView("CURRENCY_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_LOYALTY_PRE_2


df_2=spark.sql("""
    SELECT
        DISTINCT LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC 
    FROM
        LOYALTY_PRE 
    WHERE
        LOYALTY_PGM_STATUS_ID IS NOT NULL 
        AND LOYALTY_PGM_STATUS_DESC IS NOT NULL""")

df_2.createOrReplaceTempView("ASQ_Shortcut_To_LOYALTY_PRE_2")

# COMMAND ----------
# DBTITLE 1, LOYALTY_PGM_STATUS


spark.sql("""INSERT INTO LOYALTY_PGM_STATUS SELECT LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC FROM ASQ_Shortcut_To_LOYALTY_PRE_2""")