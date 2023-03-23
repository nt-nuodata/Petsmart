# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, CURRENCY_0


df_0=spark.sql("""
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

df_0.createOrReplaceTempView("CURRENCY_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_CURRENCY_1


df_1=spark.sql("""
    SELECT
        DAYS.DAY_DT,
        CURRENCY_ID,
        DATE_RATE_START,
        CURRENCY_TYPE,
        DATE_RATE_ENDED,
        EXCHANGE_RATE_PCNT,
        RATIO_TO,
        RATIO_FROM,
        STORE_CTRY_ABBR,
        CURRENCY_NBR 
    FROM
        CURRENCY C,
        DAYS 
    WHERE
        DAYS.DAY_DT BETWEEN DATE_RATE_START AND DATE_RATE_ENDED 
        AND DAYS.DAY_DT < CURRENT_TIMESTAMP + INTERVAL '14 days' 
        AND CURRENCY_ID = 'CAD'""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_CURRENCY_1")

# COMMAND ----------
# DBTITLE 1, CURRENCY_DAY


spark.sql("""INSERT INTO CURRENCY_DAY SELECT DAY_DT AS DAY_DT,
TABLE_NAME AS CURRENCY_ID,
DATE_RATE_START AS DATE_RATE_START,
CURRENCY_TYPE AS CURRENCY_TYPE,
DATE_RATE_ENDED AS DATE_RATE_ENDED,
EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
RATIO_TO AS RATIO_TO,
RATIO_FROM AS RATIO_FROM,
STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
CURRENCY_NBR AS CURRENCY_NBR FROM ASQ_Shortcut_to_CURRENCY_1""")