# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, CURRENCY_DAY_0


df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
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
        CURRENCY_DAY""")

df_0.createOrReplaceTempView("CURRENCY_DAY_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_CURRENCY_DAY_1


df_1=spark.sql("""
    SELECT
        'CURRENCY_DAY' LIMIT 1""")

df_1.createOrReplaceTempView("ASQ_Shortcut_to_CURRENCY_DAY_1")

# COMMAND ----------
# DBTITLE 1, FIL_TRUNC_2


df_2=spark.sql("""
    SELECT
        TABLE_NAME AS TABLE_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_to_CURRENCY_DAY_1 
    WHERE
        FALSE""")

df_2.createOrReplaceTempView("FIL_TRUNC_2")

# COMMAND ----------
# DBTITLE 1, DUMMY_TARGET


spark.sql("""INSERT INTO DUMMY_TARGET SELECT TABLE_NAME AS COMMENT FROM ASQ_Shortcut_to_CURRENCY_DAY_1""")

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
CURRENCY_NBR AS CURRENCY_NBR FROM FIL_TRUNC_2""")