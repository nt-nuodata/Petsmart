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

# DBTITLE 1, SQ_Shortcut_To_CURRENCY_1

df_1=spark.sql("""
    SELECT
        CURRENCY_ID AS CURRENCY_ID,
        DATE_RATE_ENDED AS DATE_RATE_ENDED,
        DATE_RATE_START AS DATE_RATE_START,
        CURRENCY_TYPE AS CURRENCY_TYPE,
        EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
        RATIO_FROM AS RATIO_FROM,
        RATIO_TO AS RATIO_TO,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        CURRENCY_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_CURRENCY_1")

# COMMAND ----------

# DBTITLE 1, FIL_NULLS_2

df_2=spark.sql("""
    SELECT
        CURRENCY_ID AS CURRENCY_ID,
        DATE_RATE_ENDED AS DATE_RATE_ENDED,
        DATE_RATE_START AS DATE_RATE_START,
        CURRENCY_TYPE AS CURRENCY_TYPE,
        EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
        RATIO_FROM AS RATIO_FROM,
        RATIO_TO AS RATIO_TO,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_CURRENCY_1 
    WHERE
        IFF(ISNULL(CURRENCY_ID) 
        OR IS_SPACES(CURRENCY_ID), FALSE, TRUE)""")

df_2.createOrReplaceTempView("FIL_NULLS_2")

# COMMAND ----------

# DBTITLE 1, EXP_DATE_RATE_START_FORMAT_3

df_3=spark.sql("""
    SELECT
        CURRENCY_ID AS CURRENCY_ID,
        DATE_RATE_ENDED AS DATE_RATE_ENDED,
        DATE_RATE_START AS DATE_RATE_START,
        TO_DATE(DATE_RATE_START,
        'YYYYMMDD') AS DATE_RATE_START1,
        CURRENCY_TYPE AS CURRENCY_TYPE,
        EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
        RATIO_FROM AS RATIO_FROM,
        RATIO_TO AS RATIO_TO,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_NULLS_2""")

df_3.createOrReplaceTempView("EXP_DATE_RATE_START_FORMAT_3")

# COMMAND ----------

# DBTITLE 1, LKP_CURRENCY_4

df_4=spark.sql("""
    SELECT
        EXP_DATE_RATE_START_FORMAT_3.CURRENCY_ID AS IN_CURRENCY_ID,
        CURRENCY_ID AS CURRENCY_ID,
        EXP_DATE_RATE_START_FORMAT_3.DATE_RATE_START1 AS IN_DATE_RATE_START,
        DATE_RATE_START AS DATE_RATE_START,
        CURRENCY_TYPE AS CURRENCY_TYPE,
        DATE_RATE_ENDED AS DATE_RATE_ENDED,
        EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
        RATIO_TO AS RATIO_TO,
        RATIO_FROM AS RATIO_FROM,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        EXP_DATE_RATE_START_FORMAT_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        CURRENCY 
    RIGHT OUTER JOIN
        EXP_DATE_RATE_START_FORMAT_3 
            ON CURRENCY_ID = EXP_DATE_RATE_START_FORMAT_3.CURRENCY_ID 
            AND DATE_RATE_START = IN_DATE_RATE_START""")

df_4.createOrReplaceTempView("LKP_CURRENCY_4")

# COMMAND ----------

# DBTITLE 1, EXP_DetectChanges_5

df_5=spark.sql("""
    SELECT
        L_CURRENCY_ID AS L_CURRENCY_ID,
        L_DATE_RATE_START AS L_DATE_RATE_START,
        DATE_RATE_START1 AS DATE_RATE_START1,
        IFF(ISNULL(CURRENCY_ID) 
        AND ISNULL(DATE_RATE_START),
        TRUE,
        FALSE) AS NewFlag,
        IFF(NOT ISNULL(CURRENCY_ID),
        TRUE,
        FALSE) AS ChangedFlag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_CURRENCY_4""")

df_5.createOrReplaceTempView("EXP_DetectChanges_5")

# COMMAND ----------

# DBTITLE 1, EXP_Convert_6

df_6=spark.sql("""
    SELECT
        EXP_DATE_RATE_START_FORMAT_3.CURRENCY_ID AS CURRENCY_ID,
        TO_DATE(EXP_DATE_RATE_START_FORMAT_3.DATE_RATE_ENDED,
        'YYYYMMDD') AS V_DATE_RATE_ENDED,
        TO_DATE(EXP_DATE_RATE_START_FORMAT_3.DATE_RATE_START,
        'YYYYMMDD') AS V_DATE_RATE_START,
        EXP_DATE_RATE_START_FORMAT_3.CURRENCY_TYPE AS CURRENCY_TYPE,
        EXP_DATE_RATE_START_FORMAT_3.EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
        EXP_DATE_RATE_START_FORMAT_3.RATIO_FROM AS RATIO_FROM,
        EXP_DATE_RATE_START_FORMAT_3.RATIO_TO AS RATIO_TO,
        EXP_DATE_RATE_START_FORMAT_3.STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        IFF(EXP_DATE_RATE_START_FORMAT_3.CURRENCY_ID = 'USD',
        0,
        1) AS CURRENCY_NBR,
        EXP_DetectChanges_5.NewFlag AS NewFlag,
        EXP_DetectChanges_5.ChangedFlag AS ChangedFlag,
        EXP_DetectChanges_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DetectChanges_5 
    INNER JOIN
        EXP_DATE_RATE_START_FORMAT_3 
            ON EXP_DetectChanges_5.Monotonically_Increasing_Id = EXP_DATE_RATE_START_FORMAT_3.Monotonically_Increasing_Id""")

df_6.createOrReplaceTempView("EXP_Convert_6")

# COMMAND ----------

# DBTITLE 1, UPD_Ins_Upd_7

df_7=spark.sql("""
    SELECT
        CURRENCY_ID AS CURRENCY_ID,
        V_DATE_RATE_START AS V_DATE_RATE_START,
        CURRENCY_TYPE AS CURRENCY_TYPE,
        V_DATE_RATE_ENDED AS V_DATE_RATE_ENDED,
        EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
        RATIO_TO AS RATIO_TO,
        RATIO_FROM AS RATIO_FROM,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        CURRENCY_NBR AS CURRENCY_NBR,
        NewFlag AS NewFlag,
        ChangedFlag AS ChangedFlag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_Convert_6""")

df_7.createOrReplaceTempView("UPD_Ins_Upd_7")

# COMMAND ----------

# DBTITLE 1, CURRENCY

spark.sql("""INSERT INTO CURRENCY SELECT CURRENCY_ID AS CURRENCY_ID,
V_DATE_RATE_START AS DATE_RATE_START,
CURRENCY_TYPE AS CURRENCY_TYPE,
V_DATE_RATE_ENDED AS DATE_RATE_ENDED,
EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
RATIO_TO AS RATIO_TO,
RATIO_FROM AS RATIO_FROM,
STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
CURRENCY_NBR AS CURRENCY_NBR FROM UPD_Ins_Upd_7""")
