# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, WM_SHIP_VIA_0


df_0=spark.sql("""
    SELECT
        WHSE AS WHSE,
        SHIP_VIA AS SHIP_VIA,
        SHIP_VIA_DESC AS SHIP_VIA_DESC,
        CARR_ID AS CARR_ID,
        CREATE_DATE_TIME AS CREATE_DATE_TIME,
        MOD_DATE_TIME AS MOD_DATE_TIME,
        USER_ID AS USER_ID,
        DEL_FLG AS DEL_FLG,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        WM_SHIP_VIA""")

df_0.createOrReplaceTempView("WM_SHIP_VIA_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WM_SHIP_VIA_1


df_1=spark.sql("""
    SELECT
        SHIP_VIA,
        MAX(UPPER(TRIM(SHIP_VIA_DESC))) AS SHIP_VIA_DESC 
    FROM
        WM_SHIP_VIA 
    WHERE
        DEL_FLG = 0 
    GROUP BY
        SHIP_VIA""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_WM_SHIP_VIA_1")

# COMMAND ----------
# DBTITLE 1, CARRIER_PROFILE_WMS_PRE


spark.sql("""INSERT INTO CARRIER_PROFILE_WMS_PRE SELECT SHIP_VIA AS SHIP_VIA,
SHIP_VIA_DESC AS SHIP_VIA_DESC FROM SQ_Shortcut_to_WM_SHIP_VIA_1""")