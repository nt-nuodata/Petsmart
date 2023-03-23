# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ZTB_SEL_DISCO_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        MATNR AS MATNR,
        WERKS AS WERKS,
        OSTATUS AS OSTATUS,
        STYPE AS STYPE,
        MATKL AS MATKL,
        PID AS PID,
        ZZSTATCD AS ZZSTATCD,
        ZZDSCS AS ZZDSCS,
        ZZMD_SCH_ID AS ZZMD_SCH_ID,
        ZZMKDN AS ZZMKDN,
        DC_EMAIL AS DC_EMAIL,
        DC_WRN_EMAIL AS DC_WRN_EMAIL,
        DC_OEMAIL AS DC_OEMAIL,
        DC_DISCO_DT AS DC_DISCO_DT,
        ST_DISCO_DT AS ST_DISCO_DT,
        ST_DISCO_OW_DT AS ST_DISCO_OW_DT,
        ST_WOFF_DT AS ST_WOFF_DT,
        VKORG AS VKORG,
        ZMERCH_STOR AS ZMERCH_STOR,
        ZMERCH_WEB AS ZMERCH_WEB,
        ZMERCH_CAT AS ZMERCH_CAT,
        CR_DATE AS CR_DATE,
        CHG_DATE AS CHG_DATE,
        ZZBUYR AS ZZBUYR,
        DESCR AS DESCR,
        OLD_ZZSTATCD AS OLD_ZZSTATCD,
        OLD_ZMERCH_STOR AS OLD_ZMERCH_STOR,
        OLD_ZMERCH_WEB AS OLD_ZMERCH_WEB,
        OLD_ZMERCH_CAT AS OLD_ZMERCH_CAT,
        EKGRP AS EKGRP,
        DSCD AS DSCD,
        ZZMD_OVRD_IND AS ZZMD_OVRD_IND,
        OLD_ZZDSCS AS OLD_ZZDSCS,
        ZTIMES AS ZTIMES,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_SEL_DISCO""")

df_0.createOrReplaceTempView("ZTB_SEL_DISCO_0")

# COMMAND ----------
# DBTITLE 1, FF_ZTB_SEL_DISCO
