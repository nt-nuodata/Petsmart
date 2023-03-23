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
# DBTITLE 1, SQ_Shortcut_to_ZTB_SEL_DISCO_1


df_1=spark.sql("""
    SELECT
        d.matnr,
        CASE 
            WHEN d.stype <> ' ' THEN d.stype 
            ELSE NULL 
        END stype,
        CASE 
            WHEN d.zzmd_sch_id <> ' ' THEN d.zzmd_sch_id 
            ELSE NULL 
        END zzmd_sch_id,
        CASE 
            WHEN d.pid <> ' ' THEN d.pid 
            ELSE NULL 
        END pid,
        CASE 
            WHEN d.zzdscs <> ' ' THEN d.zzdscs 
            ELSE NULL 
        END zzdscs,
        CASE 
            WHEN d.zzmkdn <> ' ' THEN d.zzmkdn 
            ELSE NULL 
        END zzmkdn,
        CASE 
            WHEN d.dc_disco_dt <> ' ' THEN d.dc_disco_dt 
            ELSE NULL 
        END dc_disco_dt,
        CASE 
            WHEN d.st_disco_dt <> ' ' THEN d.st_disco_dt 
            ELSE NULL 
        END st_disco_dt,
        CASE 
            WHEN d.st_disco_ow_dt <> ' ' THEN d.st_disco_ow_dt 
            ELSE NULL 
        END st_disco_ow_dt,
        CASE 
            WHEN d.st_woff_dt <> ' ' THEN d.st_woff_dt 
            ELSE NULL 
        END st_woff_dt 
    FROM
        SAPPR3.ZTB_SEL_DISCO d 
    WHERE
        d.mandt = '100' 
        AND d.werks = ' '""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_SEL_DISCO_1")

# COMMAND ----------
# DBTITLE 1, FIL_IS_NUMBER_CHECK_2


df_2=spark.sql("""
    SELECT
        MATNR AS MATNR,
        ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
        ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
        ZDISCO_PID_DT AS ZDISCO_PID_DT,
        ZDISCO_START_DT AS ZDISCO_START_DT,
        ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
        ZDISCO_DC_DT AS ZDISCO_DC_DT,
        ZDISCO_STR_DT AS ZDISCO_STR_DT,
        ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
        ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ZTB_SEL_DISCO_1 
    WHERE
        IS_NUMBER(MATNR)""")

df_2.createOrReplaceTempView("FIL_IS_NUMBER_CHECK_2")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_3


df_3=spark.sql("""
    SELECT
        MATNR AS MATNR,
        ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
        ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
        TO_DATE(ZDISCO_PID_DT,
        'YYYYMMDD') AS ZDISCO_PID_DT,
        TO_DATE(ZDISCO_START_DT,
        'YYYYMMDD') AS ZDISCO_START_DT,
        TO_DATE(ZDISCO_INIT_MKDN_DT,
        'YYYYMMDD') AS ZDISCO_INIT_MKDN_DT,
        TO_DATE(ZDISCO_DC_DT,
        'YYYYMMDD') AS ZDISCO_DC_DT,
        TO_DATE(ZDISCO_STR_DT,
        'YYYYMMDD') AS ZDISCO_STR_DT,
        TO_DATE(ZDISCO_STR_OWNRSHP_DT,
        'YYYYMMDD') AS ZDISCO_STR_OWNRSHP_DT,
        TO_DATE(ZDISCO_STR_WRT_OFF_DT,
        'YYYYMMDD') AS ZDISCO_STR_WRT_OFF_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_IS_NUMBER_CHECK_2""")

df_3.createOrReplaceTempView("EXPTRANS_3")

# COMMAND ----------
# DBTITLE 1, ZTB_SEL_DISCO_PRE


spark.sql("""INSERT INTO ZTB_SEL_DISCO_PRE SELECT MATNR AS SKU_NBR,
ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
ZDISCO_PID_DT AS ZDISCO_PID_DT,
ZDISCO_START_DT AS ZDISCO_START_DT,
ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
ZDISCO_DC_DT AS ZDISCO_DC_DT,
ZDISCO_STR_DT AS ZDISCO_STR_DT,
ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM EXPTRANS_3""")