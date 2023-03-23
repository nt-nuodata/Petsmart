# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SITE_DM_HIST_0


df_0=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        SITE_DM_EFF_DT AS SITE_DM_EFF_DT,
        DIST_MGR_NAME AS DIST_MGR_NAME,
        SITE_DM_END_DT AS SITE_DM_END_DT,
        CURRENT_DIST_MGR_CD AS CURRENT_DIST_MGR_CD,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_DM_HIST""")

df_0.createOrReplaceTempView("SITE_DM_HIST_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_DM_HIST_1


df_1=spark.sql("""
    SELECT
        LOCATION_ID,
        SITE_DM_EFF_DT,
        DIST_MGR_NAME,
        LOAD_TSTMP 
    FROM
        (SELECT
            LOCATION_ID,
            SITE_DM_EFF_DT,
            DIST_MGR_NAME,
            SITE_DM_END_DT,
            CURRENT_DIST_MGR_CD,
            UPDATE_TSTMP,
            LOAD_TSTMP,
            RANK() OVER (PARTITION 
        BY
            LOCATION_ID 
        ORDER BY
            LOAD_TSTMP DESC) AS RANK 
        FROM
            SITE_DM_HIST 
        WHERE
            CURRENT_DIST_MGR_CD = 'Y') a 
    WHERE
        RANK = 2""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SITE_DM_HIST_1")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_LOGIC_2


df_2=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        SITE_DM_EFF_DT AS SITE_DM_EFF_DT,
        DIST_MGR_NAME AS DIST_MGR_NAME,
        'N' AS o_CURRENT_DIST_MGR_CD,
        current_timestamp AS o_UPDATE_TSTMP,
        ADD_TO_DATE(current_timestamp,
        'DD',
        -1) AS o_SITE_DM_END_DT,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SITE_DM_HIST_1""")

df_2.createOrReplaceTempView("EXP_UPDATE_LOGIC_2")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_UPDATE_ONLY_3


df_3=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        SITE_DM_EFF_DT AS SITE_DM_EFF_DT,
        DIST_MGR_NAME AS DIST_MGR_NAME,
        o_SITE_DM_END_DT AS o_SITE_DM_END_DT,
        o_CURRENT_DIST_MGR_CD AS o_CURRENT_DIST_MGR_CD,
        o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_UPDATE_LOGIC_2""")

df_3.createOrReplaceTempView("UPDTRANS_UPDATE_ONLY_3")

# COMMAND ----------
# DBTITLE 1, SITE_DM_HIST


spark.sql("""INSERT INTO SITE_DM_HIST SELECT NEW_LOCATION_ID1 AS LOCATION_ID,
SITE_DM_EFF_DT AS SITE_DM_EFF_DT,
NEW_DIST_MGR_NAME1 AS DIST_MGR_NAME,
SITE_DM_END_DT AS SITE_DM_END_DT,
CURRENT_DIST_MGR_CD AS CURRENT_DIST_MGR_CD,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPDTRANS_UPDATE_ONLY_3""")