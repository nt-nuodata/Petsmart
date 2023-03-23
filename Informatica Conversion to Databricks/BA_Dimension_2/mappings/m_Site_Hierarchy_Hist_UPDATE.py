# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SITE_HIERARCHY_HIST_0


df_0=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        SITE_HIERARCHY_EFF_DT AS SITE_HIERARCHY_EFF_DT,
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        SITE_HIERARCHY_END_DT AS SITE_HIERARCHY_END_DT,
        CURRENT_SITE_CD AS CURRENT_SITE_CD,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_HIERARCHY_HIST""")

df_0.createOrReplaceTempView("SITE_HIERARCHY_HIST_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_HIERARCHY_HIST_1


df_1=spark.sql("""
    SELECT
        LOCATION_ID,
        SITE_HIERARCHY_EFF_DT,
        DISTRICT_ID,
        DISTRICT_DESC,
        REGION_ID,
        REGION_DESC,
        SITE_HIERARCHY_END_DT,
        CURRENT_SITE_CD,
        UPDATE_TSTMP,
        LOAD_TSTMP 
    FROM
        (SELECT
            LOCATION_ID,
            SITE_HIERARCHY_EFF_DT,
            DISTRICT_ID,
            DISTRICT_DESC,
            REGION_ID,
            REGION_DESC,
            SITE_HIERARCHY_END_DT,
            CURRENT_SITE_CD,
            UPDATE_TSTMP,
            LOAD_TSTMP,
            RANK() OVER (PARTITION 
        BY
            LOCATION_ID 
        ORDER BY
            LOAD_TSTMP DESC) AS RANK 
        FROM
            SITE_HIERARCHY_HIST) a 
    WHERE
        RANK = 2 
        AND current_site_cd = 'Y'""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SITE_HIERARCHY_HIST_1")

# COMMAND ----------
# DBTITLE 1, EXP_DATES_2


df_2=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        SITE_HIERARCHY_EFF_DT AS SITE_HIERARCHY_EFF_DT,
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        SITE_HIERARCHY_END_DT AS SITE_HIERARCHY_END_DT,
        CURRENT_SITE_CD AS CURRENT_SITE_CD,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        ADD_TO_DATE(current_timestamp,
        'DD',
        -1) AS o_SITE_DM_END_DT,
        'N' AS CURRENT_DIST_MGR_CD,
        current_timestamp AS O_UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SITE_HIERARCHY_HIST_1""")

df_2.createOrReplaceTempView("EXP_DATES_2")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_UPDATE_3


df_3=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        SITE_HIERARCHY_EFF_DT AS SITE_HIERARCHY_EFF_DT,
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        o_SITE_DM_END_DT AS o_SITE_DM_END_DT,
        CURRENT_DIST_MGR_CD AS CURRENT_DIST_MGR_CD,
        O_UPDATE_TSTMP AS O_UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DATES_2""")

df_3.createOrReplaceTempView("UPDTRANS_UPDATE_3")

# COMMAND ----------
# DBTITLE 1, SITE_HIERARCHY_HIST


spark.sql("""INSERT INTO SITE_HIERARCHY_HIST SELECT NEW_LOCATION_ID AS LOCATION_ID,
SITE_HIERARCHY_EFF_DT AS SITE_HIERARCHY_EFF_DT,
NEW_DISTRICT_ID AS DISTRICT_ID,
NEW_DISTRICT_DESC AS DISTRICT_DESC,
NEW_REGION_ID AS REGION_ID,
NEW_REGION_DESC AS REGION_DESC,
SITE_HIERARCHY_END_DT AS SITE_HIERARCHY_END_DT,
CURRENT_SITE_CD AS CURRENT_SITE_CD,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPDTRANS_UPDATE_3""")