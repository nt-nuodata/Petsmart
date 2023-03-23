# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, DISTRICT_0

df_0=spark.sql("""
    SELECT
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        DISTRICT_SITE_LOGIN_ID AS DISTRICT_SITE_LOGIN_ID,
        DISTRICT_SALON_LOGIN_ID AS DISTRICT_SALON_LOGIN_ID,
        DISTRICT_HOTEL_LOGIN_ID AS DISTRICT_HOTEL_LOGIN_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DISTRICT""")

df_0.createOrReplaceTempView("DISTRICT_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_DISTRICT_1

df_1=spark.sql("""
    SELECT
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        DISTRICT_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_DISTRICT_1")

# COMMAND ----------

# DBTITLE 1, REGION_2

df_2=spark.sql("""
    SELECT
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        REGION""")

df_2.createOrReplaceTempView("REGION_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_REGION_3

df_3=spark.sql("""
    SELECT
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        REGION_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_REGION_3")

# COMMAND ----------

# DBTITLE 1, SITE_PROFILE_4

df_4=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_NBR AS STORE_NBR,
        STORE_NAME AS STORE_NAME,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        COMPANY_ID AS COMPANY_ID,
        REGION_ID AS REGION_ID,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        STATE_CD AS STATE_CD,
        COUNTRY_CD AS COUNTRY_CD,
        POSTAL_CD AS POSTAL_CD,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
        SITE_SALES_FLAG AS SITE_SALES_FLAG,
        EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
        EQUINE_SITE_ID AS EQUINE_SITE_ID,
        EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
        GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
        GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        PROMO_LABEL_CD AS PROMO_LABEL_CD,
        PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
        LOCATION_NBR AS LOCATION_NBR,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        DELV_SERVICE_CLASS_ID AS DELV_SERVICE_CLASS_ID,
        PICK_SERVICE_CLASS_ID AS PICK_SERVICE_CLASS_ID,
        SITE_LOGIN_ID AS SITE_LOGIN_ID,
        SITE_MANAGER_ID AS SITE_MANAGER_ID,
        SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
        HOTEL_FLAG AS HOTEL_FLAG,
        DAYCAMP_FLAG AS DAYCAMP_FLAG,
        VET_FLAG AS VET_FLAG,
        DIST_MGR_NAME AS DIST_MGR_NAME,
        DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
        REGION_VP_NAME AS REGION_VP_NAME,
        REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
        ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
        SITE_COUNTY AS SITE_COUNTY,
        SITE_FAX_NO AS SITE_FAX_NO,
        SFT_OPEN_DT AS SFT_OPEN_DT,
        DM_EMAIL_ADDRESS AS DM_EMAIL_ADDRESS,
        DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
        RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
        TRADE_AREA AS TRADE_AREA,
        FDLPS_NAME AS FDLPS_NAME,
        FDLPS_EMAIL AS FDLPS_EMAIL,
        OVERSITE_MGR_NAME AS OVERSITE_MGR_NAME,
        OVERSITE_MGR_EMAIL AS OVERSITE_MGR_EMAIL,
        SAFETY_DIRECTOR_NAME AS SAFETY_DIRECTOR_NAME,
        SAFETY_DIRECTOR_EMAIL AS SAFETY_DIRECTOR_EMAIL,
        RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
        RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
        AREA_DIRECTOR_NAME AS AREA_DIRECTOR_NAME,
        AREA_DIRECTOR_EMAIL AS AREA_DIRECTOR_EMAIL,
        DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
        DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
        ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
        ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
        ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
        ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
        REGIONAL_DC_SAFETY_MGR_NAME AS REGIONAL_DC_SAFETY_MGR_NAME,
        REGIONAL_DC_SAFETY_MGR_EMAIL AS REGIONAL_DC_SAFETY_MGR_EMAIL,
        DC_PEOPLE_SUPERVISOR_NAME AS DC_PEOPLE_SUPERVISOR_NAME,
        DC_PEOPLE_SUPERVISOR_EMAIL AS DC_PEOPLE_SUPERVISOR_EMAIL,
        PEOPLE_MANAGER_NAME AS PEOPLE_MANAGER_NAME,
        PEOPLE_MANAGER_EMAIL AS PEOPLE_MANAGER_EMAIL,
        ASSET_PROT_DIR_NAME AS ASSET_PROT_DIR_NAME,
        ASSET_PROT_DIR_EMAIL AS ASSET_PROT_DIR_EMAIL,
        SR_REG_ASSET_PROT_MGR_NAME AS SR_REG_ASSET_PROT_MGR_NAME,
        SR_REG_ASSET_PROT_MGR_EMAIL AS SR_REG_ASSET_PROT_MGR_EMAIL,
        REG_ASSET_PROT_MGR_NAME AS REG_ASSET_PROT_MGR_NAME,
        REG_ASSET_PROT_MGR_EMAIL AS REG_ASSET_PROT_MGR_EMAIL,
        ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
        TP_START_DT AS TP_START_DT,
        OPEN_DT AS OPEN_DT,
        GR_OPEN_DT AS GR_OPEN_DT,
        CLOSE_DT AS CLOSE_DT,
        HOTEL_OPEN_DT AS HOTEL_OPEN_DT,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE""")

df_4.createOrReplaceTempView("SITE_PROFILE_4")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_5

df_5=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        REGION_ID AS REGION_ID,
        DISTRICT_ID AS DISTRICT_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE_4 
    WHERE
        location_type_id IN (
            8
        )""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_5")

# COMMAND ----------

# DBTITLE 1, JNRTRANS_6

df_6=spark.sql("""
    SELECT
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.REGION_ID AS REGION_ID,
        DETAIL.DISTRICT_ID AS DISTRICT_ID,
        MASTER.DISTRICT_DESC AS DISTRICT_DESC,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_DISTRICT_1 MASTER 
    INNER JOIN
        SQ_Shortcut_to_SITE_PROFILE_5 DETAIL 
            ON DISTRICT_ID1 = DETAIL.DISTRICT_ID""")

df_6.createOrReplaceTempView("JNRTRANS_6")

# COMMAND ----------

# DBTITLE 1, JNRTRANS1_7

df_7=spark.sql("""
    SELECT
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.DISTRICT_ID AS DISTRICT_ID,
        DETAIL.DISTRICT_DESC AS DISTRICT_DESC,
        MASTER.REGION_ID AS REGION_ID1,
        MASTER.REGION_DESC AS REGION_DESC,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_REGION_3 MASTER 
    INNER JOIN
        JNRTRANS_6 DETAIL 
            ON MASTER.REGION_ID = REGION_ID""")

df_7.createOrReplaceTempView("JNRTRANS1_7")

# COMMAND ----------

# DBTITLE 1, EXPTRANS_AFTER_JOINS_8

df_8=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        REGION_ID1 AS REGION_ID1,
        REGION_DESC AS REGION_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNRTRANS1_7""")

df_8.createOrReplaceTempView("EXPTRANS_AFTER_JOINS_8")

# COMMAND ----------

# DBTITLE 1, LKP_NZ_SITE_HIERARCHY_HIST_9

df_9=spark.sql("""
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
        EXPTRANS_AFTER_JOINS_8.LOCATION_ID AS NEW_LOCATION_ID,
        EXPTRANS_AFTER_JOINS_8.DISTRICT_ID AS NEW_DISTRICT_ID,
        EXPTRANS_AFTER_JOINS_8.DISTRICT_DESC AS NEW_DISTRICT_DESC,
        EXPTRANS_AFTER_JOINS_8.REGION_ID1 AS NEW_REGION_ID,
        EXPTRANS_AFTER_JOINS_8.REGION_DESC AS NEW_REGION_DESC,
        EXPTRANS_AFTER_JOINS_8.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_HIERARCHY_HIST 
    RIGHT OUTER JOIN
        EXPTRANS_AFTER_JOINS_8 
            ON LOCATION_ID = EXPTRANS_AFTER_JOINS_8.LOCATION_ID""")

df_9.createOrReplaceTempView("LKP_NZ_SITE_HIERARCHY_HIST_9")

# COMMAND ----------

# DBTITLE 1, EXP_DATES_STRATEGY_10

df_10=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        NEW_LOCATION_ID AS NEW_LOCATION_ID,
        NEW_DISTRICT_ID AS NEW_DISTRICT_ID,
        NEW_DISTRICT_DESC AS NEW_DISTRICT_DESC,
        NEW_REGION_ID AS NEW_REGION_ID,
        NEW_REGION_DESC AS NEW_REGION_DESC,
        IFF(ISNULL(LOCATION_ID) 
        OR NEW_DISTRICT_ID <> DISTRICT_ID 
        OR NEW_DISTRICT_DESC <> DISTRICT_DESC 
        OR NEW_REGION_ID <> REGION_ID 
        OR NEW_REGION_DESC <> REGION_DESC,
        'I',
        'R') AS STRATEGY,
        date_trunc('DAY',
        ADD_TO_DATE(current_timestamp,
        'DD',
        -1)) AS SITE_HIERARCHY_EFF_DT,
        TO_DATE('12/31/9999',
        'MM/DD/YYYY') AS SITE_HIERARCHY_END_DT,
        'Y' AS CURRENT_SITE_CD,
        current_timestamp AS UPDATE_TSTMP,
        current_timestamp AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_NZ_SITE_HIERARCHY_HIST_9""")

df_10.createOrReplaceTempView("EXP_DATES_STRATEGY_10")

# COMMAND ----------

# DBTITLE 1, FILT_EXISTING_RECORDS_11

df_11=spark.sql("""
    SELECT
        STRATEGY AS STRATEGY,
        NEW_LOCATION_ID AS NEW_LOCATION_ID,
        SITE_HIERARCHY_EFF_DT AS SITE_HIERARCHY_EFF_DT,
        NEW_DISTRICT_ID AS NEW_DISTRICT_ID,
        NEW_DISTRICT_DESC AS NEW_DISTRICT_DESC,
        NEW_REGION_ID AS NEW_REGION_ID,
        NEW_REGION_DESC AS NEW_REGION_DESC,
        SITE_HIERARCHY_END_DT AS SITE_HIERARCHY_END_DT,
        CURRENT_SITE_CD AS CURRENT_SITE_CD,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DATES_STRATEGY_10 
    WHERE
        STRATEGY <> 'R'""")

df_11.createOrReplaceTempView("FILT_EXISTING_RECORDS_11")

# COMMAND ----------

# DBTITLE 1, UPDTRANS_INSERT_REJECT_12

df_12=spark.sql("""
    SELECT
        STRATEGY AS STRATEGY,
        NEW_LOCATION_ID AS NEW_LOCATION_ID,
        SITE_HIERARCHY_EFF_DT AS SITE_HIERARCHY_EFF_DT,
        NEW_DISTRICT_ID AS NEW_DISTRICT_ID,
        NEW_DISTRICT_DESC AS NEW_DISTRICT_DESC,
        NEW_REGION_ID AS NEW_REGION_ID,
        NEW_REGION_DESC AS NEW_REGION_DESC,
        SITE_HIERARCHY_END_DT AS SITE_HIERARCHY_END_DT,
        CURRENT_SITE_CD AS CURRENT_SITE_CD,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FILT_EXISTING_RECORDS_11""")

df_12.createOrReplaceTempView("UPDTRANS_INSERT_REJECT_12")

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
LOAD_TSTMP AS LOAD_TSTMP FROM UPDTRANS_INSERT_REJECT_12""")
