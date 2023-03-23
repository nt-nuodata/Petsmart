# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, LOCATION_0

df_0=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        COMPANY_DESC AS COMPANY_DESC,
        COMPANY_ID AS COMPANY_ID,
        DATE_CLOSED AS DATE_CLOSED,
        DATE_OPEN AS DATE_OPEN,
        DATE_LOC_ADDED AS DATE_LOC_ADDED,
        DATE_LOC_DELETED AS DATE_LOC_DELETED,
        DATE_LOC_REFRESHED AS DATE_LOC_REFRESHED,
        DISTRICT_DESC AS DISTRICT_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        REGION_DESC AS REGION_DESC,
        REGION_ID AS REGION_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        STORE_CTRY AS STORE_CTRY,
        STORE_NAME AS STORE_NAME,
        STORE_NBR AS STORE_NBR,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        STORE_STATE_ABBR AS STORE_STATE_ABBR,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        EQUINE_MERCH AS EQUINE_MERCH,
        DATE_GR_OPEN AS DATE_GR_OPEN,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        TP_START_DT AS TP_START_DT,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        SITE_POSTAL_CD AS SITE_POSTAL_CD,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LOCATION""")

df_0.createOrReplaceTempView("LOCATION_0")

# COMMAND ----------

# DBTITLE 1, SITE_PROFILE_1

df_1=spark.sql("""
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

df_1.createOrReplaceTempView("SITE_PROFILE_1")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_to_SITE_PROFILE_2

df_2=spark.sql("""
    SELECT
        SITE.LOCATION_ID AS LOCATION_ID,
        L.LOCATION_ID AS LOC_LOCATION_ID,
        C.COMPANY_DESC AS COMPANY_DESC,
        SITE.COMPANY_ID AS COMPANY_ID,
        SITE.CLOSE_DT AS CLOSE_DT,
        SITE.OPEN_DT AS OPEN_DT,
        SITE.ADD_DT AS ADD_DT,
        SITE.DELETE_DT AS DELETE_DT,
        SITE.LOAD_DT AS LOAD_DT,
        D.DISTRICT_DESC AS DISTRICT_DESC,
        SITE.DISTRICT_ID AS DISTRICT_ID,
        PA.PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        SITE.PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        P.PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        SITE.PRICE_ZONE_ID AS PRICE_ZONE_ID,
        R.REGION_DESC AS REGION_DESC,
        SITE.REGION_ID AS REGION_ID,
        SITE.REPL_DC_NBR AS REPL_DC_NBR,
        SITE.REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        SITE.REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        SITE.COUNTRY_CD AS COUNTRY_CD,
        CT.COUNTRY_NAME AS COUNTRY_NAME,
        SITE.STORE_NAME AS STORE_NAME,
        SITE.STORE_NBR AS STORE_NBR,
        SITE.STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        SITE.STATE_CD AS STATE_CD,
        ST.STORE_TYPE_DESC AS STORE_TYPE_DESC,
        SITE.STORE_TYPE_ID AS STORE_TYPE_ID,
        EM.EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC,
        SITE.GR_OPEN_DT AS GR_OPEN_DT,
        SITE.SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SITE.SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        SITE.BP_COMPANY_NBR AS BP_COMPANY_NBR,
        SITE.BP_GL_ACCT AS BP_GL_ACCT,
        SITE.TP_LOC_FLAG AS TP_LOC_FLAG,
        SITE.TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        SITE.TP_START_DT AS TP_START_DT,
        SITE.SITE_ADDRESS AS SITE_ADDRESS,
        SITE.SITE_CITY AS SITE_CITY,
        SITE.POSTAL_CD AS POSTAL_CD,
        SITE.SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE.SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO 
    FROM
        SITE_PROFILE SITE 
    LEFT OUTER JOIN
        LOCATION L 
            ON SITE.LOCATION_ID = L.LOCATION_ID 
    LEFT OUTER JOIN
        STORE_TYPE ST 
            ON SITE.STORE_TYPE_ID = ST.STORE_TYPE_ID 
    LEFT OUTER JOIN
        COMPANY C 
            ON SITE.COMPANY_ID = C.COMPANY_ID 
    LEFT OUTER JOIN
        REGION R 
            ON SITE.REGION_ID = R.REGION_ID 
    LEFT OUTER JOIN
        DISTRICT D 
            ON SITE.DISTRICT_ID = D.DISTRICT_ID 
    LEFT OUTER JOIN
        PRICE_ZONE P 
            ON SITE.PRICE_ZONE_ID = P.PRICE_ZONE_ID 
    LEFT OUTER JOIN
        PRICE_AD_ZONE PA 
            ON SITE.PRICE_AD_ZONE_ID = PA.PRICE_AD_ZONE_ID 
    LEFT OUTER JOIN
        COUNTRY CT 
            ON SITE.COUNTRY_CD = CT.COUNTRY_CD 
    LEFT OUTER JOIN
        EQUINE_MERCH EM 
            ON SITE.EQUINE_MERCH_ID = EM.EQUINE_MERCH_ID 
    WHERE
        SITE.LOAD_DT = CURRENT_DATE""")

df_2.createOrReplaceTempView("ASQ_Shortcut_to_SITE_PROFILE_2")

# COMMAND ----------

# DBTITLE 1, UPD_LOCATION_3

df_3=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOC_LOCATION_ID AS LOC_LOCATION_ID,
        COMPANY_DESC AS COMPANY_DESC,
        COMPANY_ID AS COMPANY_ID,
        CLOSE_DT AS DATE_CLOSED,
        OPEN_DT AS DATE_OPEN,
        ADD_DT AS DATE_LOC_ADDED,
        DELETE_DT AS DATE_LOC_DELETED,
        LOAD_DT AS DATE_LOC_REFRESHED,
        DISTRICT_DESC AS DISTRICT_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        REGION_DESC AS REGION_DESC,
        REGION_ID AS REGION_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        COUNTRY_CD AS STORE_CTRY_ABBR,
        COUNTRY_NAME AS STORE_CTRY,
        STORE_NAME AS STORE_NAME,
        STORE_NBR AS STORE_NBR,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        STATE_CD AS STORE_STATE_ABBR,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        EQUINE_MERCH_DESC AS EQUINE_MERCH,
        GR_OPEN_DT AS DATE_GR_OPEN,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        TP_START_DT AS TP_START_DT,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        POSTAL_CD AS SITE_POSTAL_CD,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_to_SITE_PROFILE_2""")

df_3.createOrReplaceTempView("UPD_LOCATION_3")

# COMMAND ----------

# DBTITLE 1, LOCATION

spark.sql("""INSERT INTO LOCATION SELECT LOCATION_ID AS LOCATION_ID,
COMPANY_DESC AS COMPANY_DESC,
COMPANY_ID AS COMPANY_ID,
DATE_CLOSED AS DATE_CLOSED,
DATE_OPEN AS DATE_OPEN,
DATE_LOC_ADDED AS DATE_LOC_ADDED,
DATE_LOC_DELETED AS DATE_LOC_DELETED,
DATE_LOC_REFRESHED AS DATE_LOC_REFRESHED,
DISTRICT_DESC AS DISTRICT_DESC,
DISTRICT_ID AS DISTRICT_ID,
PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
PRICE_ZONE_ID AS PRICE_ZONE_ID,
REGION_DESC AS REGION_DESC,
REGION_ID AS REGION_ID,
REPL_DC_NBR AS REPL_DC_NBR,
REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
STORE_CTRY AS STORE_CTRY,
STORE_NAME AS STORE_NAME,
STORE_NBR AS STORE_NBR,
STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
STORE_STATE_ABBR AS STORE_STATE_ABBR,
STORE_TYPE_DESC AS STORE_TYPE_DESC,
STORE_TYPE_ID AS STORE_TYPE_ID,
EQUINE_MERCH AS EQUINE_MERCH,
DATE_GR_OPEN AS DATE_GR_OPEN,
SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
BP_COMPANY_NBR AS BP_COMPANY_NBR,
BP_GL_ACCT AS BP_GL_ACCT,
TP_LOC_FLAG AS TP_LOC_FLAG,
TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
TP_START_DT AS TP_START_DT,
SITE_ADDRESS AS SITE_ADDRESS,
SITE_CITY AS SITE_CITY,
SITE_POSTAL_CD AS SITE_POSTAL_CD,
SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO FROM UPD_LOCATION_3""")
