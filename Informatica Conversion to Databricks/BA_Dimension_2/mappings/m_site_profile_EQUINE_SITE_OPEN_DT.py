# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, POG_STORE_PRO_0

df_0=spark.sql("""
    SELECT
        POG_ID AS POG_ID,
        LOCATION_ID AS LOCATION_ID,
        POG_STATUS AS POG_STATUS,
        LAST_CHNG_DT AS LAST_CHNG_DT,
        POG_CANADIAN AS POG_CANADIAN,
        POG_EQUINE AS POG_EQUINE,
        POG_REGISTER AS POG_REGISTER,
        POG_GENERIC_MAP AS POG_GENERIC_MAP,
        DATE_POG_ADDED AS DATE_POG_ADDED,
        DATE_POG_REFRESHED AS DATE_POG_REFRESHED,
        DATE_POG_DELETED AS DATE_POG_DELETED,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        POG_STORE_PRO""")

df_0.createOrReplaceTempView("POG_STORE_PRO_0")

# COMMAND ----------

# DBTITLE 1, PLANOGRAM_PRO_1

df_1=spark.sql("""
    SELECT
        POG_ID AS POG_ID,
        POG_DBKEY AS POG_DBKEY,
        PROJECT_ID AS PROJECT_ID,
        FIXTURE_TYPE AS FIXTURE_TYPE,
        POG_NBR AS POG_NBR,
        PROJECT_NAME AS PROJECT_NAME,
        POG_DESC AS POG_DESC,
        POG_IMPLEMENT_DT AS POG_IMPLEMENT_DT,
        POG_REVIEW_YM AS POG_REVIEW_YM,
        POG_REV_DT AS POG_REV_DT,
        FIX_LINEAR_IN AS FIX_LINEAR_IN,
        FIX_CUBIC_IN AS FIX_CUBIC_IN,
        FIX_FLOOR_FT AS FIX_FLOOR_FT,
        MIX_FLAG AS MIX_FLAG,
        US_FLAG AS US_FLAG,
        CA_FLAG AS CA_FLAG,
        POG_STATUS AS POG_STATUS,
        LAST_CHNG_DT AS LAST_CHNG_DT,
        LISTING_FLAG AS LISTING_FLAG,
        EVENT_ID AS EVENT_ID,
        POG_DISPLAY_FLAG AS POG_DISPLAY_FLAG,
        EFF_START_DT AS EFF_START_DT,
        EFF_END_DT AS EFF_END_DT,
        POG_TYPE_CD AS POG_TYPE_CD,
        POG_DIVISION AS POG_DIVISION,
        POG_DEPT AS POG_DEPT,
        DATE_POG_ADDED AS DATE_POG_ADDED,
        DATE_POG_REFRESHED AS DATE_POG_REFRESHED,
        DATE_POG_DELETED AS DATE_POG_DELETED,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PLANOGRAM_PRO""")

df_1.createOrReplaceTempView("PLANOGRAM_PRO_1")

# COMMAND ----------

# DBTITLE 1, SITE_PROFILE_2

df_2=spark.sql("""
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

df_2.createOrReplaceTempView("SITE_PROFILE_2")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_to_SITE_PROFILE_3

df_3=spark.sql("""
    SELECT
        DISTINCT PSP.LOCATION_ID,
        CURRENT_DATE - 1 
    FROM
        PLANOGRAM_PRO PP,
        POG_STORE_PRO PSP,
        SITE_PROFILE SP 
    WHERE
        PP.POG_ID = PSP.POG_ID 
        AND PSP.LOCATION_ID = SP.LOCATION_ID 
        AND (
            PP.POG_NBR LIKE 'EBM%' 
            OR PP.POG_NBR = 'ESLT'
        ) 
        AND PSP.POG_STATUS <> 'D' 
        AND SP.EQUINE_SITE_OPEN_DT IS NULL""")

df_3.createOrReplaceTempView("ASQ_Shortcut_to_SITE_PROFILE_3")

# COMMAND ----------

# DBTITLE 1, SITE_PROFILE_RPT

spark.sql("""INSERT INTO SITE_PROFILE_RPT SELECT LOCATION_ID AS LOCATION_ID,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
o_LOCATION_TYPE_DESC AS LOCATION_TYPE_DESC,
STORE_NBR1 AS STORE_NBR,
o_STORE_NAME AS STORE_NAME,
STORE_TYPE_ID AS STORE_TYPE_ID,
o_STORE_TYPE_DESC AS STORE_TYPE_DESC,
PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
LOCATION_NBR AS LOCATION_NBR,
COMPANY_ID AS COMPANY_ID,
o_COMPANY_DESC AS COMPANY_DESC,
SUPER_REGION_ID AS SUPER_REGION_ID,
SUPER_REGION_DESC AS SUPER_REGION_DESC,
REGION_ID AS REGION_ID,
o_REGION_DESC AS REGION_DESC,
DISTRICT_ID AS DISTRICT_ID,
o_DISTRICT_DESC AS DISTRICT_DESC,
SITE_ADDRESS AS SITE_ADDRESS,
o_SITE_CITY AS SITE_CITY,
o_SITE_COUNTY AS SITE_COUNTY,
STATE_CD AS STATE_CD,
o_STATE_NAME AS STATE_NAME,
POSTAL_CD AS POSTAL_CD,
COUNTRY_CD AS COUNTRY_CD,
o_COUNTRY_NAME AS COUNTRY_NAME,
GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
PETSMART_DMA_CD AS PETSMART_DMA_CD,
o_PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
SITE_FAX_NO AS SITE_FAX_NO,
o_SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
SFT_OPEN_DT AS SFT_OPEN_DT,
OPEN_DT AS OPEN_DT,
GR_OPEN_DT AS GR_OPEN_DT,
CLOSE_DT AS CLOSE_DT,
SITE_SALES_FLAG AS SITE_SALES_FLAG,
SALES_CURR_FLAG AS SALES_CURR_FLAG,
SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
FIRST_SALE_DT AS FIRST_SALE_DT,
FIRST_MEASURED_SALE_DT AS FIRST_MEASURED_SALE_DT,
LAST_SALE_DT AS LAST_SALE_DT,
COMP_CURR_FLAG AS COMP_CURR_FLAG,
COMP_EFF_DT AS COMP_EFF_DT,
COMP_END_DT AS COMP_END_DT,
TP_LOC_FLAG AS TP_LOC_FLAG,
TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
TP_START_DT AS TP_START_DT,
HOTEL_FLAG AS HOTEL_FLAG,
HOTEL_OPEN_DT AS HOTEL_OPEN_DT,
DAYCAMP_FLAG AS DAYCAMP_FLAG,
VET_FLAG AS VET_FLAG,
o_TIME_ZONE_ID AS TIME_ZONE_ID,
o_TIME_ZONE AS TIME_ZONE,
SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
TRADE_AREA AS TRADE_AREA,
DELV_SERVICE_CLASS_ID AS DELV_SERVICE_CLASS_ID,
PICK_SERVICE_CLASS_ID AS PICK_SERVICE_CLASS_ID,
REPL_DC_NBR AS REPL_DC_NBR,
REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
PROMO_LABEL_CD AS PROMO_LABEL_CD,
PRICE_ZONE_ID AS PRICE_ZONE_ID,
o_PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
o_PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
o_EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC,
EQUINE_SITE_ID AS EQUINE_SITE_ID,
o_EQUINE_SITE_DESC AS EQUINE_SITE_DESC,
EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
o_LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
o_LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
BP_COMPANY_NBR AS BP_COMPANY_NBR,
BP_GL_ACCT AS BP_GL_ACCT,
SITE_LOGIN_ID AS SITE_LOGIN_ID,
o_SITE_MANAGER_ID AS SITE_MANAGER_ID,
SITE_MANAGER_NAME AS SITE_MANAGER_NAME,
MGR_ID AS MGR_ID,
o_MGR_DESC AS MGR_DESC,
DVL_ID AS DVL_ID,
o_DVL_DESC AS DVL_DESC,
PURCH_GROUP_ID AS PURCH_GROUP_ID,
PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
TOTAL_SALES_RANKING_CD AS TOTAL_SALES_RANKING_CD,
MERCH_SALES_RANKING_CD AS MERCH_SALES_RANKING_CD,
SERVICES_SALES_RANKING_CD AS SERVICES_SALES_RANKING_CD,
SALON_SALES_RANKING_CD AS SALON_SALES_RANKING_CD,
TRAINING_SALES_RANKING_CD AS TRAINING_SALES_RANKING_CD,
HOTEL_DDC_SALES_RANKING_CD AS HOTEL_DDC_SALES_RANKING_CD,
CONSUMABLES_SALES_RANKING_CD AS CONSUMABLES_SALES_RANKING_CD,
HARDGOODS_SALES_RANKING_CD AS HARDGOODS_SALES_RANKING_CD,
SPECIALTY_SALES_RANKING_CD AS SPECIALTY_SALES_RANKING_CD,
DIST_MGR_NAME AS DIST_MGR_NAME,
DM_EMAIL_ADDRESS AS DM_EMAIL_ADDRESS,
AREA_DIRECTOR_NAME AS DC_AREA_DIRECTOR_NAME,
AREA_DIRECTOR_EMAIL AS DC_AREA_DIRECTOR_EMAIL,
DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
REGION_VP_NAME AS REGION_VP_NAME,
RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
SAFETY_DIRECTOR_NAME AS LP_SAFETY_DIRECTOR_NAME,
SAFETY_DIRECTOR_EMAIL AS LP_SAFETY_DIRECTOR_EMAIL,
SR_LP_SAFETY_MGR_NAME AS SR_LP_SAFETY_MGR_NAME,
SR_LP_SAFETY_MGR_EMAIL AS SR_LP_SAFETY_MGR_EMAIL,
o_REGIONAL_LP_SAFETY_MGR_NAME AS REGIONAL_LP_SAFETY_MGR_NAME,
o_REGIONAL_LP_SAFETY_MGR_EMAIL AS REGIONAL_LP_SAFETY_MGR_EMAIL,
RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
PEOPLE_MANAGER_NAME AS HR_MANAGER_NAME,
PEOPLE_MANAGER_EMAIL AS HR_MANAGER_EMAIL,
HR_SUPERVISOR_NAME1 AS HR_SUPERVISOR_NAME1,
HR_SUPERVISOR_EMAIL1 AS HR_SUPERVISOR_EMAIL1,
HR_SUPERVISOR_NAME2 AS HR_SUPERVISOR_NAME2,
HR_SUPERVISOR_EMAIL2 AS HR_SUPERVISOR_EMAIL2,
LEARN_SOLUTION_MGR_NAME AS LEARN_SOLUTION_MGR_NAME,
LEARN_SOLUTION_MGR_EMAIL AS LEARN_SOLUTION_MGR_EMAIL,
ADD_DT AS ADD_DT,
DELETE_DT AS DELETE_DT,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM ASQ_Shortcut_to_SITE_PROFILE_3""")

# COMMAND ----------

# DBTITLE 1, SITE_PROFILE

spark.sql("""INSERT INTO SITE_PROFILE SELECT LOCATION_ID AS LOCATION_ID,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
STORE_NBR AS STORE_NBR,
VENDOR_NAME AS STORE_NAME,
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
ADDRESS AS SITE_ADDRESS,
CITY AS SITE_CITY,
STATE AS STATE_CD,
COUNTRY_CD AS COUNTRY_CD,
ZIP AS POSTAL_CD,
PHONE AS SITE_MAIN_TELE_NO,
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
PARENT_VENDOR_ID AS PARENT_LOCATION_ID,
VENDOR_NBR AS LOCATION_NBR,
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
LOAD_DT AS LOAD_DT FROM ASQ_Shortcut_to_SITE_PROFILE_3""")
