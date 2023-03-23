# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, TimeZone_0

df_0=spark.sql("""
    SELECT
        TimeZoneId AS TimeZoneId,
        TimeZoneDesc AS TimeZoneDesc,
        UpdateTstmp AS UpdateTstmp,
        LoadTstmp AS LoadTstmp,
        TimeZoneTlmsId AS TimeZoneTlmsId,
        TimeZoneTlmsName AS TimeZoneTlmsName,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TimeZone""")

df_0.createOrReplaceTempView("TimeZone_0")

# COMMAND ----------

# DBTITLE 1, PetSmartFacility_1

df_1=spark.sql("""
    SELECT
        FacilityGid AS FacilityGid,
        FacilityTypeId AS FacilityTypeId,
        FacilityNbr AS FacilityNbr,
        FacilityName AS FacilityName,
        FacilityDesc AS FacilityDesc,
        CompanyCode AS CompanyCode,
        TimeZoneId AS TimeZoneId,
        SrcCreateTstmp AS SrcCreateTstmp,
        UpdateTstmp AS UpdateTstmp,
        LoadTstmp AS LoadTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PetSmartFacility""")

df_1.createOrReplaceTempView("PetSmartFacility_1")

# COMMAND ----------

# DBTITLE 1, FacilityEDWXref_2

df_2=spark.sql("""
    SELECT
        FacilityGid AS FacilityGid,
        LocationID AS LocationID,
        LocationNbr AS LocationNbr,
        UpdateTstmp AS UpdateTstmp,
        LoadTstmp AS LoadTstmp,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        FacilityEDWXref""")

df_2.createOrReplaceTempView("FacilityEDWXref_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_PetSmartFacility_3

df_3=spark.sql("""
    SELECT
        LocationID AS LocationID,
        TimeZoneDesc AS TimeZoneDesc,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FacilityEDWXref_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_PetSmartFacility_3")

# COMMAND ----------

# DBTITLE 1, EXP_SITE_PROFILE_4

df_4=spark.sql("""
    SELECT
        LocationID AS LocationID,
        TimeZoneDesc AS TimeZoneDesc,
        current_timestamp AS Update_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_PetSmartFacility_3""")

df_4.createOrReplaceTempView("EXP_SITE_PROFILE_4")

# COMMAND ----------

# DBTITLE 1, UPD_SITE_PROFILE_5

df_5=spark.sql("""
    SELECT
        LocationID AS LocationID,
        TimeZoneDesc AS TimeZoneDesc,
        Update_DT AS Update_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_SITE_PROFILE_4""")

df_5.createOrReplaceTempView("UPD_SITE_PROFILE_5")

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
LOAD_DT AS LOAD_DT FROM UPD_SITE_PROFILE_5""")