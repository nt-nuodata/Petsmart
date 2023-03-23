# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, USR_STORE_ATTRIBUTES_0


df_0=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
        VET_TYPE_ID AS VET_TYPE_ID,
        MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
        ONP_DIST_FLAG AS ONP_DIST_FLAG,
        PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
        HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
        HOTEL_TIER_ID AS HOTEL_TIER_ID,
        STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
        STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
        FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
        STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
        MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
        NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
        MARKET_LEADER_ID AS MARKET_LEADER_ID,
        MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        USR_STORE_ATTRIBUTES""")

df_0.createOrReplaceTempView("USR_STORE_ATTRIBUTES_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1


df_1=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
        VET_TYPE_ID AS VET_TYPE_ID,
        MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
        ONP_DIST_FLAG AS ONP_DIST_FLAG,
        PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
        HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
        HOTEL_TIER_ID AS HOTEL_TIER_ID,
        STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
        STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
        FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
        STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
        MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
        NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
        MARKET_LEADER_ID AS MARKET_LEADER_ID,
        MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_STORE_ATTRIBUTES_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1")

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
# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_3


df_3=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        STORE_NBR AS STORE_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_3")

# COMMAND ----------
# DBTITLE 1, JNR_SITE_PROFILE_4


df_4=spark.sql("""
    SELECT
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.STORE_NBR AS STORE_NBR,
        MASTER.BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
        MASTER.VET_TYPE_ID AS VET_TYPE_ID,
        MASTER.MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
        MASTER.ONP_DIST_FLAG AS ONP_DIST_FLAG,
        MASTER.PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
        MASTER.HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
        MASTER.HOTEL_TIER_ID AS HOTEL_TIER_ID,
        MASTER.STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
        MASTER.STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
        MASTER.FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
        MASTER.STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
        MASTER.MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
        MASTER.NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
        MASTER.MARKET_LEADER_ID AS MARKET_LEADER_ID,
        MASTER.MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_SITE_PROFILE_3 DETAIL 
            ON STORE_NBR1 = DETAIL.STORE_NBR""")

df_4.createOrReplaceTempView("JNR_SITE_PROFILE_4")

# COMMAND ----------
# DBTITLE 1, EXP_DEFAULT_VALUES_5


df_5=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        STORE_NBR AS STORE_NBR,
        IFF(ISNULL(BOUTIQUE_FLAG),
        0,
        BOUTIQUE_FLAG) AS o_BOUTIQUE_FLAG,
        IFF(ISNULL(VET_TYPE_ID),
        0,
        VET_TYPE_ID) AS o_VET_TYPE_ID,
        IFF(ISNULL(MICRO_HOTEL_FLAG),
        0,
        MICRO_HOTEL_FLAG) AS o_MICRO_HOTEL_FLAG,
        IFF(ISNULL(ONP_DIST_FLAG),
        0,
        ONP_DIST_FLAG) AS o_ONP_DIST_FLAG,
        IFF(ISNULL(PET_TRAINING_FLAG),
        0,
        PET_TRAINING_FLAG) AS o_PET_TRAINING_FLAG,
        IFF(ISNULL(HOTEL_PROTOTYPE_FLAG),
        0,
        HOTEL_PROTOTYPE_FLAG) AS o_HOTEL_PROTOTYPE_FLAG,
        IFF(ISNULL(HOTEL_TIER_ID),
        '0',
        HOTEL_TIER_ID) AS o_HOTEL_TIER_ID,
        IFF(ISNULL(STORE_CENTER_TYPE_ID),
        0,
        STORE_CENTER_TYPE_ID) AS o_STORE_CENTER_TYPE_ID,
        IFF(ISNULL(STORE_SIZE_TYPE_ID),
        0,
        STORE_SIZE_TYPE_ID) AS o_STORE_SIZE_TYPE_ID,
        IFF(ISNULL(FISH_SYSTEM_TYPE_ID),
        0,
        FISH_SYSTEM_TYPE_ID) AS o_FISH_SYSTEM_TYPE_ID,
        IFF(ISNULL(STORE_PROGRAM_ID),
        '0',
        STORE_PROGRAM_ID) AS o_STORE_PROGRAM_ID,
        IFF(ISNULL(MICRO_SALON_FLAG),
        0,
        MICRO_SALON_FLAG) AS o_MICRO_SALON_FLAG,
        IFF(ISNULL(NO_FORKLIFT_FLAG),
        0,
        NO_FORKLIFT_FLAG) AS o_NO_FORKLIFT_FLAG,
        IFF(ISNULL(MARKET_LEADER_ID),
        0,
        MARKET_LEADER_ID) AS o_MARKET_LEADER_ID,
        IFF(ISNULL(MARKET_LEADER_DESC),
        'N/A',
        MARKET_LEADER_DESC) AS o_MARKET_LEADER_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_SITE_PROFILE_4""")

df_5.createOrReplaceTempView("EXP_DEFAULT_VALUES_5")

# COMMAND ----------
# DBTITLE 1, LKP_VET_TYPE_6


df_6=spark.sql("""
    SELECT
        VET_TYPE_DESC AS VET_TYPE_DESC,
        EXP_DEFAULT_VALUES_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_VET_TYPE_LKUP 
    RIGHT OUTER JOIN
        EXP_DEFAULT_VALUES_5 
            ON USR_VET_TYPE_LKUP.VET_TYPE_ID = EXP_DEFAULT_VALUES_5.o_VET_TYPE_ID""")

df_6.createOrReplaceTempView("LKP_VET_TYPE_6")

# COMMAND ----------
# DBTITLE 1, LKP_FISH_SYSTEM_TYPE_7


df_7=spark.sql("""
    SELECT
        NEW_SYSTEM_TYPE AS NEW_SYSTEM_TYPE,
        EXP_DEFAULT_VALUES_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_SYSTEM_TYPES 
    RIGHT OUTER JOIN
        EXP_DEFAULT_VALUES_5 
            ON USR_SYSTEM_TYPES.SYSTEM_TYPE_ID = EXP_DEFAULT_VALUES_5.o_FISH_SYSTEM_TYPE_ID""")

df_7.createOrReplaceTempView("LKP_FISH_SYSTEM_TYPE_7")

# COMMAND ----------
# DBTITLE 1, LKP_STORE_SIZE_TYPE_8


df_8=spark.sql("""
    SELECT
        STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
        EXP_DEFAULT_VALUES_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_STORE_SIZE_TYPE_LKUP 
    RIGHT OUTER JOIN
        EXP_DEFAULT_VALUES_5 
            ON USR_STORE_SIZE_TYPE_LKUP.STORE_SIZE_TYPE_ID = EXP_DEFAULT_VALUES_5.o_STORE_SIZE_TYPE_ID""")

df_8.createOrReplaceTempView("LKP_STORE_SIZE_TYPE_8")

# COMMAND ----------
# DBTITLE 1, LKP_STORE_CENTER_TYPE_9


df_9=spark.sql("""
    SELECT
        STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
        EXP_DEFAULT_VALUES_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_STORE_CENTER_TYPE_LKUP 
    RIGHT OUTER JOIN
        EXP_DEFAULT_VALUES_5 
            ON USR_STORE_CENTER_TYPE_LKUP.STORE_CENTER_TYPE_ID = EXP_DEFAULT_VALUES_5.o_STORE_CENTER_TYPE_ID""")

df_9.createOrReplaceTempView("LKP_STORE_CENTER_TYPE_9")

# COMMAND ----------
# DBTITLE 1, EXP_GROOMERY_FLAG_RULE_10


df_10=spark.sql("""
    SELECT
        EXP_DEFAULT_VALUES_5.LOCATION_ID AS LOCATION_ID,
        EXP_DEFAULT_VALUES_5.STORE_NBR AS STORE_NBR,
        EXP_DEFAULT_VALUES_5.o_BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
        EXP_DEFAULT_VALUES_5.o_VET_TYPE_ID AS VET_TYPE_ID,
        LKP_VET_TYPE_6.VET_TYPE_DESC AS VET_TYPE_DESC,
        EXP_DEFAULT_VALUES_5.o_MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
        EXP_DEFAULT_VALUES_5.o_ONP_DIST_FLAG AS ONP_DIST_FLAG,
        EXP_DEFAULT_VALUES_5.o_PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
        EXP_DEFAULT_VALUES_5.o_HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
        EXP_DEFAULT_VALUES_5.o_HOTEL_TIER_ID AS HOTEL_TIER_ID,
        EXP_DEFAULT_VALUES_5.o_STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
        LKP_STORE_CENTER_TYPE_9.STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
        EXP_DEFAULT_VALUES_5.o_STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
        LKP_STORE_SIZE_TYPE_8.STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
        EXP_DEFAULT_VALUES_5.o_FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
        LKP_FISH_SYSTEM_TYPE_7.NEW_SYSTEM_TYPE AS FISH_SYSTEM_TYPE_DESC,
        IFF(EXP_DEFAULT_VALUES_5.STORE_NBR >= 2600 
        AND EXP_DEFAULT_VALUES_5.STORE_NBR <= 2699,
        1,
        0) AS GROOMERY_FLAG,
        EXP_DEFAULT_VALUES_5.o_STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
        EXP_DEFAULT_VALUES_5.o_MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
        EXP_DEFAULT_VALUES_5.o_NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
        EXP_DEFAULT_VALUES_5.o_MARKET_LEADER_ID AS o_MARKET_LEADER_ID,
        EXP_DEFAULT_VALUES_5.o_MARKET_LEADER_DESC AS o_MARKET_LEADER_DESC,
        LKP_STORE_CENTER_TYPE_9.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_STORE_CENTER_TYPE_9 
    INNER JOIN
        LKP_STORE_SIZE_TYPE_8 
            ON LKP_STORE_CENTER_TYPE_9.Monotonically_Increasing_Id = LKP_STORE_SIZE_TYPE_8.Monotonically_Increasing_Id 
    INNER JOIN
        LKP_FISH_SYSTEM_TYPE_7 
            ON LKP_STORE_CENTER_TYPE_9.Monotonically_Increasing_Id = LKP_FISH_SYSTEM_TYPE_7.Monotonically_Increasing_Id 
    INNER JOIN
        LKP_VET_TYPE_6 
            ON LKP_STORE_CENTER_TYPE_9.Monotonically_Increasing_Id = LKP_VET_TYPE_6.Monotonically_Increasing_Id 
    INNER JOIN
        EXP_DEFAULT_VALUES_5 
            ON LKP_STORE_CENTER_TYPE_9.Monotonically_Increasing_Id = EXP_DEFAULT_VALUES_5.Monotonically_Increasing_Id""")

df_10.createOrReplaceTempView("EXP_GROOMERY_FLAG_RULE_10")

# COMMAND ----------
# DBTITLE 1, EXP_DEFAULT_DESC_11


df_11=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        STORE_NBR AS STORE_NBR,
        BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
        VET_TYPE_ID AS VET_TYPE_ID,
        IFF(ISNULL(VET_TYPE_DESC),
        '',
        VET_TYPE_DESC) AS o_VET_TYPE_DESC,
        MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
        ONP_DIST_FLAG AS ONP_DIST_FLAG,
        PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
        HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
        HOTEL_TIER_ID AS HOTEL_TIER_ID,
        STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
        IFF(ISNULL(STORE_CENTER_TYPE_DESC),
        '',
        STORE_CENTER_TYPE_DESC) AS o_STORE_CENTER_TYPE_DESC,
        STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
        IFF(ISNULL(STORE_SIZE_TYPE_DESC),
        '',
        STORE_SIZE_TYPE_DESC) AS o_STORE_SIZE_TYPE_DESC,
        FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
        IFF(ISNULL(FISH_SYSTEM_TYPE_DESC),
        'N/A',
        FISH_SYSTEM_TYPE_DESC) AS o_FISH_SYSTEM_TYPE_DESC,
        IFF(ISNULL(GROOMERY_FLAG),
        0,
        GROOMERY_FLAG) AS o_GROOMERY_FLAG,
        STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
        MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
        NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
        current_timestamp AS LOAD_TSTMP,
        o_MARKET_LEADER_ID AS o_MARKET_LEADER_ID,
        o_MARKET_LEADER_DESC AS o_MARKET_LEADER_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_GROOMERY_FLAG_RULE_10""")

df_11.createOrReplaceTempView("EXP_DEFAULT_DESC_11")

# COMMAND ----------
# DBTITLE 1, USR_STORE_ATTRIBUTES_PRE


spark.sql("""INSERT INTO USR_STORE_ATTRIBUTES_PRE SELECT LOCATION_ID AS LOCATION_ID,
STORE_NBR AS STORE_NBR,
BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
VET_TYPE_ID AS VET_TYPE_ID,
o_VET_TYPE_DESC AS VET_TYPE_DESC,
MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
ONP_DIST_FLAG AS ONP_DIST_FLAG,
PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
HOTEL_TIER_ID AS HOTEL_TIER_ID,
STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
o_STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
o_STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
o_FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
o_GROOMERY_FLAG AS GROOMERY_FLAG,
STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
o_MARKET_LEADER_ID AS MARKET_LEADER_ID,
o_MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
LOAD_TSTMP AS LOAD_TSTMP FROM EXP_DEFAULT_DESC_11""")