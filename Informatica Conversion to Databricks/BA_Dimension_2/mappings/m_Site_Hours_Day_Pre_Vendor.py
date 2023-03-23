# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, DAYS_0


df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
        HOLIDAY_FLAG AS HOLIDAY_FLAG,
        DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
        DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
        DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
        CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
        CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
        CAL_WK AS CAL_WK,
        CAL_WK_NBR AS CAL_WK_NBR,
        CAL_MO AS CAL_MO,
        CAL_MO_NBR AS CAL_MO_NBR,
        CAL_MO_NAME AS CAL_MO_NAME,
        CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
        CAL_QTR AS CAL_QTR,
        CAL_QTR_NBR AS CAL_QTR_NBR,
        CAL_HALF AS CAL_HALF,
        CAL_YR AS CAL_YR,
        FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
        FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_WK_NBR AS FISCAL_WK_NBR,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        FISCAL_QTR AS FISCAL_QTR,
        FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
        FISCAL_HALF AS FISCAL_HALF,
        FISCAL_YR AS FISCAL_YR,
        LYR_WEEK_DT AS LYR_WEEK_DT,
        LWK_WEEK_DT AS LWK_WEEK_DT,
        WEEK_DT AS WEEK_DT,
        EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
        EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
        ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
        ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
        CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
        CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
        CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
        CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
        MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
        MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
        MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
        MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
        PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
        PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DAYS""")

df_0.createOrReplaceTempView("DAYS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DAYS_1


df_1=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        HOLIDAY_FLAG AS HOLIDAY_FLAG,
        DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        DAYS_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_DAYS_1")

# COMMAND ----------
# DBTITLE 1, Fil_Day_Dt_2


df_2=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
        HOLIDAY_FLAG AS HOLIDAY_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_DAYS_1 
    WHERE
        DAY_DT >= date_trunc('DAY', current_timestamp) 
        AND DAY_DT < ADD_TO_DATE(date_trunc('DAY', current_timestamp), 'D', 15)""")

df_2.createOrReplaceTempView("Fil_Day_Dt_2")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_3


df_3=spark.sql("""SELECT DAY_DT AS DAY_DT,
DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
1 AS Join,
HOLIDAY_FLAG AS HOLIDAY_FLAG,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM Fil_Day_Dt_2""")

df_3.createOrReplaceTempView("EXPTRANS_3")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE_RPT_4


df_4=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        LOCATION_TYPE_DESC AS LOCATION_TYPE_DESC,
        STORE_NBR AS STORE_NBR,
        STORE_NAME AS STORE_NAME,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
        LOCATION_NBR AS LOCATION_NBR,
        COMPANY_ID AS COMPANY_ID,
        COMPANY_DESC AS COMPANY_DESC,
        SUPER_REGION_ID AS SUPER_REGION_ID,
        SUPER_REGION_DESC AS SUPER_REGION_DESC,
        REGION_ID AS REGION_ID,
        REGION_DESC AS REGION_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        SITE_COUNTY AS SITE_COUNTY,
        STATE_CD AS STATE_CD,
        STATE_NAME AS STATE_NAME,
        POSTAL_CD AS POSTAL_CD,
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
        GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
        PETSMART_DMA_CD AS PETSMART_DMA_CD,
        PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        SITE_FAX_NO AS SITE_FAX_NO,
        SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
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
        TIME_ZONE_ID AS TIME_ZONE_ID,
        TIME_ZONE AS TIME_ZONE,
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
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
        EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC,
        EQUINE_SITE_ID AS EQUINE_SITE_ID,
        EQUINE_SITE_DESC AS EQUINE_SITE_DESC,
        EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
        LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
        LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
        LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
        LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
        LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
        LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        SITE_LOGIN_ID AS SITE_LOGIN_ID,
        SITE_MANAGER_ID AS SITE_MANAGER_ID,
        SITE_MANAGER_NAME AS SITE_MANAGER_NAME,
        MGR_ID AS MGR_ID,
        MGR_DESC AS MGR_DESC,
        DVL_ID AS DVL_ID,
        DVL_DESC AS DVL_DESC,
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
        DC_AREA_DIRECTOR_NAME AS DC_AREA_DIRECTOR_NAME,
        DC_AREA_DIRECTOR_EMAIL AS DC_AREA_DIRECTOR_EMAIL,
        DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
        DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
        REGION_VP_NAME AS REGION_VP_NAME,
        RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
        REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
        ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
        ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
        LP_SAFETY_DIRECTOR_NAME AS LP_SAFETY_DIRECTOR_NAME,
        LP_SAFETY_DIRECTOR_EMAIL AS LP_SAFETY_DIRECTOR_EMAIL,
        SR_LP_SAFETY_MGR_NAME AS SR_LP_SAFETY_MGR_NAME,
        SR_LP_SAFETY_MGR_EMAIL AS SR_LP_SAFETY_MGR_EMAIL,
        REGIONAL_LP_SAFETY_MGR_NAME AS REGIONAL_LP_SAFETY_MGR_NAME,
        REGIONAL_LP_SAFETY_MGR_EMAIL AS REGIONAL_LP_SAFETY_MGR_EMAIL,
        RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
        RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
        DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
        DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
        ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
        ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
        ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
        ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
        HR_MANAGER_NAME AS HR_MANAGER_NAME,
        HR_MANAGER_EMAIL AS HR_MANAGER_EMAIL,
        HR_SUPERVISOR_NAME1 AS HR_SUPERVISOR_NAME1,
        HR_SUPERVISOR_EMAIL1 AS HR_SUPERVISOR_EMAIL1,
        HR_SUPERVISOR_NAME2 AS HR_SUPERVISOR_NAME2,
        HR_SUPERVISOR_EMAIL2 AS HR_SUPERVISOR_EMAIL2,
        LEARN_SOLUTION_MGR_NAME AS LEARN_SOLUTION_MGR_NAME,
        LEARN_SOLUTION_MGR_EMAIL AS LEARN_SOLUTION_MGR_EMAIL,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE_RPT""")

df_4.createOrReplaceTempView("SITE_PROFILE_RPT_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_RPT_5


df_5=spark.sql("""
    SELECT
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        LOCATION_NBR AS LOCATION_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE_RPT_4""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_RPT_5")

# COMMAND ----------
# DBTITLE 1, Fil_Location_Type_6


df_6=spark.sql("""
    SELECT
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        LOCATION_NBR AS LOCATION_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SITE_PROFILE_RPT_5 
    WHERE
        LOCATION_TYPE_ID = 19""")

df_6.createOrReplaceTempView("Fil_Location_Type_6")

# COMMAND ----------
# DBTITLE 1, EXPTRANS1_7


df_7=spark.sql("""SELECT LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
LOCATION_NBR AS LOCATION_NBR,
1 AS Join,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM Fil_Location_Type_6""")

df_7.createOrReplaceTempView("EXPTRANS1_7")

# COMMAND ----------
# DBTITLE 1, JNRTRANS_8


df_8=spark.sql("""SELECT DETAIL.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
DETAIL.LOCATION_NBR AS LOCATION_NBR,
DETAIL.Join AS Join,
MASTER.DAY_DT AS DAY_DT,
MASTER.DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
MASTER.HOLIDAY_FLAG AS HOLIDAY_FLAG,
MASTER.Join AS Join1,
MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM EXPTRANS_3 MASTER  INNER JOIN EXPTRANS1_7 DETAIL  ON MASTER.Join = DETAIL.Join""")

df_8.createOrReplaceTempView("JNRTRANS_8")

# COMMAND ----------
# DBTITLE 1, EXPTRANS2_9


df_9=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        'Vendor' AS BUSINESS_AREA,
        LOCATION_NBR AS LOCATION_NBR,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        0 AS IS_CLOSE_FLAG,
        DAY_DT AS OPEN_TSTMP,
        ADD_TO_DATE(ADD_TO_DATE(DAY_DT,
        'D',
        1),
        'MS',
        -1) AS CLOSE_TSTMP,
        current_timestamp AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNRTRANS_8""")

df_9.createOrReplaceTempView("EXPTRANS2_9")

# COMMAND ----------
# DBTITLE 1, SITE_HOURS_DAY_PRE


spark.sql("""INSERT INTO SITE_HOURS_DAY_PRE SELECT DAY_DT AS DAY_DT,
LOCATION_NBR AS LOCATION_NBR,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
BUSINESS_AREA AS BUSINESS_AREA,
OPEN_TSTMP AS OPEN_TSTMP,
CLOSE_TSTMP AS CLOSE_TSTMP,
IS_CLOSE_FLAG AS IS_CLOSED,
LOAD_TSTMP AS LOAD_TSTMP FROM EXPTRANS2_9""")