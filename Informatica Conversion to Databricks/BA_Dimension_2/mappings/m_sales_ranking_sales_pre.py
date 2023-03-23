# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, USR_STORE_DIVISIONS_0

df_0=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        STORE_DIVISION_ID AS STORE_DIVISION_ID,
        STORE_DIVISION_DESC AS STORE_DIVISION_DESC,
        STORE_DIVISION_SORT_ID AS STORE_DIVISION_SORT_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        USR_STORE_DIVISIONS""")

df_0.createOrReplaceTempView("USR_STORE_DIVISIONS_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_USR_STORE_DIVISIONS_1

df_1=spark.sql("""
    SELECT
        SAP_DEPT_ID AS SAP_DEPT_ID,
        STORE_DIVISION_ID AS STORE_DIVISION_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_STORE_DIVISIONS_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_USR_STORE_DIVISIONS_1")

# COMMAND ----------

# DBTITLE 1, SALES_RANKING_DATE_PRE_2

df_2=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        RANKING_WEEK_DT AS RANKING_WEEK_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_RANKING_DATE_PRE""")

df_2.createOrReplaceTempView("SALES_RANKING_DATE_PRE_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SALES_RANKING_DATE_PRE_3

df_3=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
        RANKING_WEEK_DT AS RANKING_WEEK_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SALES_RANKING_DATE_PRE_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SALES_RANKING_DATE_PRE_3")

# COMMAND ----------

# DBTITLE 1, SALES_DAY_SKU_STORE_RPT_4

df_4=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        WEEK_DT AS WEEK_DT,
        FISCAL_YR AS FISCAL_YR,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        OPT_SALES_TYPE_ID AS OPT_SALES_TYPE_ID,
        VENDOR_ID AS VENDOR_ID,
        PROMO_FLAG AS PROMO_FLAG,
        STATUS_ID AS STATUS_ID,
        BRAND_NAME AS BRAND_NAME,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        SKU_VEND_TXN_CNT AS SKU_VEND_TXN_CNT,
        NET_SALES_AMT AS NET_SALES_AMT,
        NET_SALES_QTY AS NET_SALES_QTY,
        NET_MARGIN_AMT AS NET_MARGIN_AMT,
        SALES_AMT AS SALES_AMT,
        SALES_COST AS SALES_COST,
        SALES_QTY AS SALES_QTY,
        RETURN_AMT AS RETURN_AMT,
        RETURN_COST AS RETURN_COST,
        RETURN_QTY AS RETURN_QTY,
        DISCOUNT_AMT AS DISCOUNT_AMT,
        DISCOUNT_QTY AS DISCOUNT_QTY,
        DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
        DISCOUNT_RETURN_QTY AS DISCOUNT_RETURN_QTY,
        POS_COUPON_AMT AS POS_COUPON_AMT,
        POS_COUPON_QTY AS POS_COUPON_QTY,
        SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
        SPECIAL_SALES_QTY AS SPECIAL_SALES_QTY,
        SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
        SPECIAL_RETURN_QTY AS SPECIAL_RETURN_QTY,
        SPECIAL_SRVC_AMT AS SPECIAL_SRVC_AMT,
        SPECIAL_SRVC_QTY AS SPECIAL_SRVC_QTY,
        MA_SALES_AMT AS MA_SALES_AMT,
        MA_SALES_QTY AS MA_SALES_QTY,
        MA_TRANS_AMT AS MA_TRANS_AMT,
        MA_TRANS_COST AS MA_TRANS_COST,
        MA_TRANS_QTY AS MA_TRANS_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_DAY_SKU_STORE_RPT""")

df_4.createOrReplaceTempView("SALES_DAY_SKU_STORE_RPT_4")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_5

df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        WEEK_DT AS WEEK_DT,
        NET_SALES_AMT AS NET_SALES_AMT,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SALES_DAY_SKU_STORE_RPT_4""")

df_5.createOrReplaceTempView("SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_5")

# COMMAND ----------

# DBTITLE 1, JNR_WEEKS_6

df_6=spark.sql("""
    SELECT
        MASTER.WEEK_DT AS WEEK_DT_wk,
        MASTER.RANKING_WEEK_DT AS RANKING_WEEK_DT,
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.WEEK_DT AS WEEK_DT,
        DETAIL.NET_SALES_AMT AS NET_SALES_AMT,
        DETAIL.EXCH_RATE_PCT AS EXCH_RATE_PCT,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SALES_RANKING_DATE_PRE_3 MASTER 
    INNER JOIN
        SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_5 DETAIL 
            ON MASTER.WEEK_DT = DETAIL.WEEK_DT""")

df_6.createOrReplaceTempView("JNR_WEEKS_6")

# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_7

df_7=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_TYPE AS SKU_TYPE,
        PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
        STATUS_ID AS STATUS_ID,
        SUBS_HIST_FLAG AS SUBS_HIST_FLAG,
        SUBS_CURR_FLAG AS SUBS_CURR_FLAG,
        SKU_DESC AS SKU_DESC,
        ALT_DESC AS ALT_DESC,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        COUNTRY_CD AS COUNTRY_CD,
        IMPORT_FLAG AS IMPORT_FLAG,
        HTS_CODE_ID AS HTS_CODE_ID,
        CONTENTS AS CONTENTS,
        CONTENTS_UNITS AS CONTENTS_UNITS,
        WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
        WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
        SIZE_DESC AS SIZE_DESC,
        BUM_QTY AS BUM_QTY,
        UOM_CD AS UOM_CD,
        UNIT_NUMERATOR AS UNIT_NUMERATOR,
        UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
        BUYER_ID AS BUYER_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_COST_AMT AS PURCH_COST_AMT,
        NAT_PRICE_US_AMT AS NAT_PRICE_US_AMT,
        TAX_CLASS_ID AS TAX_CLASS_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        BRAND_CD AS BRAND_CD,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        STATELINE_FLAG AS STATELINE_FLAG,
        SIGN_TYPE_CD AS SIGN_TYPE_CD,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
        INIT_MKDN_DT AS INIT_MKDN_DT,
        DISC_START_DT AS DISC_START_DT,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        UPDATE_DT AS UPDATE_DT,
        FIRST_SALE_DT AS FIRST_SALE_DT,
        LAST_SALE_DT AS LAST_SALE_DT,
        FIRST_INV_DT AS FIRST_INV_DT,
        LAST_INV_DT AS LAST_INV_DT,
        LOAD_DT AS LOAD_DT,
        BASE_NBR AS BASE_NBR,
        BP_COLOR_ID AS BP_COLOR_ID,
        BP_SIZE_ID AS BP_SIZE_ID,
        BP_BREED_ID AS BP_BREED_ID,
        BP_ITEM_CONCATENATED AS BP_ITEM_CONCATENATED,
        BP_AEROSOL_FLAG AS BP_AEROSOL_FLAG,
        BP_HAZMAT_FLAG AS BP_HAZMAT_FLAG,
        CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
        NAT_PRICE_CA_AMT AS NAT_PRICE_CA_AMT,
        NAT_PRICE_PR_AMT AS NAT_PRICE_PR_AMT,
        RTV_DEPT_CD AS RTV_DEPT_CD,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
        COMPONENT_FLAG AS COMPONENT_FLAG,
        ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
        ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
        ZDISCO_PID_DT AS ZDISCO_PID_DT,
        ZDISCO_START_DT AS ZDISCO_START_DT,
        ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
        ZDISCO_DC_DT AS ZDISCO_DC_DT,
        ZDISCO_STR_DT AS ZDISCO_STR_DT,
        ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
        ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE""")

df_7.createOrReplaceTempView("SKU_PROFILE_7")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKU_PROFILE_8

df_8=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_7""")

df_8.createOrReplaceTempView("SQ_Shortcut_to_SKU_PROFILE_8")

# COMMAND ----------

# DBTITLE 1, SLSCMP_STORE_9

df_9=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        FIRST_SALE_DT AS FIRST_SALE_DT,
        FIRST_MEASURED_SALE_DT AS FIRST_MEASURED_SALE_DT,
        LAST_SALE_DT AS LAST_SALE_DT,
        SALES_CURR_FLAG AS SALES_CURR_FLAG,
        COMP_EFF_DT AS COMP_EFF_DT,
        COMP_END_DT AS COMP_END_DT,
        COMP_CURR_FLAG AS COMP_CURR_FLAG,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SLSCMP_STORE""")

df_9.createOrReplaceTempView("SLSCMP_STORE_9")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SLSCMP_STORE_10

df_10=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        SALES_CURR_FLAG AS SALES_CURR_FLAG,
        COMP_CURR_FLAG AS COMP_CURR_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SLSCMP_STORE_9""")

df_10.createOrReplaceTempView("SQ_Shortcut_to_SLSCMP_STORE_10")

# COMMAND ----------

# DBTITLE 1, JNR_SLS_CMP_11

df_11=spark.sql("""
    SELECT
        DETAIL.RANKING_WEEK_DT AS RANKING_WEEK_DT,
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.NET_SALES_AMT AS NET_SALES_AMT,
        DETAIL.EXCH_RATE_PCT AS EXCH_RATE_PCT,
        MASTER.LOCATION_ID AS LOCATION_ID_cmp,
        MASTER.COMP_CURR_FLAG AS COMP_CURR_FLAG,
        MASTER.SALES_CURR_FLAG AS SALES_CURR_FLAG,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SLSCMP_STORE_10 MASTER 
    INNER JOIN
        JNR_WEEKS_6 DETAIL 
            ON MASTER.LOCATION_ID = DETAIL.LOCATION_ID""")

df_11.createOrReplaceTempView("JNR_SLS_CMP_11")

# COMMAND ----------

# DBTITLE 1, SITE_PROFILE_12

df_12=spark.sql("""
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

df_12.createOrReplaceTempView("SITE_PROFILE_12")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_13

df_13=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE_12""")

df_13.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_13")

# COMMAND ----------

# DBTITLE 1, JNR_SITE_PROFILE_14

df_14=spark.sql("""
    SELECT
        DETAIL.RANKING_WEEK_DT AS RANKING_WEEK_DT,
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.NET_SALES_AMT AS NET_SALES_AMT,
        DETAIL.EXCH_RATE_PCT AS EXCH_RATE_PCT,
        DETAIL.COMP_CURR_FLAG AS COMP_CURR_FLAG,
        DETAIL.SALES_CURR_FLAG AS SALES_CURR_FLAG,
        MASTER.LOCATION_ID AS LOCATION_ID_site,
        MASTER.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SITE_PROFILE_13 MASTER 
    INNER JOIN
        JNR_SLS_CMP_11 DETAIL 
            ON MASTER.LOCATION_ID = DETAIL.LOCATION_ID""")

df_14.createOrReplaceTempView("JNR_SITE_PROFILE_14")

# COMMAND ----------

# DBTITLE 1, JNR_SKU_PROFILE_15

df_15=spark.sql("""
    SELECT
        DETAIL.RANKING_WEEK_DT AS RANKING_WEEK_DT,
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.NET_SALES_AMT AS NET_SALES_AMT,
        DETAIL.EXCH_RATE_PCT AS EXCH_RATE_PCT,
        DETAIL.COMP_CURR_FLAG AS COMP_CURR_FLAG,
        DETAIL.SALES_CURR_FLAG AS SALES_CURR_FLAG,
        DETAIL.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        MASTER.PRODUCT_ID AS PRODUCT_ID_sku,
        MASTER.SAP_DEPT_ID AS SAP_DEPT_ID,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_PROFILE_8 MASTER 
    INNER JOIN
        JNR_SITE_PROFILE_14 DETAIL 
            ON MASTER.PRODUCT_ID = DETAIL.PRODUCT_ID""")

df_15.createOrReplaceTempView("JNR_SKU_PROFILE_15")

# COMMAND ----------

# DBTITLE 1, JNR_USR_STORE_DIVISIONS_16

df_16=spark.sql("""
    SELECT
        DETAIL.RANKING_WEEK_DT AS RANKING_WEEK_DT,
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.NET_SALES_AMT AS NET_SALES_AMT,
        DETAIL.EXCH_RATE_PCT AS EXCH_RATE_PCT,
        DETAIL.COMP_CURR_FLAG AS COMP_CURR_FLAG,
        DETAIL.SALES_CURR_FLAG AS SALES_CURR_FLAG,
        DETAIL.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        DETAIL.SAP_DEPT_ID AS SAP_DEPT_ID,
        MASTER.SAP_DEPT_ID AS SAP_DEPT_ID_store_div,
        MASTER.STORE_DIVISION_ID AS STORE_DIVISION_ID,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_USR_STORE_DIVISIONS_1 MASTER 
    INNER JOIN
        JNR_SKU_PROFILE_15 DETAIL 
            ON MASTER.SAP_DEPT_ID = DETAIL.SAP_DEPT_ID""")

df_16.createOrReplaceTempView("JNR_USR_STORE_DIVISIONS_16")

# COMMAND ----------

# DBTITLE 1, EXP_CONV_LOCAL_DOLLARS_17

df_17=spark.sql("""
    SELECT
        RANKING_WEEK_DT AS RANKING_WEEK_DT,
        LOCATION_ID AS LOCATION_ID,
        NET_SALES_AMT AS NET_SALES_AMT,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        COMP_CURR_FLAG AS COMP_CURR_FLAG,
        SALES_CURR_FLAG AS SALES_CURR_FLAG,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_DIVISION_ID AS STORE_DIVISION_ID,
        IFF(STORE_DIVISION_ID = 1,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS TOTAL_SALES,
        IFF(STORE_DIVISION_ID = 2,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS MERCH_SALES,
        IFF(STORE_DIVISION_ID = 5,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS SERVICES_SALES,
        IFF(STORE_DIVISION_ID = 6,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS SALON_SALES,
        IFF(STORE_DIVISION_ID = 7,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS TRAINING_SALES,
        IFF(STORE_DIVISION_ID = 8,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS HOTEL_DDC_SALES,
        IFF(STORE_DIVISION_ID = 9,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS CONSUMABLES_SALES,
        IFF(STORE_DIVISION_ID = 10,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS HARDGOODS_SALES,
        IFF(STORE_DIVISION_ID = 3,
        NET_SALES_AMT * EXCH_RATE_PCT,
        0) AS SPECIALTY_SALES,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_USR_STORE_DIVISIONS_16""")

df_17.createOrReplaceTempView("EXP_CONV_LOCAL_DOLLARS_17")

# COMMAND ----------

# DBTITLE 1, AGG_52_TOTALS_18

df_18=spark.sql("""
    SELECT
        MAX(in_RANKING_WEEK_DT) AS RANKING_WEEK_DT1,
        LOCATION_ID AS LOCATION_ID,
        ROUND(SUM(in_TOTAL_SALES),
        2) AS TOTAL_SALES,
        ROUND(SUM(in_MERCH_SALES),
        2) AS MERCH_SALES,
        ROUND(SUM(in_SERVICES_SALES),
        2) AS SERVICES_SALES,
        ROUND(SUM(in_SALON_SALES),
        2) AS SALON_SALES,
        ROUND(SUM(in_TRAINING_SALES),
        2) AS TRAINING_SALES,
        ROUND(SUM(in_HOTEL_DDC_SALES),
        2) AS HOTEL_DDC_SALES,
        ROUND(SUM(in_CONSUMABLES_SALES),
        2) AS CONSUMABLES_SALES,
        ROUND(SUM(in_HARDGOODS_SALES),
        2) AS HARDGOODS_SALES,
        ROUND(SUM(in_SPECIALTY_SALES),
        2) AS SPECIALTY_SALES,
        MAX(in_COMP_CURR_FLAG) AS COMP_CURR_FLAG,
        MAX(in_SALES_CURR_FLAG) AS SALES_CURR_FLAG,
        MAX(in_LOCATION_TYPE_ID) AS LOCATION_TYPE_ID 
    FROM
        EXP_CONV_LOCAL_DOLLARS_17 
    GROUP BY
        LOCATION_ID""")

df_18.createOrReplaceTempView("AGG_52_TOTALS_18")

# COMMAND ----------

# DBTITLE 1, SALES_RANKING_SALES_PRE

spark.sql("""INSERT INTO SALES_RANKING_SALES_PRE SELECT RANKING_WEEK_DT1 AS WEEK_DT,
LOCATION_ID AS LOCATION_ID,
TOTAL_SALES AS TOTAL_52WK_SALES_AMT,
MERCH_SALES AS MERCH_52WK_SALES_AMT,
SERVICES_SALES AS SERVICES_52WK_SALES_AMT,
SALON_SALES AS SALON_52WK_SALES_AMT,
TRAINING_SALES AS TRAINING_52WK_SALES_AMT,
HOTEL_DDC_SALES AS HOTEL_DDC_52WK_SALES_AMT,
CONSUMABLES_SALES AS CONSUMABLES_52WK_SALES_AMT,
HARDGOODS_SALES AS HARDGOODS_52WK_SALES_AMT,
SPECIALTY_SALES AS SPECIALTY_52WK_SALES_AMT,
COMP_CURR_FLAG AS COMP_CURR_FLAG,
SALES_CURR_FLAG AS SALES_CURR_FLAG,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID FROM AGG_52_TOTALS_18""")
