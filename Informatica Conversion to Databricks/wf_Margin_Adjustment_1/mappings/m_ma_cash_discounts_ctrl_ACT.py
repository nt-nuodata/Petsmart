# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, MA_EVENT_TYPE_VAR_CTRL_0


df_0=spark.sql("""
    SELECT
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_TYPE_VAR_TYPE_CD AS MA_EVENT_TYPE_VAR_TYPE_CD,
        MA_EVENT_TYPE_VAR_VALUE AS MA_EVENT_TYPE_VAR_VALUE,
        START_EFF_DT AS START_EFF_DT,
        END_EFF_DT AS END_EFF_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_TYPE_VAR_CTRL""")

df_0.createOrReplaceTempView("MA_EVENT_TYPE_VAR_CTRL_0")

# COMMAND ----------
# DBTITLE 1, MA_CASH_DISCOUNT_CTRL_1


df_1=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        EST_CASH_DISCOUNT_PCT AS EST_CASH_DISCOUNT_PCT,
        ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
        ACT_CASH_DISCOUNT_PCT AS ACT_CASH_DISCOUNT_PCT,
        OVRD_CASH_DISCOUNT_PCT AS OVRD_CASH_DISCOUNT_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_CASH_DISCOUNT_CTRL""")

df_1.createOrReplaceTempView("MA_CASH_DISCOUNT_CTRL_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_2


df_2=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        ACT_CASH_DISCOUNT_PCT AS ACT_CASH_DISCOUNT_PCT,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        MA_CASH_DISCOUNT_CTRL_1""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_2")

# COMMAND ----------
# DBTITLE 1, MA_FISCAL_MO_CTRL_3


df_3=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        RESTATE_DT AS RESTATE_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_FISCAL_MO_CTRL""")

df_3.createOrReplaceTempView("MA_FISCAL_MO_CTRL_3")

# COMMAND ----------
# DBTITLE 1, MONTHS_4


df_4=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        FISCAL_HALF AS FISCAL_HALF,
        FISCAL_MO_NAME AS FISCAL_MO_NAME,
        FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
        FISCAL_MO_NBR AS FISCAL_MO_NBR,
        FISCAL_QTR AS FISCAL_QTR,
        FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
        FISCAL_YR AS FISCAL_YR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MONTHS""")

df_4.createOrReplaceTempView("MONTHS_4")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE_RPT_5


df_5=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_TYPE_DESC AS VENDOR_TYPE_DESC,
        VENDOR_NBR AS VENDOR_NBR,
        LOCATION_ID AS LOCATION_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        MGR_ID AS MGR_ID,
        MGR_DESC AS MGR_DESC,
        DVL_ID AS DVL_ID,
        DVL_DESC AS DVL_DESC,
        EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
        PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        PAYMENT_TERM_DESC AS PAYMENT_TERM_DESC,
        INCO_TERM_CD AS INCO_TERM_CD,
        INCO_TERM_DESC AS INCO_TERM_DESC,
        ADDRESS AS ADDRESS,
        CITY AS CITY,
        STATE AS STATE,
        STATE_NAME AS STATE_NAME,
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        ZIP AS ZIP,
        CONTACT AS CONTACT,
        CONTACT_PHONE AS CONTACT_PHONE,
        PHONE AS PHONE,
        PHONE_EXT AS PHONE_EXT,
        FAX AS FAX,
        RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
        RTV_TYPE_CD AS RTV_TYPE_CD,
        RTV_TYPE_DESC AS RTV_TYPE_DESC,
        RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
        INDUSTRY_CD AS INDUSTRY_CD,
        LATITUDE AS LATITUDE,
        LONGITUDE AS LONGITUDE,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        ADD_DT AS ADD_DT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PROFILE_RPT""")

df_5.createOrReplaceTempView("VENDOR_PROFILE_RPT_5")

# COMMAND ----------
# DBTITLE 1, SALES_DAY_SKU_STORE_RPT_6


df_6=spark.sql("""
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
        CLEARANCE_AMT AS CLEARANCE_AMT,
        CLEARANCE_QTY AS CLEARANCE_QTY,
        CLEARANCE_RETURN_AMT AS CLEARANCE_RETURN_AMT,
        CLEARANCE_RETURN_QTY AS CLEARANCE_RETURN_QTY,
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

df_6.createOrReplaceTempView("SALES_DAY_SKU_STORE_RPT_6")

# COMMAND ----------
# DBTITLE 1, GL_ACCOUNT_PROFILE_7


df_7=spark.sql("""
    SELECT
        GL_ACCOUNT_GID AS GL_ACCOUNT_GID,
        GL_CHART_OF_ACCOUNTS_CD AS GL_CHART_OF_ACCOUNTS_CD,
        GL_ACCOUNT_NBR AS GL_ACCOUNT_NBR,
        GL_ACCOUNT_DESC AS GL_ACCOUNT_DESC,
        GL_ACCOUNT_GROUP_CD AS GL_ACCOUNT_GROUP_CD,
        GL_ACCOUNT_GROUP_DESC AS GL_ACCOUNT_GROUP_DESC,
        GL_BAL_SHEET_IND AS GL_BAL_SHEET_IND,
        GL_PL_IND AS GL_PL_IND,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_ACCOUNT_PROFILE""")

df_7.createOrReplaceTempView("GL_ACCOUNT_PROFILE_7")

# COMMAND ----------
# DBTITLE 1, DAYS_8


df_8=spark.sql("""
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

df_8.createOrReplaceTempView("DAYS_8")

# COMMAND ----------
# DBTITLE 1, SQ_SALES_QUERY_9


df_9=spark.sql("""
    SELECT
        SLS.FISCAL_MO,
        SLS.VENDOR_ID,
        SLS.ACT_NET_SALES_COST 
    FROM
        (SELECT
            D.FISCAL_MO,
            S.VENDOR_ID,
            SUM((SALES_COST - RETURN_COST) * EXCH_RATE_PCT) ACT_NET_SALES_COST 
        FROM
            SALES_DAY_SKU_STORE_RPT S 
        JOIN
            DAYS D 
                ON S.DAY_DT = D.DAY_DT 
        JOIN
            MA_FISCAL_MO_CTRL MFMC 
                ON D.FISCAL_MO = MFMC.FISCAL_MO 
                AND MFMC.RESTATE_DT = CURRENT_DATE 
        GROUP BY
            D.FISCAL_MO,
            S.VENDOR_ID) SLS""")

df_9.createOrReplaceTempView("SQ_SALES_QUERY_9")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE_RPT_10


df_10=spark.sql("""
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

df_10.createOrReplaceTempView("SITE_PROFILE_RPT_10")

# COMMAND ----------
# DBTITLE 1, GL_ACTUAL_DETAIL_11


df_11=spark.sql("""
    SELECT
        FISCAL_YR AS FISCAL_YR,
        FISCAL_MO AS FISCAL_MO,
        GL_DOCUMENT_NBR AS GL_DOCUMENT_NBR,
        GL_COMPANY_CD AS GL_COMPANY_CD,
        GL_DOCUMENT_LINE_NBR AS GL_DOCUMENT_LINE_NBR,
        FISCAL_WK AS FISCAL_WK,
        WEEK_DT AS WEEK_DT,
        GL_DOCUMENT_DT AS GL_DOCUMENT_DT,
        GL_POSTING_DT AS GL_POSTING_DT,
        GL_DOCUMENT_ENTRY_DT AS GL_DOCUMENT_ENTRY_DT,
        GL_DOCUMENT_TYPE_CD AS GL_DOCUMENT_TYPE_CD,
        GL_REF_DOCUMENT_NBR AS GL_REF_DOCUMENT_NBR,
        GL_PROFIT_CENTER_GID AS GL_PROFIT_CENTER_GID,
        LOCATION_ID AS LOCATION_ID,
        STORE_NBR AS STORE_NBR,
        GL_DEPARTMENT_CD AS GL_DEPARTMENT_CD,
        GL_DEBIT_CREDIT_IND AS GL_DEBIT_CREDIT_IND,
        GL_ACCOUNT_GID AS GL_ACCOUNT_GID,
        GL_BALANCE_SHEET_IND AS GL_BALANCE_SHEET_IND,
        GL_PL_SHEET_IND AS GL_PL_SHEET_IND,
        GL_SPLIT_LINE_ITEM_IND AS GL_SPLIT_LINE_ITEM_IND,
        GL_BUSINESS_TXN_TYPE_CD AS GL_BUSINESS_TXN_TYPE_CD,
        GL_REF_TXN_TYPE_CD AS GL_REF_TXN_TYPE_CD,
        GL_TXN_TYPE_CD AS GL_TXN_TYPE_CD,
        GL_COST_ELEMENT_CD AS GL_COST_ELEMENT_CD,
        GL_POSTING_KEY_GID AS GL_POSTING_KEY_GID,
        GL_CONTROLLING_AREA AS GL_CONTROLLING_AREA,
        GL_SEGMENT_CD AS GL_SEGMENT_CD,
        GL_PARTNER_PROFIT_CENTER_GID AS GL_PARTNER_PROFIT_CENTER_GID,
        GL_PARTNER_COMPANY_CD AS GL_PARTNER_COMPANY_CD,
        GL_PARTNER_SEGMENT_CD AS GL_PARTNER_SEGMENT_CD,
        VENDOR_ID AS VENDOR_ID,
        GL_ITEM_CATEGORY_CD AS GL_ITEM_CATEGORY_CD,
        GL_PURCH_DOC_NBR AS GL_PURCH_DOC_NBR,
        GL_DOC_CURRENCY_ID AS GL_DOC_CURRENCY_ID,
        GL_DOC_AMT AS GL_DOC_AMT,
        GL_LOC_CURRENCY_ID AS GL_LOC_CURRENCY_ID,
        GL_LOC_AMT AS GL_LOC_AMT,
        GL_GRP_CURRENCY_ID AS GL_GRP_CURRENCY_ID,
        GL_GRP_AMT AS GL_GRP_AMT,
        GL_QTY AS GL_QTY,
        GL_QTY_UOM_CD AS GL_QTY_UOM_CD,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        GL_USER_NAME AS GL_USER_NAME,
        GL_LOAD_TSTMP AS GL_LOAD_TSTMP,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_ACTUAL_DETAIL""")

df_11.createOrReplaceTempView("GL_ACTUAL_DETAIL_11")

# COMMAND ----------
# DBTITLE 1, SQ_GL_QUERY_12


df_12=spark.sql("""
    SELECT
        GL.FISCAL_MO,
        GL.VENDOR_ID,
        GL.ACT_CASH_DISCOUNT_GL_AMT 
    FROM
        (SELECT
            P.FISCAL_MO,
            V.VENDOR_ID,
            -SUM(P.CASH_DISCOUNT_AMT) AS ACT_CASH_DISCOUNT_GL_AMT 
        FROM
            (SELECT
                GAD.FISCAL_MO,
                GAD.GL_DOCUMENT_NBR,
                GAD.GL_COMPANY_CD,
                MAX(GAD.VENDOR_ID) OVER (PARTITION 
            BY
                gad.FISCAL_MO,
                gad.GL_DOCUMENT_NBR,
                gad.GL_COMPANY_CD ) AS VENDOR_ID,
                CASE 
                    WHEN METVC.MA_EVENT_TYPE_VAR_VALUE IS NOT NULL THEN GAD.GL_GRP_AMT 
                    ELSE 0 
                END CASH_DISCOUNT_AMT 
            FROM
                GL_ACTUAL_DETAIL GAD 
            JOIN
                SITE_PROFILE_RPT SITE 
                    ON GAD.LOCATION_ID = SITE.LOCATION_ID 
            JOIN
                GL_ACCOUNT_PROFILE GAP 
                    ON GAD.GL_ACCOUNT_GID = GAP.GL_ACCOUNT_GID 
            JOIN
                (
                    SELECT
                        DISTINCT GAD.FISCAL_MO,
                        GAD.GL_DOCUMENT_NBR,
                        GAD.GL_COMPANY_CD 
                    FROM
                        GL_ACTUAL_DETAIL GAD 
                    JOIN
                        GL_ACCOUNT_PROFILE GAP 
                            ON GAD.GL_ACCOUNT_GID = GAP.GL_ACCOUNT_GID 
                    JOIN
                        MONTHS m 
                            ON GAD.FISCAL_MO = M.FISCAL_MO 
                    JOIN
                        MA_FISCAL_MO_CTRL MFMC 
                            ON M.FISCAL_MO = MFMC.FISCAL_MO 
                            AND MFMC.RESTATE_DT = CURRENT_DATE 
                    JOIN
                        (
                            SELECT
                                MA_EVENT_TYPE_VAR_VALUE,
                                START_EFF_DT,
                                END_EFF_DT 
                            FROM
                                MA_EVENT_TYPE_VAR_CTRL 
                            WHERE
                                MA_EVENT_TYPE_VAR_TYPE_CD = 'GL_ACCT_NBR' 
                                AND MA_EVENT_TYPE_ID = 90
                        ) METVC 
                            ON GAP.GL_ACCOUNT_NBR::VARCHAR (25) = METVC.MA_EVENT_TYPE_VAR_VALUE 
                            AND GAD.GL_POSTING_DT BETWEEN METVC.START_EFF_DT AND METVC.END_EFF_DT
                        ) pre 
                            ON gad.FISCAL_MO = pre.FISCAL_MO 
                            AND gad.GL_DOCUMENT_NBR = pre.GL_DOCUMENT_NBR 
                            AND gad.GL_COMPANY_CD = pre.GL_COMPANY_CD 
                    LEFT JOIN
                        (
                            SELECT
                                MA_EVENT_TYPE_VAR_VALUE,
                                START_EFF_DT,
                                END_EFF_DT 
                            FROM
                                MA_EVENT_TYPE_VAR_CTRL 
                            WHERE
                                MA_EVENT_TYPE_VAR_TYPE_CD = 'GL_ACCT_NBR' 
                                AND MA_EVENT_TYPE_ID = 90
                        ) METVC 
                            ON GAP.GL_ACCOUNT_NBR::VARCHAR (25) = METVC.MA_EVENT_TYPE_VAR_VALUE 
                            AND GAD.GL_POSTING_DT BETWEEN METVC.START_EFF_DT AND METVC.END_EFF_DT
                        ) P 
                JOIN
                    VENDOR_PROFILE_RPT V 
                        ON CAST(P.VENDOR_ID AS VARCHAR (50)) = V.VENDOR_NBR 
                        AND V.VENDOR_TYPE_ID IN (
                            2
                        ) 
                GROUP BY
                    P.FISCAL_MO,
                    V.VENDOR_ID
            ) GL""")

df_12.createOrReplaceTempView("SQ_GL_QUERY_12")

# COMMAND ----------
# DBTITLE 1, JNR_GL_SALES_13


df_13=spark.sql("""
    SELECT
        MASTER.FISCAL_MO AS FISCAL_MO1,
        MASTER.VENDOR_ID AS VENDOR_ID1,
        MASTER.ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
        DETAIL.FISCAL_MO AS FISCAL_MO,
        DETAIL.VENDOR_ID AS VENDOR_ID,
        DETAIL.ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_GL_QUERY_12 MASTER 
    RIGHT JOIN
        SQ_SALES_QUERY_9 DETAIL 
            ON MASTER.FISCAL_MO = FISCAL_MO 
            AND VENDOR_ID1 = DETAIL.VENDOR_ID""")

df_13.createOrReplaceTempView("JNR_GL_SALES_13")

# COMMAND ----------
# DBTITLE 1, JNR_MA_CASH_DISCOUNT_14


df_14=spark.sql("""
    SELECT
        MASTER.FISCAL_MO1 AS FISCAL_MO1,
        MASTER.VENDOR_ID1 AS VENDOR_ID1,
        MASTER.ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
        MASTER.ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        DETAIL.FISCAL_MO AS FISCAL_MO,
        DETAIL.SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        DETAIL.ACT_CASH_DISCOUNT_PCT AS ACT_CASH_DISCOUNT_PCT,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_GL_SALES_13 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_MA_CASH_DISCOUNT_CTRL_2 DETAIL 
            ON MASTER.FISCAL_MO1 = FISCAL_MO 
            AND VENDOR_ID1 = DETAIL.SOURCE_VENDOR_ID""")

df_14.createOrReplaceTempView("JNR_MA_CASH_DISCOUNT_14")

# COMMAND ----------
# DBTITLE 1, EXP_ACT_CASH_DISCOUNT_PCT_15


df_15=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
        ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        v_THIS_ACT_CASH_DISCOUNT_PCT AS THIS_ACT_CASH_DISCOUNT_PCT,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP,
        IFF(ISNULL(FISCAL_MO),
        'I',
        IFF(v_THIS_ACT_CASH_DISCOUNT_PCT <> v_ACT_CASH_DISCOUNT_PCT,
        'U',
        'X')) AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_MA_CASH_DISCOUNT_14""")

df_15.createOrReplaceTempView("EXP_ACT_CASH_DISCOUNT_PCT_15")

# COMMAND ----------
# DBTITLE 1, FIL_INS_UPD_FLAG_16


df_16=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
        THIS_ACT_CASH_DISCOUNT_PCT AS THIS_ACT_CASH_DISCOUNT_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_ACT_CASH_DISCOUNT_PCT_15 
    WHERE
        INS_UPD_FLAG <> 'X'""")

df_16.createOrReplaceTempView("FIL_INS_UPD_FLAG_16")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_17


df_17=spark.sql("""
    SELECT
        FISCAL_MO AS FISCAL_MO,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
        THIS_ACT_CASH_DISCOUNT_PCT AS THIS_ACT_CASH_DISCOUNT_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_INS_UPD_FLAG_16""")

df_17.createOrReplaceTempView("UPD_STRATEGY_17")

# COMMAND ----------
# DBTITLE 1, MA_CASH_DISCOUNT_CTRL


spark.sql("""INSERT INTO MA_CASH_DISCOUNT_CTRL SELECT FISCAL_MO AS FISCAL_MO,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
EST_CASH_DISCOUNT_PCT AS EST_CASH_DISCOUNT_PCT,
ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
ACT_CASH_DISCOUNT_GL_AMT AS ACT_CASH_DISCOUNT_GL_AMT,
ACT_CASH_DISCOUNT_PCT AS ACT_CASH_DISCOUNT_PCT,
OVRD_CASH_DISCOUNT_PCT1 AS OVRD_CASH_DISCOUNT_PCT,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPD_STRATEGY_17""")