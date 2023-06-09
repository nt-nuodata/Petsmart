# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SEQ_MA_EVENT_ID

spark.sql("""CREATE TABLE SEQ_MA_EVENT_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int);""")

spark.sql("""INSERT INTO SEQ_MA_EVENT_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int) VALUES(2, 1, 1)""")

# COMMAND ----------

# DBTITLE 1, MA_OB_FREIGHT_CTRL_1

df_1=spark.sql("""
    SELECT
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        FISCAL_MO AS FISCAL_MO,
        R12_NET_SALES_COST AS R12_NET_SALES_COST,
        R12_OB_FREIGHT_COST AS R12_OB_FREIGHT_COST,
        R12_OB_FREIGHT_PCT AS R12_OB_FREIGHT_PCT,
        ACT_NET_SALES_COST AS ACT_NET_SALES_COST,
        ACT_OB_FREIGHT_COST AS ACT_OB_FREIGHT_COST,
        ACT_OB_FREIGHT_PCT AS ACT_OB_FREIGHT_PCT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_OB_FREIGHT_CTRL""")

df_1.createOrReplaceTempView("MA_OB_FREIGHT_CTRL_1")

# COMMAND ----------

# DBTITLE 1, MA_EVENT_2

df_2=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        OFFER_ID AS OFFER_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        LOCATION_ID AS LOCATION_ID,
        MOVEMENT_ID AS MOVEMENT_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
        BRAND_CD AS BRAND_CD,
        MA_FORMULA_CD AS MA_FORMULA_CD,
        FISCAL_MO AS FISCAL_MO,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        COMPANY_ID AS COMPANY_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
        EM_COMMENT AS EM_COMMENT,
        EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
        EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
        EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
        EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
        EM_VENDOR_ID AS EM_VENDOR_ID,
        EM_VENDOR_NAME AS EM_VENDOR_NAME,
        EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
        VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
        MA_PCT_IND AS MA_PCT_IND,
        MA_AMT AS MA_AMT,
        MA_MAX_AMT AS MA_MAX_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT""")

df_2.createOrReplaceTempView("MA_EVENT_2")

# COMMAND ----------

# DBTITLE 1, SQ_MA_OB_FREIGHT_CTRL_UPDATES_3

df_3=spark.sql("""
    SELECT
        M.MA_EVENT_ID,
        M.START_DT,
        M.END_DT,
        70 MA_EVENT_TYPE_ID,
        12 MA_EVENT_SOURCE_ID,
        MOFC.FISCAL_MO,
        MOFC.FROM_LOCATION_ID,
        'Outbound Freight: DC=' || SITE.STORE_NBR::VARCHAR (10) || '; FMth=' || MOFC.FISCAL_MO::VARCHAR (10) MA_EVENT_DESC,
        1 MA_PCT_IND,
        NVL(MOFC.ACT_OB_FREIGHT_PCT,
        R12_OB_FREIGHT_PCT) * -100 AS NEW_MA_AMT,
        M.MA_AMT AS ORIG_MA_AMT,
        'U' AS INS_UPD_DEL_FLAG,
        M.LOAD_DT 
    FROM
        MA_OB_FREIGHT_CTRL MOFC 
    JOIN
        SITE_PROFILE SITE 
            ON MOFC.FROM_LOCATION_ID = SITE.LOCATION_ID 
    JOIN
        MA_EVENT M 
            ON MOFC.FISCAL_MO = M.FISCAL_MO 
            AND MOFC.FROM_LOCATION_ID = M.FROM_LOCATION_ID 
    WHERE
        NVL(MOFC.ACT_OB_FREIGHT_PCT, R12_OB_FREIGHT_PCT) * -100 <> M.MA_AMT 
        AND M.START_DT >= (
            SELECT
                WEEK_DT + 1 
            FROM
                DAYS 
            WHERE
                DAY_DT = CURRENT_DATE - 96
        )""")

df_3.createOrReplaceTempView("SQ_MA_OB_FREIGHT_CTRL_UPDATES_3")

# COMMAND ----------

# DBTITLE 1, EXP_ORIG_4

df_4=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        FISCAL_MO AS FISCAL_MO,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        NEW_MA_AMT AS NEW_MA_AMT,
        ORIG_MA_AMT AS ORIG_MA_AMT,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        current_timestamp AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_MA_OB_FREIGHT_CTRL_UPDATES_3""")

df_4.createOrReplaceTempView("EXP_ORIG_4")

# COMMAND ----------

# DBTITLE 1, DAYS_5

df_5=spark.sql("""
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

df_5.createOrReplaceTempView("DAYS_5")

# COMMAND ----------

# DBTITLE 1, SQ_MA_OB_FREIGHT_CTRL_INSERTS_6

df_6=spark.sql("""
    SELECT
        D.START_DT,
        D.END_DT,
        70 MA_EVENT_TYPE_ID,
        12 MA_EVENT_SOURCE_ID,
        MOFC.FISCAL_MO,
        MOFC.FROM_LOCATION_ID,
        'Outbound Freight: DC=' || SITE.STORE_NBR::VARCHAR (10) || '; FMth=' || MOFC.FISCAL_MO::VARCHAR (10) MA_EVENT_DESC,
        1 MA_PCT_IND,
        NVL(MOFC.ACT_OB_FREIGHT_PCT,
        R12_OB_FREIGHT_PCT) * -100 AS NEW_MA_AMT,
        NEW_MA_AMT AS ORIG_MA_AMT,
        'I' AS INS_UPD_DEL_FLAG 
    FROM
        MA_OB_FREIGHT_CTRL MOFC 
    JOIN
        SITE_PROFILE SITE 
            ON MOFC.FROM_LOCATION_ID = SITE.LOCATION_ID 
    JOIN
        (
            SELECT
                FISCAL_MO,
                MIN(DAY_DT) START_DT,
                MAX(DAY_DT) END_DT 
            FROM
                DAYS 
            GROUP BY
                FISCAL_MO
        ) D 
            ON MOFC.FISCAL_MO = D.FISCAL_MO 
    LEFT JOIN
        MA_EVENT M 
            ON MOFC.FISCAL_MO = M.FISCAL_MO 
            AND MOFC.FROM_LOCATION_ID = M.FROM_LOCATION_ID 
    WHERE
        M.MA_EVENT_ID IS NULL 
        AND D.START_DT >= (
            SELECT
                week_dt + 1 
            FROM
                days 
            WHERE
                day_dt = CURRENT_DATE - 96
        )""")

df_6.createOrReplaceTempView("SQ_MA_OB_FREIGHT_CTRL_INSERTS_6")

# COMMAND ----------

# DBTITLE 1, EXP_NEW_7

df_7=spark.sql("""
    SELECT
        (ROW_NUMBER() OVER (
    ORDER BY
        (SELECT
            NULL)) - 1) * (SELECT
            Increment_By 
        FROM
            SEQ_MA_EVENT_ID) + (SELECT
            NEXTVAL 
        FROM
            SEQ_MA_EVENT_ID) AS MA_EVENT_ID,
        SEQ_MA_EVENT_ID.NEXTVAL AS MA_EVENT_ID,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.START_DT AS START_DT,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.END_DT AS END_DT,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.FISCAL_MO AS FISCAL_MO,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.FROM_LOCATION_ID AS FROM_LOCATION_ID,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.MA_EVENT_DESC AS MA_EVENT_DESC,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.MA_PCT_IND AS MA_PCT_IND,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.NEW_MA_AMT AS NEW_MA_AMT,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.ORIG_MA_AMT AS ORIG_MA_AMT,
        current_timestamp AS UPDATE_DT,
        current_timestamp AS LOAD_DT,
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        SEQ_MA_EVENT_ID.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SEQ_MA_EVENT_ID 
    INNER JOIN
        SQ_MA_OB_FREIGHT_CTRL_INSERTS_6 
            ON SEQ_MA_EVENT_ID.Monotonically_Increasing_Id = SQ_MA_OB_FREIGHT_CTRL_INSERTS_6.Monotonically_Increasing_Id""")

df_7.createOrReplaceTempView("EXP_NEW_7")

spark.sql("""UPDATE SEQ_MA_EVENT_ID SET CURRVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_NEW_7) , NEXTVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_NEW_7) + (SELECT Increment_By FROM EXP_NEW_7)""")

# COMMAND ----------

# DBTITLE 1, UNI_NEW_ORIG_8

df_8=spark.sql("""SELECT END_DT AS END_DT,
FISCAL_MO AS FISCAL_MO,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
LOAD_DT AS LOAD_DT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_PCT_IND AS MA_PCT_IND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
NEW_MA_AMT AS NEW_MA_AMT,
ORIG_MA_AMT AS ORIG_MA_AMT,
START_DT AS START_DT,
UPDATE_DT AS UPDATE_DT FROM EXP_NEW_7 UNION ALL SELECT END_DT AS END_DT,
FISCAL_MO AS FISCAL_MO,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
LOAD_DT AS LOAD_DT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_PCT_IND AS MA_PCT_IND,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
NEW_MA_AMT AS NEW_MA_AMT,
ORIG_MA_AMT AS ORIG_MA_AMT,
START_DT AS START_DT,
UPDATE_DT AS UPDATE_DT FROM EXP_ORIG_4""")

df_8.createOrReplaceTempView("UNI_NEW_ORIG_8")

# COMMAND ----------

# DBTITLE 1, UPD_STRATEGY_9

df_9=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        FISCAL_MO AS FISCAL_MO,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        NEW_MA_AMT AS NEW_MA_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UNI_NEW_ORIG_8""")

df_9.createOrReplaceTempView("UPD_STRATEGY_9")

# COMMAND ----------

# DBTITLE 1, EXP_INS_UPD_10

df_10=spark.sql("""
    SELECT
        date_trunc('DAY',
        current_timestamp) AS LOAD_DT,
        MA_EVENT_ID AS MA_EVENT_ID,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        FISCAL_MO AS FISCAL_MO,
        FROM_LOCATION_ID AS FROM_LOCATION_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_PCT_IND AS MA_PCT_IND,
        ORIG_MA_AMT AS ORIG_MA_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT1 AS LOAD_DT1,
        INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UNI_NEW_ORIG_8""")

df_10.createOrReplaceTempView("EXP_INS_UPD_10")

# COMMAND ----------

# DBTITLE 1, SITE_PROFILE_11

df_11=spark.sql("""
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

df_11.createOrReplaceTempView("SITE_PROFILE_11")

# COMMAND ----------

# DBTITLE 1, MA_EVENT_RESTATE_HIST

spark.sql("""INSERT INTO MA_EVENT_RESTATE_HIST SELECT LOAD_DT AS LOAD_DT,
MA_EVENT_ID AS MA_EVENT_ID,
OFFER_ID AS OFFER_ID,
SAP_DEPT_ID AS SAP_DEPT_ID,
PRODUCT_ID AS PRODUCT_ID,
COUNTRY_CD AS COUNTRY_CD,
START_DT AS START_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
LOCATION_ID AS LOCATION_ID,
MOVEMENT_ID AS MOVEMENT_ID,
VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
GL_ACCT_NBR AS GL_ACCT_NBR,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
COMPANY_ID AS COMPANY_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_COMMENT AS EM_COMMENT,
EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
MA_PCT_IND AS MA_PCT_IND,
ORIG_MA_AMT AS MA_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
INS_UPD_DEL_FLAG AS INS_UPD_DEL_FLAG FROM EXP_INS_UPD_10""")

# COMMAND ----------

# DBTITLE 1, MA_EVENT

spark.sql("""INSERT INTO MA_EVENT SELECT MA_EVENT_ID AS MA_EVENT_ID,
OFFER_ID AS OFFER_ID,
SAP_DEPT_ID AS SAP_DEPT_ID,
PRODUCT_ID AS PRODUCT_ID,
COUNTRY_CD AS COUNTRY_CD,
START_DT AS START_DT,
END_DT AS END_DT,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
LOCATION_ID AS LOCATION_ID,
MOVEMENT_ID AS MOVEMENT_ID,
VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
GL_ACCT_NBR AS GL_ACCT_NBR,
LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
ROYALTY_BRAND_ID AS ROYALTY_BRAND_ID,
BRAND_CD AS BRAND_CD,
MA_FORMULA_CD AS MA_FORMULA_CD,
FISCAL_MO AS FISCAL_MO,
SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
FROM_LOCATION_ID AS FROM_LOCATION_ID,
SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
COMPANY_ID AS COMPANY_ID,
MA_EVENT_DESC AS MA_EVENT_DESC,
EM_VENDOR_FUNDING_ID AS EM_VENDOR_FUNDING_ID,
EM_COMMENT AS EM_COMMENT,
EM_BILL_ALT_VENDOR_FLAG AS EM_BILL_ALT_VENDOR_FLAG,
EM_ALT_VENDOR_ID AS EM_ALT_VENDOR_ID,
EM_ALT_VENDOR_NAME AS EM_ALT_VENDOR_NAME,
EM_ALT_VENDOR_COUNTRY_CD AS EM_ALT_VENDOR_COUNTRY_CD,
EM_VENDOR_ID AS EM_VENDOR_ID,
EM_VENDOR_NAME AS EM_VENDOR_NAME,
EM_VENDOR_COUNTRY_CD AS EM_VENDOR_COUNTRY_CD,
VENDOR_NAME_TXT AS VENDOR_NAME_TXT,
MA_PCT_IND AS MA_PCT_IND,
NEW_MA_AMT AS MA_AMT,
MA_MAX_AMT AS MA_MAX_AMT,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM UPD_STRATEGY_9""")
