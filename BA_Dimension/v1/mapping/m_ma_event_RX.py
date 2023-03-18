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
# DBTITLE 1, MA_EVENT_SOURCE_1


df_1=spark.sql("""
    SELECT
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        MA_EVENT_SOURCE_DESC AS MA_EVENT_SOURCE_DESC,
        TARGET_TABLE AS TARGET_TABLE,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MA_EVENT_SOURCE""")

df_1.createOrReplaceTempView("MA_EVENT_SOURCE_1")

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
# DBTITLE 1, ASQ_MA_EVENT_NEW_3


df_3=spark.sql("""
    SELECT
        sp.product_id,
        c.country_cd,
        CURRENT_DATE - 1 start_dt,
        TO_DATE('12/31/9999',
        'mm/dd/yyyy') end_dt,
        1 ma_event_type_id,
        mes.ma_event_source_id,
        mes.ma_event_source_desc || ' - SKU=' || CAST(sp.sku_nbr AS CHARACTER (7)) || ' - Country=' || c.country_cd ma_event_desc,
        -.25 ma_amt,
        CURRENT_DATE update_dt,
        CURRENT_DATE load_dt,
        'I' ins_upd_flag 
    FROM
        sku_profile sp,
        country c,
        (SELECT
            * 
        FROM
            ma_event_source 
        WHERE
            ma_event_source_id IN (
                4, 5
            )) mes 
    WHERE
        sp.sap_dept_id IN (
            3117, 3007, 18, 28
        ) 
        AND c.country_cd IN (
            'CA', 'US'
        ) 
        AND (
            sp.product_id, c.country_cd, mes.ma_event_source_id
        ) NOT IN (
            SELECT
                e.product_id,
                e.country_cd,
                e.ma_event_source_id 
            FROM
                ma_event e 
            WHERE
                e.ma_event_source_id IN (
                    4, 5
                ) 
                AND e.ma_event_type_id = 1 
                AND e.end_dt = TO_DATE('12/31/9999', 'mm/dd/yyyy')
        )""")

df_3.createOrReplaceTempView("ASQ_MA_EVENT_NEW_3")

# COMMAND ----------
# DBTITLE 1, EXP_NEW_4


df_4=spark.sql("""
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
        ASQ_MA_EVENT_NEW_3.PRODUCT_ID AS PRODUCT_ID,
        ASQ_MA_EVENT_NEW_3.COUNTRY_CD AS COUNTRY_CD,
        ASQ_MA_EVENT_NEW_3.START_DT AS START_DT,
        ASQ_MA_EVENT_NEW_3.END_DT AS END_DT,
        ASQ_MA_EVENT_NEW_3.MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        ASQ_MA_EVENT_NEW_3.MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        ASQ_MA_EVENT_NEW_3.MA_EVENT_DESC AS MA_EVENT_DESC,
        ASQ_MA_EVENT_NEW_3.MA_AMT AS MA_AMT,
        ASQ_MA_EVENT_NEW_3.UPDATE_DT AS UPDATE_DT,
        ASQ_MA_EVENT_NEW_3.LOAD_DT AS LOAD_DT,
        ASQ_MA_EVENT_NEW_3.INS_UPD_FLAG AS INS_UPD_FLAG,
        SEQ_MA_EVENT_ID.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SEQ_MA_EVENT_ID 
    INNER JOIN
        ASQ_MA_EVENT_NEW_3 
            ON SEQ_MA_EVENT_ID.Monotonically_Increasing_Id = ASQ_MA_EVENT_NEW_3.Monotonically_Increasing_Id""")

df_4.createOrReplaceTempView("EXP_NEW_4")

spark.sql("""UPDATE SEQ_MA_EVENT_ID SET CURRVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_NEW_4) , NEXTVAL = (SELECT MAX(MA_EVENT_ID) FROM EXP_NEW_4) + (SELECT Increment_By FROM EXP_NEW_4)""")

# COMMAND ----------
# DBTITLE 1, ASQ_MA_EVENT_EXISTING_5


df_5=spark.sql("""
    SELECT
        e.ma_event_id,
        e.product_id,
        e.country_cd,
        e.start_dt,
        CURRENT_DATE - 1 end_dt,
        e.ma_event_type_id,
        e.ma_event_source_id,
        e.ma_event_desc,
        e.ma_amt,
        CURRENT_DATE update_dt,
        e.load_dt,
        'U' ins_upd_flag 
    FROM
        ma_event e 
    WHERE
        e.ma_event_source_id IN (
            4, 5
        ) 
        AND e.ma_event_type_id = 1 
        AND e.end_dt = TO_DATE('12/31/9999', 'mm/dd/yyyy') 
        AND (
            product_id, country_cd, ma_event_source_id
        ) NOT IN (
            SELECT
                product_id,
                c.country_cd,
                mes.ma_event_source_id 
            FROM
                sku_profile sp,
                country c,
                (SELECT
                    * 
                FROM
                    ma_event_source 
                WHERE
                    ma_event_source_id IN (
                        4, 5
                    )) mes 
            WHERE
                sp.sap_dept_id IN (
                    3117, 3007, 18, 28
                ) 
                AND c.country_cd IN (
                    'CA', 'US'
                )
            )""")

df_5.createOrReplaceTempView("ASQ_MA_EVENT_EXISTING_5")

# COMMAND ----------
# DBTITLE 1, UNI_RX_6


df_6=spark.sql("""SELECT COUNTRY_CD AS COUNTRY_CD,
END_DT AS END_DT,
INS_UPD_FLAG AS INS_UPD_FLAG,
LOAD_DT AS LOAD_DT,
MA_AMT AS MA_AMT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PRODUCT_ID AS PRODUCT_ID,
START_DT AS START_DT,
UPDATE_DT AS UPDATE_DT FROM ASQ_MA_EVENT_EXISTING_5 UNION ALL SELECT COUNTRY_CD AS COUNTRY_CD,
END_DT AS END_DT,
INS_UPD_FLAG AS INS_UPD_FLAG,
LOAD_DT AS LOAD_DT,
MA_AMT AS MA_AMT,
MA_EVENT_DESC AS MA_EVENT_DESC,
MA_EVENT_ID AS MA_EVENT_ID,
MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
PRODUCT_ID AS PRODUCT_ID,
START_DT AS START_DT,
UPDATE_DT AS UPDATE_DT FROM EXP_NEW_4""")

df_6.createOrReplaceTempView("UNI_RX_6")

# COMMAND ----------
# DBTITLE 1, UPD_MA_EVENT_7


df_7=spark.sql("""
    SELECT
        MA_EVENT_ID AS MA_EVENT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        COUNTRY_CD AS COUNTRY_CD,
        START_DT AS START_DT,
        END_DT AS END_DT,
        MA_EVENT_TYPE_ID AS MA_EVENT_TYPE_ID,
        MA_EVENT_SOURCE_ID AS MA_EVENT_SOURCE_ID,
        MA_EVENT_DESC AS MA_EVENT_DESC,
        MA_AMT AS MA_AMT,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        INS_UPD_FLAG AS INS_UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UNI_RX_6""")

df_7.createOrReplaceTempView("UPD_MA_EVENT_7")

# COMMAND ----------
# DBTITLE 1, COUNTRY_8


df_8=spark.sql("""
    SELECT
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        COUNTRY""")

df_8.createOrReplaceTempView("COUNTRY_8")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_9


df_9=spark.sql("""
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

df_9.createOrReplaceTempView("SKU_PROFILE_9")

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
LOAD_DT AS LOAD_DT FROM UPD_MA_EVENT_7""")