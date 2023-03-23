# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SKU_SUBSTITUTION_0

df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SUBS_EFF_DT AS SUBS_EFF_DT,
        SUBS_PRODUCT_ID AS SUBS_PRODUCT_ID,
        SUBS_END_DT AS SUBS_END_DT,
        EXPIRY_FLAG AS EXPIRY_FLAG,
        ADD_DT AS ADD_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_SUBSTITUTION""")

df_0.createOrReplaceTempView("SKU_SUBSTITUTION_0")

# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_1

df_1=spark.sql("""
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

df_1.createOrReplaceTempView("SKU_PROFILE_1")

# COMMAND ----------

# DBTITLE 1, ASQ_Shortcut_To_SKU_SUBSTITUTION_2

df_2=spark.sql("""
    SELECT
        PRODUCT_ID,
        SUBS_HIST_FLAG,
        SUBS_CURR_FLAG 
    FROM
        (SELECT
            SSL.PRODUCT_ID,
            CASE 
                WHEN MAX(SSL.SS_CD) = 'S' 
                AND MAX(SSL.SL_CD) = 'D' THEN 'B' 
                WHEN MAX(SSL.SS_CD) = 'S' 
                AND MAX(SSL.SL_CD) = ' ' THEN 'S' 
                WHEN MAX(SSL.SS_CD) = ' ' 
                AND MAX(SSL.SL_CD) = 'D' THEN 'D' 
            END AS SOURCE_CD,
            MAX(SUBS_EFF_DT),
            MAX(SUBS_END_DT),
            CASE 
                WHEN CURRENT_DATE BETWEEN MAX(SUBS_EFF_DT) AND MAX(SUBS_END_DT) THEN 'Y' 
                ELSE 'N' 
            END AS SUBS_CURR_FLAG,
            MAX(SUBS_EFF_DT),
            MAX(SUBS_END_DT),
            CASE 
                WHEN MAX(SP1.SUBS_HIST_FLAG) = 'N' 
                AND CURRENT_DATE BETWEEN MAX(SUBS_EFF_DT) AND MAX(SUBS_END_DT) 
                OR MAX(SP2.SUBS_HIST_FLAG) = 'N' 
                AND CURRENT_DATE BETWEEN MAX(SUBS_EFF_DT) AND MAX(SUBS_END_DT) 
                OR CURRENT_DATE > MAX(SUBS_END_DT) THEN 'Y' 
                WHEN MAX(SP1.SUBS_HIST_FLAG) = 'Y' THEN 'Y' 
                WHEN MAX(SP2.SUBS_HIST_FLAG) = 'Y' THEN 'Y' 
                WHEN CURRENT_DATE BETWEEN MAX(SUBS_EFF_DT) AND MAX(SUBS_END_DT) THEN 'Y' 
                ELSE 'N' 
            END AS SUBS_HIST_FLAG 
        FROM
            (SELECT
                SS.PRODUCT_ID,
                SS.SUBS_PRODUCT_ID,
                'S' AS SS_CD,
                ' ' AS SL_CD,
                SUBS_EFF_DT,
                SUBS_END_DT 
            FROM
                SKU_SUBSTITUTION SS 
            UNION
            ALL SELECT
                SL.PRODUCT_ID,
                SL.LINK_PRODUCT_ID,
                ' ' AS SS_CD,
                'D' AS SL_CD,
                SKU_LINK_EFF_DT,
                SKU_LINK_END_DT 
            FROM
                DP_SKU_LINK SL 
            GROUP BY
                SL.PRODUCT_ID,
                SL.LINK_PRODUCT_ID,
                SKU_LINK_EFF_DT,
                SKU_LINK_END_DT 
            UNION
            ALL SELECT
                SS.SUBS_PRODUCT_ID,
                SS.PRODUCT_ID,
                'S' AS SS_CD,
                ' ' AS SL_CD,
                SUBS_EFF_DT,
                SUBS_END_DT 
            FROM
                SKU_SUBSTITUTION SS 
            UNION
            ALL SELECT
                SL.LINK_PRODUCT_ID,
                SL.PRODUCT_ID,
                ' ' AS SS_CD,
                'D' AS SL_CD,
                SKU_LINK_EFF_DT,
                SKU_LINK_END_DT 
            FROM
                DP_SKU_LINK SL 
            GROUP BY
                SL.LINK_PRODUCT_ID,
                SL.PRODUCT_ID,
                SKU_LINK_EFF_DT,
                SKU_LINK_END_DT
        ) SSL, SKU_PROFILE SP1, SKU_PROFILE SP2 
    WHERE
        SSL.PRODUCT_ID = SP1.PRODUCT_ID 
        AND SSL.SUBS_PRODUCT_ID = SP2.PRODUCT_ID 
    GROUP BY
        SSL.PRODUCT_ID
) ADVENTNET_ALIAS1""")

df_2.createOrReplaceTempView("ASQ_Shortcut_To_SKU_SUBSTITUTION_2")

# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_RPT

spark.sql("""INSERT INTO SKU_PROFILE_RPT SELECT PRODUCT_ID AS PRODUCT_ID,
SKU_NBR AS SKU_NBR,
SKU_TYPE AS SKU_TYPE,
PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
STATUS_ID AS STATUS_ID,
STATUS_NAME AS STATUS_NAME,
SUBS_HIST_FLAG AS SUBS_HIST_FLAG,
SUBS_CURR_FLAG AS SUBS_CURR_FLAG,
SKU_DESC AS SKU_DESC,
ALT_DESC AS ALT_DESC,
SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
GL_CATEGORY_CD AS GL_CATEGORY_CD,
GL_CATEGORY_DESC AS GL_CATEGORY_DESC,
BUS_UNIT_ID AS BUS_UNIT_ID,
BUS_UNIT_DESC AS BUS_UNIT_DESC,
SVP_ID AS SVP_ID,
SVP_DESC AS SVP_DESC,
VP_ID AS VP_ID,
VP_NM AS VP_NM,
VP_DESC AS VP_DESC,
CATEGORY_BUYER_ID AS CATEGORY_BUYER_ID,
CATEGORY_BUYER_NM AS CATEGORY_BUYER_NM,
CA_BUYER_ID AS CA_BUYER_ID,
CA_BUYER_NM AS CA_BUYER_NM,
DIRECTOR_ID AS DIRECTOR_ID,
DIRECTOR_NM AS DIRECTOR_DESC,
CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
CA_DIRECTOR_NM AS CA_DIRECTOR_DESC,
PRICING_ROLE_ID AS PRICING_ROLE_ID,
PRICING_ROLE_DESC AS PRICING_ROLE_DESC,
SAP_CLASS_ID AS SAP_CLASS_ID,
SAP_CLASS_DESC AS SAP_CLASS_DESC,
SAP_DEPT_ID AS SAP_DEPT_ID,
SAP_DEPT_DESC AS SAP_DEPT_DESC,
CONSUM_ID AS CONSUM_ID,
CONSUM_DESC AS CONSUM_DESC,
SEGMENT_ID AS SEGMENT_ID,
SEGMENT_DESC AS SEGMENT_DESC,
SAP_DIVISION_ID AS SAP_DIVISION_ID,
SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
PURCH_GROUP_ID AS PURCH_GROUP_ID,
PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
COUNTRY_CD AS COUNTRY_CD,
COUNTRY_NAME AS COUNTRY_NAME,
IMPORT_FLAG AS IMPORT_FLAG,
IMPORT_DESC AS IMPORT_DESC,
HTS_CODE_ID AS HTS_CODE_ID,
HTS_CODE_DESC AS HTS_CODE_DESC,
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
BUYER_NAME AS BUYER_NAME,
PURCH_COST_AMT AS PURCH_COST_AMT,
NAT_PRICE_US_AMT AS NAT_PRICE_US_AMT,
TAX_CLASS_ID AS TAX_CLASS_ID,
TAX_CLASS_DESC AS TAX_CLASS_DESC,
VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
VALUATION_CLASS_DESC AS VALUATION_CLASS_DESC,
BRAND_CD AS BRAND_CD,
BRAND_NAME AS BRAND_NAME,
BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
BRAND_CLASSIFICATION_NAME AS BRAND_CLASSIFICATION_NAME,
OWNBRAND_FLAG AS OWNBRAND_FLAG,
OWNBRAND_DESC AS OWNBRAND_DESC,
STATELINE_FLAG AS STATELINE_FLAG,
SIGN_TYPE_CD AS SIGN_TYPE_CD,
SIGN_TYPE_DESC AS SIGN_TYPE_DESC,
OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
INIT_MKDN_DT AS INIT_MKDN_DT,
DISC_START_DT AS DISC_START_DT,
ADD_DT AS ADD_DT,
DELETE_DT AS DELETE_DT,
FIRST_SALE_DT AS FIRST_SALE_DT,
LAST_SALE_DT AS LAST_SALE_DT,
FIRST_INV_DT AS FIRST_INV_DT,
LAST_INV_DT AS LAST_INV_DT,
BASE_NBR AS BASE_NBR,
BP_COLOR_ID AS BP_COLOR_ID,
BP_SIZE_ID AS BP_SIZE_ID,
BP_BREED_ID AS BP_BREED_ID,
BP_ITEM_CONCATENATED AS BP_ITEM_CONCATENATED,
CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
NAT_PRICE_CA_AMT AS NAT_PRICE_CA_AMT,
NAT_PRICE_PR_AMT AS NAT_PRICE_PR_AMT,
RTV_DEPT_CD AS RTV_DEPT_CD,
RTV_DESC AS RTV_DESC,
HAZ_FLAG AS HAZ_FLAG,
AEROSOL_FLAG AS AEROSOL_FLAG,
GL_ACCT_NBR AS GL_ACCT_NBR,
ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
ARTICLE_CATEGORY_DESC AS ARTICLE_CATEGORY_DESC,
ARTICLE_CATEGORY_CD AS ARTICLE_CATEGORY_CD,
COMPONENT_FLAG AS COMPONENT_FLAG,
ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
ZDISCO_SCHED_TYPE_DESC AS ZDISCO_SCHED_TYPE_DESC,
ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
ZDISCO_MKDN_SCHED_DESC AS ZDISCO_MKDN_SCHED_DESC,
ZDISCO_PID_DT AS ZDISCO_PID_DT,
ZDISCO_START_DT AS ZDISCO_START_DT,
ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
ZDISCO_DC_DT AS ZDISCO_DC_DT,
ZDISCO_STR_DT AS ZDISCO_STR_DT,
ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT,
SAP_DEMAND_GROUP AS OPT_DEMAND_GROUP,
SAP_PRODUCT_LINE AS OPT_PRODUCT_LINE,
SAP_PRICE_FAMILY AS OPT_PRICE_FAMILY,
EFFECTIVE_SIZE AS OPT_EFFECTIVE_SIZE,
SELL_UNITS AS OPT_SELL_UNITS,
UNIT_PRICE AS OPT_UNIT_PRICE,
PRICE AS OPT_MOD_PRICE,
COST AS OPT_MOD_COST,
IS_NEW AS OPT_IS_NEW,
ERROR_CODE AS OPT_ERROR_CODE,
RELATIONSHIP AS OPT_RELATIONSHIP,
METRIC AS OPT_METRIC,
DIFF AS OPT_DIFF,
ASSOC_PROD AS OPT_ASSOC_PROD,
DELETE_OPTION AS OPT_DELETE_OPTION,
DISC_SKU_NBR AS DISC_SKU_NBR,
AVG_SALES AS AVG_SALES,
COPY_SKU_NBR AS COPY_SKU_NBR,
COPY_SKU_PCT AS COPY_SKU_PCT,
PROCUREMENT_RULE_CD AS PROCUREMENT_RULE_CD,
PROCUREMENT_RULE_DESC AS PROCUREMENT_RULE_DESC,
CATEGORY_ANALYST_ID AS CATEGORY_ANALYST_ID,
CATEGORY_ANALYST_NM AS CATEGORY_ANALYST_NM,
CATEGORY_REPLENISHMENT_MGR_ID AS CATEGORY_REPLENISHMENT_MGR_ID,
CATEGORY_REPLENISHMENT_MGR_NM AS CATEGORY_REPLENISHMENT_MGR_NM,
CREATED_BY AS CREATED_BY,
SHELF_LIFE_REM_CNT AS SHELF_LIFE_REM_CNT,
TEMP_SKU AS TEMP_SKU,
CONTENTS_METRIC_AMT AS CONTENTS_METRICS,
CONTENTS_UNITS_METRIC_CD AS CONTENTS_UNITS_METRICS,
FLAVOR_CD AS FLAVOR_CD,
FLAVOR_DESC AS FLAVOR_DESC,
MATERIAL AS BASIC_MATERIAL,
PACKAGING_CD AS CONTAINER_CD,
PACKAGING_DEC AS CONTAINER_DESC,
RX_FLAG AS RX_FLAG,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM ASQ_Shortcut_To_SKU_SUBSTITUTION_2""")

# COMMAND ----------

# DBTITLE 1, SKU_PROFILE

spark.sql("""INSERT INTO SKU_PROFILE SELECT PRODUCT_ID AS PRODUCT_ID,
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
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM ASQ_Shortcut_To_SKU_SUBSTITUTION_2""")
