# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_0

df_0=spark.sql("""
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

df_0.createOrReplaceTempView("SKU_PROFILE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKU_PROFILE_1

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
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_PROFILE_1")

# COMMAND ----------

# DBTITLE 1, Lkp_Brand_2

df_2=spark.sql("""
    SELECT
        BRAND_NAME AS BRAND_NAME,
        SQ_Shortcut_to_SKU_PROFILE_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            BRAND.BRAND_NAME AS BRAND_NAME,
            BRAND.BRAND_CD AS BRAND_CD 
        FROM
            BRAND 
        WHERE
            BRAND.DELETE_FLAG = 0) AS BRAND 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_SKU_PROFILE_1 
            ON BRAND.BRAND_CD = SQ_Shortcut_to_SKU_PROFILE_1.BRAND_CD""")

df_2.createOrReplaceTempView("Lkp_Brand_2")

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
BRAND_NAME AS BRAND_NAME,
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
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM SQ_Shortcut_to_SKU_PROFILE_1""")

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
BRAND_NAME AS BRAND_NAME,
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
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM Lkp_Brand_2""")
