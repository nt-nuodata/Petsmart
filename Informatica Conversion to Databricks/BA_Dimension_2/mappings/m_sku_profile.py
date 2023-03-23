# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_PRE_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        DELETE_IND AS DELETE_IND,
        ALT_DESC AS ALT_DESC,
        BUM_QTY AS BUM_QTY,
        BUYER_ID AS BUYER_ID,
        BUYER_NAME AS BUYER_NAME,
        CONTENTS AS CONTENTS,
        CONTENTS_UNITS AS CONTENTS_UNITS,
        ADD_DT AS ADD_DT,
        DELETE_DT AS DELETE_DT,
        HTS_CODE_ID AS HTS_CODE_ID,
        HTS_CODE_DESC AS HTS_CODE_DESC,
        PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        SKU_TYPE AS SKU_TYPE,
        SKU_DESC AS SKU_DESC,
        SKU_NBR AS SKU_NBR,
        PURCH_COST_AMT AS PURCH_COST_AMT,
        WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
        WEIGHT_UNIT_DESC AS WEIGHT_UNIT_DESC,
        SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_DESC AS SAP_CLASS_DESC,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        DISC_START_DT AS DISC_START_DT,
        BRAND_NAME AS BRAND_NAME,
        IMPORT_FLAG AS IMPORT_FLAG,
        STATELINE_FLAG AS STATELINE_FLAG,
        SIZE_DESC AS SIZE_DESC,
        INIT_MKDN_DT AS INIT_MKDN_DT,
        STATUS_ID AS STATUS_ID,
        STATUS_NAME AS STATUS_NAME,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        TAX_CLASS_ID AS TAX_CLASS_ID,
        TAX_CLASS_DESC AS TAX_CLASS_DESC,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        BASE_UOM_CD AS BASE_UOM_CD,
        SIGN_TYPE_CD AS SIGN_TYPE_CD,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
        RTV_DEPT_CD AS RTV_DEPT_CD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_PRE""")

df_0.createOrReplaceTempView("SKU_PROFILE_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_SKU_PROFILE_PRE_1


df_1=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_TYPE AS SKU_TYPE,
        PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
        STATUS_ID AS STATUS_ID,
        SKU_DESC AS SKU_DESC,
        ALT_DESC AS ALT_DESC,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        COUNTRY_CD AS COUNTRY_CD,
        IMPORT_FLAG AS IMPORT_FLAG,
        HTS_CODE_ID AS HTS_CODE_ID,
        CONTENTS AS CONTENTS,
        CONTENTS_UNIT AS CONTENTS_UNIT,
        WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
        WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
        SIZE_DESC AS SIZE_DESC,
        BUM_QTY AS BUM_QTY,
        UOM_CD AS UOM_CD,
        BUYER_ID AS BUYER_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        TAX_CLASS_ID AS TAX_CLASS_ID,
        VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
        BRAND_NAME AS BRAND_NAME,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        STATELINE_FLAG AS STATELINE_FLAG,
        SIGN_TYPE_CD AS SIGN_TYPE_CD,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        DATE_INIT_MKDN AS DATE_INIT_MKDN,
        DATE_DISC_START AS DATE_DISC_START,
        DATE_ADDED AS DATE_ADDED,
        DATE_DELETED AS DATE_DELETED,
        CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
        RTV_DEPT_CD AS RTV_DEPT_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_PRE_0""")

df_1.createOrReplaceTempView("ASQ_Shortcut_To_SKU_PROFILE_PRE_1")

# COMMAND ----------
# DBTITLE 1, LKP_Brand_2


df_2=spark.sql("""
    SELECT
        BRAND_CD AS BRAND_CD,
        BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
        ASQ_Shortcut_To_SKU_PROFILE_PRE_1.SKU_NBR AS SKU_NBR1,
        ASQ_Shortcut_To_SKU_PROFILE_PRE_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_BRAND_PRE 
    RIGHT OUTER JOIN
        ASQ_Shortcut_To_SKU_PROFILE_PRE_1 
            ON SKU_BRAND_PRE.SKU_NBR = ASQ_Shortcut_To_SKU_PROFILE_PRE_1.SKU_NBR""")

df_2.createOrReplaceTempView("LKP_Brand_2")

# COMMAND ----------
# DBTITLE 1, Exp_Brand_3


df_3=spark.sql("""
    SELECT
        IFF(ISNULL(BRAND_CD),
        NULL,
        BRAND_CD) AS BRAND_CD,
        IFF(ISNULL(BRAND_CLASSIFICATION_ID),
        NULL,
        BRAND_CLASSIFICATION_ID) AS BRAND_CLASSIFICATION_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_Brand_2""")

df_3.createOrReplaceTempView("Exp_Brand_3")

# COMMAND ----------
# DBTITLE 1, Exp_conversion_4


df_4=spark.sql("""
    SELECT
        (CAST(BUM_QTY AS DECIMAL (38,
        0))) AS BUM_QTY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_To_SKU_PROFILE_PRE_1""")

df_4.createOrReplaceTempView("Exp_conversion_4")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_DT_5


df_5=spark.sql("""
    SELECT
        date_trunc('DAY',
        current_timestamp) AS UPDATE_DT,
        current_timestamp AS LOAD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_Shortcut_To_SKU_PROFILE_PRE_1""")

df_5.createOrReplaceTempView("EXP_LOAD_DT_5")

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
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM ASQ_Shortcut_To_SKU_PROFILE_PRE_1""")

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
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM Exp_conversion_4""")

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
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM Exp_Brand_3""")

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
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM EXP_LOAD_DT_5""")