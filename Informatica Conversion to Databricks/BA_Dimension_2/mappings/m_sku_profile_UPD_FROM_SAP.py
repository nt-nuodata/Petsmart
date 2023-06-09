# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, GL_MVKE_PRE_0

df_0=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        SALES_ORG AS SALES_ORG,
        DIST_CHANNEL AS DIST_CHANNEL,
        ACCT_ASSIGNMENT_GRP AS ACCT_ASSIGNMENT_GRP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_MVKE_PRE""")

df_0.createOrReplaceTempView("GL_MVKE_PRE_0")

# COMMAND ----------

# DBTITLE 1, ARTICLE_CATEGORY_1

df_1=spark.sql("""
    SELECT
        ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
        ARTICLE_CATEGORY_CD AS ARTICLE_CATEGORY_CD,
        ARTICLE_CATEGORY_DESC AS ARTICLE_CATEGORY_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ARTICLE_CATEGORY""")

df_1.createOrReplaceTempView("ARTICLE_CATEGORY_1")

# COMMAND ----------

# DBTITLE 1, ZTB_SEL_DISCO_PRE_2

df_2=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
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
        ZTB_SEL_DISCO_PRE""")

df_2.createOrReplaceTempView("ZTB_SEL_DISCO_PRE_2")

# COMMAND ----------

# DBTITLE 1, GL_C003_PRE_3

df_3=spark.sql("""
    SELECT
        SALES_ORG AS SALES_ORG,
        ACCT_ASSIGNMENT_GRP AS ACCT_ASSIGNMENT_GRP,
        GL_ACCT_NBR AS GL_ACCT_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_C003_PRE""")

df_3.createOrReplaceTempView("GL_C003_PRE_3")

# COMMAND ----------

# DBTITLE 1, GL_MARA_PRE_4

df_4=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        ARTICLE_CATEGORY_CD AS ARTICLE_CATEGORY_CD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        GL_MARA_PRE""")

df_4.createOrReplaceTempView("GL_MARA_PRE_4")

# COMMAND ----------

# DBTITLE 1, SHIPPER_DETAIL_5

df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        BOM_PRODUCT_ID AS BOM_PRODUCT_ID,
        BOM_PRODUCT_QTY AS BOM_PRODUCT_QTY,
        UPDATE_DT AS UPDATE_DT,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SHIPPER_DETAIL""")

df_5.createOrReplaceTempView("SHIPPER_DETAIL_5")

# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_6

df_6=spark.sql("""
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

df_6.createOrReplaceTempView("SKU_PROFILE_6")

# COMMAND ----------

# DBTITLE 1, ASQ_SHORTCUT_TO_SKU_PROFILE_7

df_7=spark.sql("""
    SELECT
        SP.PRODUCT_ID,
        NVL(GL.GL_ACCT_NBR,
        CASE 
            WHEN SP.PRIMARY_UPC_ID BETWEEN 41000000000 AND 41999999999 THEN 41020 
            ELSE 41000 
        END) GL_ACCT_NBR,
        NVL(AC.ARTICLE_CATEGORY_ID,
        2) ARTICLE_CATEGORY_ID,
        CASE 
            WHEN S.BOM_PRODUCT_ID IS NULL THEN 'N' 
            ELSE 'Y' 
        END COMPONENT_FLAG,
        CASE 
            WHEN D.SKU_NBR IS NULL THEN SP.ZDISCO_SCHED_TYPE_ID 
            ELSE NVL(D.ZDISCO_SCHED_TYPE_ID,
            'ZZZZZZZZZZ') 
        END ZDISCO_SCHED_TYPE_ID,
        CASE 
            WHEN D.SKU_NBR IS NULL THEN SP.ZDISCO_MKDN_SCHED_ID 
            ELSE NVL(D.ZDISCO_MKDN_SCHED_ID,
            'ZZZZZZ') 
        END ZDISCO_MKDN_SCHED_ID,
        NVL(D.ZDISCO_PID_DT,
        SP.ZDISCO_PID_DT) ZDISCO_PID_DT,
        NVL(D.ZDISCO_START_DT,
        SP.ZDISCO_START_DT) ZDISCO_START_DT,
        NVL(D.ZDISCO_INIT_MKDN_DT,
        SP.ZDISCO_INIT_MKDN_DT) ZDISCO_INIT_MKDN_DT,
        NVL(D.ZDISCO_DC_DT,
        SP.ZDISCO_DC_DT) ZDISCO_DC_DT,
        NVL(D.ZDISCO_STR_DT,
        SP.ZDISCO_STR_DT) ZDISCO_STR_DT,
        NVL(D.ZDISCO_STR_OWNRSHP_DT,
        SP.ZDISCO_STR_OWNRSHP_DT) ZDISCO_STR_OWNRSHP_DT,
        NVL(D.ZDISCO_STR_WRT_OFF_DT,
        SP.ZDISCO_STR_WRT_OFF_DT) ZDISCO_STR_WRT_OFF_DT 
    FROM
        SKU_PROFILE SP 
    LEFT OUTER JOIN
        (
            SELECT
                M.SKU_NBR,
                AC.ARTICLE_CATEGORY_ID 
            FROM
                GL_MARA_PRE M,
                ARTICLE_CATEGORY AC 
            WHERE
                M.ARTICLE_CATEGORY_CD = AC.ARTICLE_CATEGORY_CD
        ) AC 
            ON SP.SKU_NBR = AC.SKU_NBR 
    LEFT OUTER JOIN
        (
            SELECT
                DISTINCT BOM_PRODUCT_ID 
            FROM
                SHIPPER_DETAIL
        ) S 
            ON SP.PRODUCT_ID = S.BOM_PRODUCT_ID 
    LEFT OUTER JOIN
        (
            SELECT
                M.SKU_NBR,
                C.GL_ACCT_NBR 
            FROM
                GL_MVKE_PRE M,
                GL_C003_PRE C 
            WHERE
                M.SALES_ORG = C.SALES_ORG 
                AND M.ACCT_ASSIGNMENT_GRP = C.ACCT_ASSIGNMENT_GRP 
                AND C.SALES_ORG = 'US01' 
                AND M.DIST_CHANNEL = 'D1'
        ) GL 
            ON SP.SKU_NBR = GL.SKU_NBR 
    LEFT OUTER JOIN
        ZTB_SEL_DISCO_PRE D 
            ON SP.SKU_NBR = D.SKU_NBR""")

df_7.createOrReplaceTempView("ASQ_SHORTCUT_TO_SKU_PROFILE_7")

# COMMAND ----------

# DBTITLE 1, UPD_SKU_PROFILE_8

df_8=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
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
        ASQ_SHORTCUT_TO_SKU_PROFILE_7""")

df_8.createOrReplaceTempView("UPD_SKU_PROFILE_8")

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
ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT FROM UPD_SKU_PROFILE_8""")
