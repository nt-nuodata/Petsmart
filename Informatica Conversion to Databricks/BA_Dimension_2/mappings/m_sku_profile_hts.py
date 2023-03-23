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
# DBTITLE 1, ASQ_SKU_PROFILE_1


df_1=spark.sql("""
    SELECT
        HTS_CODE_ID,
        MAX(HTS_CODE_DESC) AS HTS_DESC 
    FROM
        SKU_PROFILE_PRE 
    GROUP BY
        HTS_CODE_ID""")

df_1.createOrReplaceTempView("ASQ_SKU_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, HTS


spark.sql("""INSERT INTO HTS SELECT HTS_CODE_ID AS HTS_CODE_ID,
HTS_DESC AS HTS_CODE_DESC FROM ASQ_SKU_PROFILE_1""")