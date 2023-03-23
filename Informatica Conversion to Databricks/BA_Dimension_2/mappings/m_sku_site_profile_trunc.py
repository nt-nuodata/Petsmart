# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_SITE_PROFILE_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        COUNTRY_CD AS COUNTRY_CD,
        LAST_SALE_DT AS LAST_SALE_DT,
        COND_UNIT AS COND_UNIT,
        HIST_SALE_FLAG AS HIST_SALE_FLAG,
        HIST_INV_FLAG AS HIST_INV_FLAG,
        CURR_ORDER_FLAG AS CURR_ORDER_FLAG,
        CURR_SAP_LISTED_FLAG AS CURR_SAP_LISTED_FLAG,
        CURR_POG_LISTED_FLAG AS CURR_POG_LISTED_FLAG,
        CURR_CATALOG_FLAG AS CURR_CATALOG_FLAG,
        CURR_DOTCOM_FLAG AS CURR_DOTCOM_FLAG,
        PROJ_ORDER_FLAG AS PROJ_ORDER_FLAG,
        AD_PRICE_AMT AS AD_PRICE_AMT,
        REGULAR_PRICE_AMT AS REGULAR_PRICE_AMT,
        NAT_PRICE_AMT AS NAT_PRICE_AMT,
        PETPERKS_PRICE_AMT AS PETPERKS_PRICE_AMT,
        LOC_AD_PRICE_AMT AS LOC_AD_PRICE_AMT,
        LOC_REGULAR_PRICE_AMT AS LOC_REGULAR_PRICE_AMT,
        LOC_NAT_PRICE_AMT AS LOC_NAT_PRICE_AMT,
        LOC_PETPERKS_PRICE_AMT AS LOC_PETPERKS_PRICE_AMT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_SITE_PROFILE""")

df_0.createOrReplaceTempView("SKU_SITE_PROFILE_0")

# COMMAND ----------
# DBTITLE 1, WEEKS_1


df_1=spark.sql("""
    SELECT
        WEEK_DT AS WEEK_DT,
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
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        WEEKS""")

df_1.createOrReplaceTempView("WEEKS_1")

# COMMAND ----------
# DBTITLE 1, ASQ_DUMMY_TARGET_2


df_2=spark.sql("""
    SELECT
        'SKU_SITE_PROFILE'""")

df_2.createOrReplaceTempView("ASQ_DUMMY_TARGET_2")

# COMMAND ----------
# DBTITLE 1, FLT_Trunc_Prod_Table_3


df_3=spark.sql("""
    SELECT
        TABLE_NAME AS TABLE_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ASQ_DUMMY_TARGET_2 
    WHERE
        FALSE""")

df_3.createOrReplaceTempView("FLT_Trunc_Prod_Table_3")

# COMMAND ----------
# DBTITLE 1, SKU_SITE_PROFILE


spark.sql("""INSERT INTO SKU_SITE_PROFILE SELECT TABLE_NAME AS PRODUCT_ID,
LOCATION_ID AS LOCATION_ID,
COUNTRY_CD AS COUNTRY_CD,
LAST_SALE_DT AS LAST_SALE_DT,
COND_UNIT AS COND_UNIT,
HIST_SALE_FLAG AS HIST_SALE_FLAG,
HIST_INV_FLAG AS HIST_INV_FLAG,
CURR_ORDER_FLAG AS CURR_ORDER_FLAG,
CURR_SAP_LISTED_FLAG AS CURR_SAP_LISTED_FLAG,
CURR_POG_LISTED_FLAG AS CURR_POG_LISTED_FLAG,
CURR_CATALOG_FLAG AS CURR_CATALOG_FLAG,
CURR_DOTCOM_FLAG AS CURR_DOTCOM_FLAG,
PROJ_ORDER_FLAG AS PROJ_ORDER_FLAG,
AD_PRICE_AMT AS AD_PRICE_AMT,
REGULAR_PRICE_AMT AS REGULAR_PRICE_AMT,
NAT_PRICE_AMT AS NAT_PRICE_AMT,
PETPERKS_PRICE_AMT AS PETPERKS_PRICE_AMT,
LOC_AD_PRICE_AMT AS LOC_AD_PRICE_AMT,
LOC_REGULAR_PRICE_AMT AS LOC_REGULAR_PRICE_AMT,
LOC_NAT_PRICE_AMT AS LOC_NAT_PRICE_AMT,
LOC_PETPERKS_PRICE_AMT AS LOC_PETPERKS_PRICE_AMT FROM FLT_Trunc_Prod_Table_3""")

# COMMAND ----------
# DBTITLE 1, DUMMY_TARGET


spark.sql("""INSERT INTO DUMMY_TARGET SELECT TABLE_NAME AS COMMENT FROM ASQ_DUMMY_TARGET_2""")