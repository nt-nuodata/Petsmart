# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, LISTING_DAY_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        LISTING_END_DT AS LISTING_END_DT,
        LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
        LISTING_EFF_DT AS LISTING_EFF_DT,
        LISTING_MODULE_ID AS LISTING_MODULE_ID,
        LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
        NEGATE_FLAG AS NEGATE_FLAG,
        STRUCT_COMP_CD AS STRUCT_COMP_CD,
        STRUCT_ARTICLE_NBR AS STRUCT_ARTICLE_NBR,
        DELETE_IND AS DELETE_IND,
        LOAD_DT AS LOAD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LISTING_DAY""")

df_0.createOrReplaceTempView("LISTING_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_LISTING_DAY_1


df_1=spark.sql("""
    SELECT
        'M_SOURCE_VENDOR'""")

df_1.createOrReplaceTempView("SQ_Shortcut_To_LISTING_DAY_1")

# COMMAND ----------
# DBTITLE 1, EXP_VENDER_PRE_2


df_2=spark.sql("""
    SELECT
        0 AS SKU_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_LISTING_DAY_1""")

df_2.createOrReplaceTempView("EXP_VENDER_PRE_2")

# COMMAND ----------
# DBTITLE 1, FILTRANS_3


df_3=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_VENDER_PRE_2 
    WHERE
        SKU_NBR <> 0""")

df_3.createOrReplaceTempView("FILTRANS_3")

# COMMAND ----------
# DBTITLE 1, SOURCE_VENDOR_PRE


spark.sql("""INSERT INTO SOURCE_VENDOR_PRE SELECT SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
VENDOR_ID AS VENDOR_ID,
UNIT_NUMERATOR AS UNIT_NUMERATOR,
UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
DELIV_EFF_DT AS DELIV_EFF_DT,
DELIV_END_DT AS DELIV_END_DT,
VENDOR_TYPE AS VENDOR_TYPE FROM FILTRANS_3""")