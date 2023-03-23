# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, VENDOR_PROFILE_0

df_0=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_NBR AS VENDOR_NBR,
        LOCATION_ID AS LOCATION_ID,
        SUPERIOR_VENDOR_ID AS SUPERIOR_VENDOR_ID,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
        PURCHASE_BLOCK AS PURCHASE_BLOCK,
        POSTING_BLOCK AS POSTING_BLOCK,
        DELETION_FLAG AS DELETION_FLAG,
        VIP_CD AS VIP_CD,
        INACTIVE_FLAG AS INACTIVE_FLAG,
        PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        INCO_TERM_CD AS INCO_TERM_CD,
        ADDRESS AS ADDRESS,
        CITY AS CITY,
        STATE AS STATE,
        COUNTRY_CD AS COUNTRY_CD,
        ZIP AS ZIP,
        CONTACT AS CONTACT,
        CONTACT_PHONE AS CONTACT_PHONE,
        PHONE AS PHONE,
        PHONE_EXT AS PHONE_EXT,
        FAX AS FAX,
        RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
        RTV_TYPE_CD AS RTV_TYPE_CD,
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
        VENDOR_PROFILE""")

df_0.createOrReplaceTempView("VENDOR_PROFILE_0")

# COMMAND ----------

# DBTITLE 1, ASQ_Vendor_Profile_In_1

df_1=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        VENDOR_NAME AS VENDOR_NAME,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PROFILE_0""")

df_1.createOrReplaceTempView("ASQ_Vendor_Profile_In_1")

# COMMAND ----------

# DBTITLE 1, PRIMARY_VENDOR

spark.sql("""INSERT INTO PRIMARY_VENDOR SELECT VENDOR_ID AS PRIMARY_VENDOR_ID,
VENDOR_NAME AS PRIMARY_VENDOR_NAME FROM ASQ_Vendor_Profile_In_1""")
