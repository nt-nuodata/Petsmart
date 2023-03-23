# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, VENDOR_TYPE_0


df_0=spark.sql("""
    SELECT
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_TYPE_DESC AS VENDOR_TYPE_DESC,
        VENDOR_ACCOUNT_GROUP_CD AS VENDOR_ACCOUNT_GROUP_CD,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_TYPE""")

df_0.createOrReplaceTempView("VENDOR_TYPE_0")

# COMMAND ----------
# DBTITLE 1, VENDOR_RTV_TYPE_1


df_1=spark.sql("""
    SELECT
        RTV_TYPE_CD AS RTV_TYPE_CD,
        RTV_TYPE_DESC AS RTV_TYPE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_RTV_TYPE""")

df_1.createOrReplaceTempView("VENDOR_RTV_TYPE_1")

# COMMAND ----------
# DBTITLE 1, VENDOR_INCO_TERM_2


df_2=spark.sql("""
    SELECT
        INCO_TERM_CD AS INCO_TERM_CD,
        INCO_TERM_DESC AS INCO_TERM_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_INCO_TERM""")

df_2.createOrReplaceTempView("VENDOR_INCO_TERM_2")

# COMMAND ----------
# DBTITLE 1, VENDOR_PAYMENT_TERM_3


df_3=spark.sql("""
    SELECT
        PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        PAYMENT_TERM_DESC AS PAYMENT_TERM_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PAYMENT_TERM""")

df_3.createOrReplaceTempView("VENDOR_PAYMENT_TERM_3")

# COMMAND ----------
# DBTITLE 1, DM_PG_MGR_DVL_4


df_4=spark.sql("""
    SELECT
        MGR_ID AS MGR_ID,
        MGR_DESC AS MGR_DESC,
        DVL_ID AS DVL_ID,
        DVL_DESC AS DVL_DESC,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        DM_PG_MGR_DVL""")

df_4.createOrReplaceTempView("DM_PG_MGR_DVL_4")

# COMMAND ----------
# DBTITLE 1, STATE_5


df_5=spark.sql("""
    SELECT
        STATE_CD AS STATE_CD,
        STATE_NAME AS STATE_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STATE""")

df_5.createOrReplaceTempView("STATE_5")

# COMMAND ----------
# DBTITLE 1, COUNTRY_6


df_6=spark.sql("""
    SELECT
        COUNTRY_CD AS COUNTRY_CD,
        COUNTRY_NAME AS COUNTRY_NAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        COUNTRY""")

df_6.createOrReplaceTempView("COUNTRY_6")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE_7


df_7=spark.sql("""
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

df_7.createOrReplaceTempView("VENDOR_PROFILE_7")

# COMMAND ----------
# DBTITLE 1, PURCH_GROUP_8


df_8=spark.sql("""
    SELECT
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        DVL_MGR_ID AS DVL_MGR_ID,
        REPLEN_MGR_ID AS REPLEN_MGR_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PURCH_GROUP""")

df_8.createOrReplaceTempView("PURCH_GROUP_8")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_VENDOR_PROFILE_9


df_9=spark.sql("""
    SELECT
        VP.VENDOR_ID,
        RTRIM(VP.VENDOR_NAME) AS VENDOR_NAME,
        VP.VENDOR_TYPE_ID,
        RTRIM(VT.VENDOR_TYPE_DESC) AS VENDOR_TYPE_DESC,
        VP.VENDOR_NBR,
        VP.LOCATION_ID,
        VP.Superior_vendor_id,
        RTRIM(VP2.VENDOR_NAME) AS SUPerior_VENDOR_NAME,
        VP2.Vendor_nbr,
        VP.PARENT_VENDOR_ID,
        RTRIM(VP.PARENT_VENDOR_NAME) AS PARENT_VENDOR_NAME,
        VP.PURCH_GROUP_ID,
        RTRIM(PG.PURCH_GROUP_NAME) AS PURCH_GROUP_NAME,
        DPMV.MGR_ID,
        RTRIM(DPMV.MGR_DESC) AS MGR_DESC,
        DPMV.DVL_ID,
        RTRIM(DPMV.DVL_DESC) AS DVL_DESC,
        VP.EDI_ELIG_FLAG,
        vp.purchase_block,
        vp.posting_block,
        vp.deletion_flag,
        vp.vip_cd,
        vp.inactive_flag,
        VP.PAYMENT_TERM_CD,
        RTRIM(VPT.PAYMENT_TERM_DESC) AS PAYMENT_TERM_DESC,
        VP.INCO_TERM_CD,
        RTRIM(VIT.INCO_TERM_DESC) AS INCO_TERM_DESC,
        RTRIM(VP.ADDRESS) AS ADDRESS,
        RTRIM(VP.CITY) AS CITY,
        VP.STATE,
        RTRIM(S.STATE_NAME) AS STATE_NAME,
        VP.COUNTRY_CD,
        RTRIM(CN.COUNTRY_NAME) AS COUNTRY_NAME,
        VP.ZIP,
        RTRIM(VP.CONTACT) AS CONTACT,
        VP.CONTACT_PHONE,
        VP.PHONE,
        VP.PHONE_EXT,
        VP.FAX,
        VP.RTV_ELIG_FLAG,
        VP.RTV_TYPE_CD,
        RTRIM(VRT.RTV_TYPE_DESC) AS RTV_TYPE_DESC,
        VP.RTV_FREIGHT_TYPE_CD,
        VP.INDUSTRY_CD,
        VP.LATITUDE,
        VP.LONGITUDE,
        VP.TIME_ZONE_ID,
        VP.ADD_DT,
        VP.UPDATE_DT,
        VP.LOAD_DT 
    FROM
        VENDOR_PROFILE VP 
    LEFT OUTER JOIN
        VENDOR_TYPE VT 
            ON (
                VP.VENDOR_TYPE_ID = VT.VENDOR_TYPE_ID
            ) 
    LEFT OUTER JOIN
        PURCH_GROUP PG 
            ON (
                VP.PURCH_GROUP_ID = PG.PURCH_GROUP_ID
            ) 
    LEFT OUTER JOIN
        STATE S 
            ON (
                VP.STATE = S.STATE_CD
            ) 
    LEFT OUTER JOIN
        COUNTRY CN 
            ON (
                VP.COUNTRY_CD = CN.COUNTRY_CD
            ) 
    LEFT OUTER JOIN
        VENDOR_INCO_TERM VIT 
            ON (
                VP.INCO_TERM_CD = VIT.INCO_TERM_CD
            ) 
    LEFT OUTER JOIN
        VENDOR_RTV_TYPE VRT 
            ON (
                VP.RTV_TYPE_CD = VRT.RTV_TYPE_CD
            ) 
    LEFT OUTER JOIN
        VENDOR_PAYMENT_TERM VPT 
            ON (
                VP.PAYMENT_TERM_CD = VPT.PAYMENT_TERM_CD
            ) 
    LEFT OUTER JOIN
        DM_PG_MGR_DVL DPMV 
            ON (
                VP.PURCH_GROUP_ID = DPMV.PURCH_GROUP_ID
            ) 
    LEFT OUTER JOIN
        VENDOR_PROFILE VP2 
            ON VP.Superior_vendor_id = VP2.VENDOR_ID""")

df_9.createOrReplaceTempView("SQ_Shortcut_to_VENDOR_PROFILE_9")

# COMMAND ----------
# DBTITLE 1, VENDOR_PROFILE_RPT


spark.sql("""INSERT INTO VENDOR_PROFILE_RPT SELECT VENDOR_ID AS VENDOR_ID,
VENDOR_NAME AS VENDOR_NAME,
VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
VENDOR_TYPE_DESC AS VENDOR_TYPE_DESC,
VENDOR_NBR AS VENDOR_NBR,
LOCATION_ID AS LOCATION_ID,
SUPERIOR_VENDOR_ID AS SUPERIOR_VENDOR_ID,
VENDOR_NAME1 AS SUPERIOR_VENDOR_NAME,
VENDOR_NBR1 AS SUPERIOR_VENDOR_NBR,
PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
PURCH_GROUP_ID AS PURCH_GROUP_ID,
PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
MGR_ID AS MGR_ID,
MGR_DESC AS MGR_DESC,
DVL_ID AS DVL_ID,
DVL_DESC AS DVL_DESC,
EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
PURCHASE_BLOCK AS PURCHASE_BLOCK,
POSTING_BLOCK AS POSTING_BLOCK,
DELETION_FLAG AS DELETION_BLOCK,
VIP_CD AS VIP_CD,
INACTIVE_FLAG AS INACTIVE_FLAG,
PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
PAYMENT_TERM_DESC AS PAYMENT_TERM_DESC,
INCO_TERM_CD AS INCO_TERM_CD,
INCO_TERM_DESC AS INCO_TERM_DESC,
ADDRESS AS ADDRESS,
CITY AS CITY,
STATE AS STATE,
STATE_NAME AS STATE_NAME,
COUNTRY_CD AS COUNTRY_CD,
COUNTRY_NAME AS COUNTRY_NAME,
ZIP AS ZIP,
CONTACT AS CONTACT,
CONTACT_PHONE AS CONTACT_PHONE,
PHONE AS PHONE,
PHONE_EXT AS PHONE_EXT,
FAX AS FAX,
RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
RTV_TYPE_CD AS RTV_TYPE_CD,
RTV_TYPE_DESC AS RTV_TYPE_DESC,
RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
INDUSTRY_CD AS INDUSTRY_CD,
LATITUDE AS LATITUDE,
LONGITUDE AS LONGITUDE,
TIME_ZONE_ID AS TIME_ZONE_ID,
ADD_DT AS ADD_DT,
UPDATE_DT AS UPDATE_DT,
LOAD_DT AS LOAD_DT FROM SQ_Shortcut_to_VENDOR_PROFILE_9""")