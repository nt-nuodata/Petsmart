# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SEQ

spark.sql("""CREATE TABLE SEQ(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int);""")

spark.sql("""INSERT INTO SEQ(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int) VALUES(1000001, 1000000, 1)""")

# COMMAND ----------

# DBTITLE 1, VENDOR_PROFILE_PRE_1

df_1=spark.sql("""
    SELECT
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_NBR AS VENDOR_NBR,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_GROUP AS VENDOR_GROUP,
        SITE AS SITE,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        SUPERIOR_VENDOR_NBR AS SUPERIOR_VENDOR_NBR,
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
        FAX AS FAX,
        RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
        RTV_TYPE_CD AS RTV_TYPE_CD,
        RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
        INDUSTRY_CD AS INDUSTRY_CD,
        ADD_DT AS ADD_DT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PROFILE_PRE""")

df_1.createOrReplaceTempView("VENDOR_PROFILE_PRE_1")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_VENDOR_PROFILE_PRE1_2

df_2=spark.sql("""
    SELECT
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_NBR AS VENDOR_NBR,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_GROUP AS VENDOR_GROUP,
        SITE AS SITE,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        SUPERIOR_VENDOR_NBR AS SUPERIOR_VENDOR_NBR,
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
        FAX AS FAX,
        RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
        RTV_TYPE_CD AS RTV_TYPE_CD,
        RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
        INDUSTRY_CD AS INDUSTRY_CD,
        ADD_DT AS ADD_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PROFILE_PRE_1""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_VENDOR_PROFILE_PRE1_2")

# COMMAND ----------

# DBTITLE 1, EXP_SQ_3

df_3=spark.sql("""
    SELECT
        VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        VENDOR_NBR AS VENDOR_NBR,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_GROUP AS VENDOR_GROUP,
        SITE AS SITE,
        (CAST(LTRIM(SITE,
        '0') AS DECIMAL (38,
        0))) AS STORE_NBR,
        PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        SUPERIOR_VENDOR_ID AS SUPERIOR_VENDOR_ID,
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
        FAX AS FAX,
        RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
        RTV_TYPE_CD AS RTV_TYPE_CD,
        RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
        INDUSTRY_CD AS INDUSTRY_CD,
        ADD_DT AS ADD_DT,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_VENDOR_PROFILE_PRE1_2""")

df_3.createOrReplaceTempView("EXP_SQ_3")

# COMMAND ----------

# DBTITLE 1, LKP_SITE_PROFILE_4

df_4=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
        GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
        TIME_ZONE_ID AS TIME_ZONE_ID,
        STORE_NAME AS STORE_NAME,
        CLOSE_DT AS CLOSE_DT,
        EXP_SQ_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE 
    RIGHT OUTER JOIN
        EXP_SQ_3 
            ON SITE_PROFILE.STORE_NBR = EXP_SQ_3.STORE_NBR""")

df_4.createOrReplaceTempView("LKP_SITE_PROFILE_4")

# COMMAND ----------

# DBTITLE 1, EXP_FINAL_5

df_5=spark.sql("""
    SELECT
        EXP_SQ_3.VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
        EXP_SQ_3.VENDOR_NBR AS VENDOR_NBR,
        EXP_SQ_3.VENDOR_NAME AS VENDOR_NAME,
        EXP_SQ_3.VENDOR_GROUP AS VENDOR_GROUP,
        EXP_SQ_3.PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
        EXP_SQ_3.EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
        EXP_SQ_3.PURCHASE_BLOCK AS PURCHASE_BLOCK,
        EXP_SQ_3.POSTING_BLOCK AS POSTING_BLOCK,
        EXP_SQ_3.DELETION_FLAG AS DELETION_FLAG,
        EXP_SQ_3.VIP_CD AS VIP_CD,
        EXP_SQ_3.INACTIVE_FLAG AS INACTIVE_FLAG,
        EXP_SQ_3.PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
        EXP_SQ_3.INCO_TERM_CD AS INCO_TERM_CD,
        EXP_SQ_3.ADDRESS AS ADDRESS,
        EXP_SQ_3.CITY AS CITY,
        EXP_SQ_3.STATE AS STATE,
        EXP_SQ_3.COUNTRY_CD AS COUNTRY_CD,
        EXP_SQ_3.ZIP AS ZIP,
        EXP_SQ_3.CONTACT AS CONTACT,
        EXP_SQ_3.CONTACT_PHONE AS CONTACT_PHONE,
        EXP_SQ_3.PHONE AS PHONE,
        EXP_SQ_3.FAX AS FAX,
        EXP_SQ_3.RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
        EXP_SQ_3.RTV_TYPE_CD AS RTV_TYPE_CD,
        EXP_SQ_3.RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
        EXP_SQ_3.INDUSTRY_CD AS INDUSTRY_CD,
        EXP_SQ_3.ADD_DT AS ADD_DT,
        LKP_SITE_PROFILE_4.LOCATION_ID AS LOCATION_ID,
        LKP_SITE_PROFILE_4.STORE_TYPE_ID AS STORE_TYPE_ID,
        LKP_SITE_PROFILE_4.GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
        LKP_SITE_PROFILE_4.GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
        EXP_SQ_3.STORE_NBR AS STORE_NBR,
        LKP_SITE_PROFILE_4.TIME_ZONE_ID AS TIME_ZONE_ID,
        LKP_SITE_PROFILE_4.STORE_NAME AS STORE_NAME,
        LKP_SITE_PROFILE_4.CLOSE_DT AS CLOSE_DT,
        EXP_SQ_3.PURCH_GROUP_ID AS PURCH_GROUP_ID,
        EXP_SQ_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_SQ_3 
    INNER JOIN
        LKP_SITE_PROFILE_4 
            ON EXP_SQ_3.Monotonically_Increasing_Id = LKP_SITE_PROFILE_4.Monotonically_Increasing_Id""")

df_5.createOrReplaceTempView("EXP_FINAL_5")

# COMMAND ----------

# DBTITLE 1, LKP_VENDOR_PROFILE_6

df_6=spark.sql("""
    SELECT
        VENDOR_ID AS VENDOR_ID,
        EXP_FINAL_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        VENDOR_PROFILE 
    RIGHT OUTER JOIN
        EXP_FINAL_5 
            ON VENDOR_PROFILE.VENDOR_TYPE_ID = EXP_FINAL_5.VENDOR_TYPE_ID 
            AND VENDOR_PROFILE.VENDOR_NBR = VENDOR_NBR1""")

df_6.createOrReplaceTempView("LKP_VENDOR_PROFILE_6")

# COMMAND ----------

# DBTITLE 1, EXP_DC_7

df_7=spark.sql("""SELECT STORE_NBR4 AS STORE_NBR4,
STORE_NBR4 + 900000 AS VENDOR_ID4,
STORE_NAME4 AS STORE_NAME4,
STORE_NAME4 AS VENDOR_NAME,
10 AS VENDOR_TYPE_ID4,
'V' || TO_CHAR(LPAD(STORE_NBR4,4,0)) AS VENDOR_NBR4,
LOCATION_ID4 AS LOCATION_ID4,
PARENT_VENDOR_ID4 AS PARENT_VENDOR_ID4,
'N' AS EDI_ELIG_FLAG4,
ADDRESS4 AS ADDRESS4,
CITY4 AS CITY4,
STATE4 AS STATE4,
COUNTRY_CD4 AS COUNTRY_CD4,
ZIP4 AS ZIP4,
PHONE4 AS PHONE4,
GEO_LATITUDE_NBR4 AS GEO_LATITUDE_NBR4,
GEO_LONGITUDE_NBR4 AS GEO_LONGITUDE_NBR4,
ADD_DT4 AS ADD_DT4,
TIME_ZONE_ID4 AS TIME_ZONE_ID4,
CLOSE_DT4 AS CLOSE_DT4,
SESSSTARTTIME AS LOAD_DT,
PURCH_GROUP_ID4 AS PURCH_GROUP_ID4,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_7.createOrReplaceTempView("EXP_DC_7")

# COMMAND ----------

# DBTITLE 1, UPDATE_DC_INSERT_8

df_8=spark.sql("""
    SELECT
        VENDOR_ID4 AS VENDOR_ID4,
        VENDOR_NAME AS VENDOR_NAME,
        VENDOR_TYPE_ID4 AS VENDOR_TYPE_ID4,
        VENDOR_NBR4 AS VENDOR_NBR4,
        LOCATION_ID4 AS LOCATION_ID4,
        PARENT_VENDOR_ID4 AS PARENT_VENDOR_ID4,
        EDI_ELIG_FLAG4 AS EDI_ELIG_FLAG4,
        ADDRESS4 AS ADDRESS4,
        CITY4 AS CITY4,
        STATE4 AS STATE4,
        COUNTRY_CD4 AS COUNTRY_CD4,
        ZIP4 AS ZIP4,
        PHONE4 AS PHONE4,
        GEO_LATITUDE_NBR4 AS GEO_LATITUDE_NBR4,
        GEO_LONGITUDE_NBR4 AS GEO_LONGITUDE_NBR4,
        ADD_DT4 AS ADD_DT4,
        TIME_ZONE_ID4 AS TIME_ZONE_ID4,
        CLOSE_DT4 AS CLOSE_DT4,
        LOAD_DT AS LOAD_DT,
        PURCH_GROUP_ID4 AS PURCH_GROUP_ID4,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_DC_7""")

df_8.createOrReplaceTempView("UPDATE_DC_INSERT_8")

# COMMAND ----------

# DBTITLE 1, EXP_MERCH_9

df_9=spark.sql("""SELECT VENDOR_NBR1 AS VENDOR_NBR1,
TO_INTEGER(VENDOR_NBR1) AS VENDOR_ID,
LKP_VENDOR_ID1 AS LKP_VENDOR_ID1,
VENDOR_TYPE_ID1 AS VENDOR_TYPE_ID1,
STORE_TYPE_ID1 AS STORE_TYPE_ID1,
VENDOR_NAME1 AS VENDOR_NAME1,
VENDOR_GROUP1 AS VENDOR_GROUP1,
PARENT_VENDOR_ID1 AS PARENT_VENDOR_ID1,
EDI_ELIG_FLAG1 AS EDI_ELIG_FLAG1,
PURCHASE_BLOCK1 AS PURCHASE_BLOCK1,
POSTING_BLOCK1 AS POSTING_BLOCK1,
DELETION_FLAG AS DELETION_FLAG,
VIP_CD AS VIP_CD,
INACTIVE_FLAG1 AS INACTIVE_FLAG1,
PAYMENT_TERM_CD1 AS PAYMENT_TERM_CD1,
INCO_TERM_CD1 AS INCO_TERM_CD1,
ADDRESS1 AS ADDRESS1,
CITY1 AS CITY1,
STATE1 AS STATE1,
COUNTRY_CD1 AS COUNTRY_CD1,
ZIP1 AS ZIP1,
CONTACT1 AS CONTACT1,
CONTACT_PHONE1 AS CONTACT_PHONE1,
PHONE1 AS PHONE1,
FAX1 AS FAX1,
RTV_ELIG_FLAG1 AS RTV_ELIG_FLAG1,
RTV_TYPE_CD1 AS RTV_TYPE_CD1,
RTV_FREIGHT_TYPE_CD1 AS RTV_FREIGHT_TYPE_CD1,
INDUSTRY_CD1 AS INDUSTRY_CD1,
ADD_DT1 AS ADD_DT1,
LOCATION_ID1 AS LOCATION_ID1,
SESSSTARTTIME AS LOAD_DT,
PURCH_GROUP_ID1 AS PURCH_GROUP_ID1,
IIF (ISNULL(VENDOR_ID1), 0,1) AS UpdateStrategy,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_9.createOrReplaceTempView("EXP_MERCH_9")

# COMMAND ----------

# DBTITLE 1, UPD_INS_UPD_10

df_10=spark.sql("""
    SELECT
        LKP_VENDOR_ID1 AS LKP_VENDOR_ID1,
        VENDOR_NBR1 AS VENDOR_NBR1,
        VENDOR_ID AS VENDOR_ID,
        VENDOR_TYPE_ID1 AS VENDOR_TYPE_ID1,
        VENDOR_NAME1 AS VENDOR_NAME1,
        EDI_ELIG_FLAG1 AS EDI_ELIG_FLAG1,
        PURCHASE_BLOCK1 AS PURCHASE_BLOCK1,
        POSTING_BLOCK1 AS POSTING_BLOCK1,
        DELETION_FLAG AS DELETION_FLAG,
        VIP_CD AS VIP_CD,
        INACTIVE_FLAG1 AS INACTIVE_FLAG1,
        PAYMENT_TERM_CD1 AS PAYMENT_TERM_CD1,
        INCO_TERM_CD1 AS INCO_TERM_CD1,
        ADDRESS1 AS ADDRESS1,
        CITY1 AS CITY1,
        STATE1 AS STATE1,
        COUNTRY_CD1 AS COUNTRY_CD1,
        ZIP1 AS ZIP1,
        CONTACT1 AS CONTACT1,
        CONTACT_PHONE1 AS CONTACT_PHONE1,
        PHONE1 AS PHONE1,
        FAX1 AS FAX1,
        RTV_ELIG_FLAG1 AS RTV_ELIG_FLAG1,
        RTV_TYPE_CD1 AS RTV_TYPE_CD1,
        RTV_FREIGHT_TYPE_CD1 AS RTV_FREIGHT_TYPE_CD1,
        INDUSTRY_CD1 AS INDUSTRY_CD1,
        ADD_DT1 AS ADD_DT1,
        LOCATION_ID1 AS LOCATION_ID1,
        LOAD_DT AS LOAD_DT,
        PURCH_GROUP_ID1 AS PURCH_GROUP_ID1,
        UpdateStrategy AS UpdateStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_MERCH_9""")

df_10.createOrReplaceTempView("UPD_INS_UPD_10")

# COMMAND ----------

# DBTITLE 1, EXP_NON_MERCH_11

df_11=spark.sql("""SELECT VENDOR_TYPE_ID3 AS VENDOR_TYPE_ID3,
VENDOR_NBR3 AS VENDOR_NBR3,
STORE_TYPE_ID3 AS STORE_TYPE_ID3,
VENDOR_NAME3 AS VENDOR_NAME3,
VENDOR_GROUP3 AS VENDOR_GROUP3,
PARENT_VENDOR_ID3 AS PARENT_VENDOR_ID3,
EDI_ELIG_FLAG3 AS EDI_ELIG_FLAG3,
PURCHASE_BLOCK3 AS PURCHASE_BLOCK3,
POSTING_BLOCK3 AS POSTING_BLOCK3,
DELETION_FLAG AS DELETION_FLAG,
VIP_CD AS VIP_CD,
INACTIVE_FLAG3 AS INACTIVE_FLAG3,
PAYMENT_TERM_CD3 AS PAYMENT_TERM_CD3,
INCO_TERM_CD3 AS INCO_TERM_CD3,
ADDRESS3 AS ADDRESS3,
CITY3 AS CITY3,
STATE3 AS STATE3,
COUNTRY_CD3 AS COUNTRY_CD3,
ZIP3 AS ZIP3,
CONTACT3 AS CONTACT3,
CONTACT_PHONE3 AS CONTACT_PHONE3,
PHONE3 AS PHONE3,
FAX3 AS FAX3,
RTV_ELIG_FLAG3 AS RTV_ELIG_FLAG3,
RTV_TYPE_CD3 AS RTV_TYPE_CD3,
RTV_FREIGHT_TYPE_CD3 AS RTV_FREIGHT_TYPE_CD3,
INDUSTRY_CD3 AS INDUSTRY_CD3,
ADD_DT3 AS ADD_DT3,
LOCATION_ID3 AS LOCATION_ID3,
PURCH_GROUP_ID3 AS PURCH_GROUP_ID3,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_11.createOrReplaceTempView("EXP_NON_MERCH_11")

# COMMAND ----------

# DBTITLE 1, UPD_NON_MERCH_INSERT_12

df_12=spark.sql("""
    SELECT
        VENDOR_TYPE_ID3 AS VENDOR_TYPE_ID3,
        VENDOR_NBR3 AS VENDOR_NBR3,
        VENDOR_NAME3 AS VENDOR_NAME3,
        EDI_ELIG_FLAG3 AS EDI_ELIG_FLAG3,
        PURCHASE_BLOCK3 AS PURCHASE_BLOCK3,
        POSTING_BLOCK3 AS POSTING_BLOCK3,
        DELETION_FLAG AS DELETION_FLAG,
        INACTIVE_FLAG3 AS INACTIVE_FLAG3,
        PAYMENT_TERM_CD3 AS PAYMENT_TERM_CD3,
        INCO_TERM_CD3 AS INCO_TERM_CD3,
        ADDRESS3 AS ADDRESS3,
        CITY3 AS CITY3,
        STATE3 AS STATE3,
        COUNTRY_CD3 AS COUNTRY_CD3,
        ZIP3 AS ZIP3,
        CONTACT3 AS CONTACT3,
        CONTACT_PHONE3 AS CONTACT_PHONE3,
        PHONE3 AS PHONE3,
        RTV_ELIG_FLAG3 AS RTV_ELIG_FLAG3,
        RTV_TYPE_CD3 AS RTV_TYPE_CD3,
        RTV_FREIGHT_TYPE_CD3 AS RTV_FREIGHT_TYPE_CD3,
        INDUSTRY_CD3 AS INDUSTRY_CD3,
        ADD_DT3 AS ADD_DT3,
        LOCATION_ID3 AS LOCATION_ID3,
        PURCH_GROUP_ID3 AS PURCH_GROUP_ID3,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_NON_MERCH_11""")

df_12.createOrReplaceTempView("UPD_NON_MERCH_INSERT_12")

# COMMAND ----------

# DBTITLE 1, VENDOR_PROFILE

spark.sql("""INSERT INTO VENDOR_PROFILE SELECT NEXTVAL AS VENDOR_ID,
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
LOAD_DT AS LOAD_DT FROM UPD_INS_UPD_10""")

# COMMAND ----------

# DBTITLE 1, VENDOR_PROFILE

spark.sql("""INSERT INTO VENDOR_PROFILE SELECT NEXTVAL AS VENDOR_ID,
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
LOAD_DT AS LOAD_DT FROM UPDATE_DC_INSERT_8""")

# COMMAND ----------

# DBTITLE 1, VENDOR_PROFILE

spark.sql("""INSERT INTO VENDOR_PROFILE SELECT NEXTVAL AS VENDOR_ID,
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
LOAD_DT AS LOAD_DT FROM SEQ""")

spark.sql("""INSERT INTO VENDOR_PROFILE SELECT NEXTVAL AS VENDOR_ID,
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
LOAD_DT AS LOAD_DT FROM UPD_NON_MERCH_INSERT_12""")