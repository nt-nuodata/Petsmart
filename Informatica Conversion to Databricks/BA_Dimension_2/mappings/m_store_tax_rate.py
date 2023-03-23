# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, STORE_TAX_RATE_FLAT_0

df_0=spark.sql("""
    SELECT
        SITE_NBR AS SITE_NBR,
        JURISDICTION_TAX AS JURISDICTION_TAX,
        CITY_TAX AS CITY_TAX,
        COUNTY_TAX AS COUNTY_TAX,
        STATE_TAX AS STATE_TAX,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STORE_TAX_RATE_FLAT""")

df_0.createOrReplaceTempView("STORE_TAX_RATE_FLAT_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1

df_1=spark.sql("""
    SELECT
        SITE_NBR AS SITE_NBR,
        JURISDICTION_TAX AS JURISDICTION_TAX,
        CITY_TAX AS CITY_TAX,
        COUNTY_TAX AS COUNTY_TAX,
        STATE_TAX AS STATE_TAX,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        STORE_TAX_RATE_FLAT_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1")

# COMMAND ----------

# DBTITLE 1, LKP_PetsmartFacility_2

df_2=spark.sql("""
    SELECT
        FacilityGid AS FacilityGid,
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PetSmartFacility 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1 
            ON PetSmartFacility.FacilityNbr = SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.SITE_NBR""")

df_2.createOrReplaceTempView("LKP_PetsmartFacility_2")

# COMMAND ----------

# DBTITLE 1, LKP_FacilityAddress_3

df_3=spark.sql("""
    SELECT
        FacilityGid AS FacilityGid,
        CountryCd AS CountryCd,
        LKP_PetsmartFacility_2.FacilityGid AS FacilityGid_PetsmartFacility,
        LKP_PetsmartFacility_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FacilityAddress 
    RIGHT OUTER JOIN
        LKP_PetsmartFacility_2 
            ON FacilityGid = LKP_PetsmartFacility_2.FacilityGid""")

df_3.createOrReplaceTempView("LKP_FacilityAddress_3")

# COMMAND ----------

# DBTITLE 1, FLT_FacilityGID_Not_Null_4

df_4=spark.sql("""
    SELECT
        FacilityGid AS FacilityGid,
        COUNTRY_CD AS COUNTRY_CD,
        JURISDICTION_TAX AS JURISDICTION_TAX,
        CITY_TAX AS CITY_TAX,
        COUNTY_TAX AS COUNTY_TAX,
        STATE_TAX AS STATE_TAX,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1 
    WHERE
        NOT ISNULL(FacilityGid)""")

df_4.createOrReplaceTempView("FLT_FacilityGID_Not_Null_4")

df_4=spark.sql("""
    SELECT
        FacilityGid AS FacilityGid,
        COUNTRY_CD AS COUNTRY_CD,
        JURISDICTION_TAX AS JURISDICTION_TAX,
        CITY_TAX AS CITY_TAX,
        COUNTY_TAX AS COUNTY_TAX,
        STATE_TAX AS STATE_TAX,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_FacilityAddress_3 
    WHERE
        NOT ISNULL(FacilityGid)""")

df_4.createOrReplaceTempView("FLT_FacilityGID_Not_Null_4")

# COMMAND ----------

# DBTITLE 1, EXP_StoreRetail_5

df_5=spark.sql("""
    SELECT
        FacilityGid AS FacilityGid,
        CITY_TAX AS CityTaxRate,
        IFF(COUNTRY_CD = 'CA',
        0,
        COUNTY_TAX) AS CountyTaxRate,
        IFF(COUNTRY_CD = 'CA',
        0,
        STATE_TAX) AS StateTaxRate,
        JURISDICTION_TAX AS JurisdictionTax,
        IFF(COUNTRY_CD = 'CA',
        COUNTY_TAX,
        0) AS PSTRate,
        IFF(COUNTRY_CD = 'CA',
        STATE_TAX,
        0) AS GSTRate,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FLT_FacilityGID_Not_Null_4""")

df_5.createOrReplaceTempView("EXP_StoreRetail_5")

# COMMAND ----------

# DBTITLE 1, LKPTRANS_6

df_6=spark.sql("""
    SELECT
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        (SELECT
            TRIM(SITE_PROFILE.COUNTRY_CD) AS COUNTRY_CD,
            SITE_PROFILE.STORE_NBR AS STORE_NBR 
        FROM
            SITE_PROFILE) AS SITE_PROFILE 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1 
            ON SITE_PROFILE.STORE_NBR = SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.SITE_NBR""")

df_6.createOrReplaceTempView("LKPTRANS_6")

# COMMAND ----------

# DBTITLE 1, EXPTRANS_7

df_7=spark.sql("""
    SELECT
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.SITE_NBR AS SITE_NBR,
        LKPTRANS_6.COUNTRY_CD AS COUNTRY_CD,
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.JURISDICTION_TAX AS JURISDICTION_TAX,
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.CITY_TAX AS CITY_TAX,
        IFF(LKPTRANS_6.COUNTRY_CD = 'CA',
        0,
        COUNTY_TAX) AS o_COUNTY_TAX,
        IFF(LKPTRANS_6.COUNTRY_CD = 'CA',
        0,
        STATE_TAX) AS o_STATE_TAX,
        IFF(LKPTRANS_6.COUNTRY_CD = 'CA',
        COUNTY_TAX,
        0) AS PST,
        IFF(LKPTRANS_6.COUNTRY_CD = 'CA',
        STATE_TAX,
        0) AS GST,
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1 
    INNER JOIN
        LKPTRANS_6 
            ON SQ_Shortcut_to_STORE_TAX_RATE_FLAT_1.Monotonically_Increasing_Id = LKPTRANS_6.Monotonically_Increasing_Id""")

df_7.createOrReplaceTempView("EXPTRANS_7")

# COMMAND ----------

# DBTITLE 1, STORE_TAX_RATE

spark.sql("""INSERT INTO STORE_TAX_RATE SELECT SITE_NBR AS SITE_NBR,
COUNTRY_CD AS COUNTRY_CD,
JURISDICTION_TAX AS JURISDICTION_TAX,
CITY_TAX AS CITY_TAX,
o_COUNTY_TAX AS COUNTY_TAX,
o_STATE_TAX AS STATE_TAX,
PST AS PST,
GST AS GST FROM EXPTRANS_7""")

# COMMAND ----------

# DBTITLE 1, StoreRetail

spark.sql("""INSERT INTO StoreRetail SELECT FacilityGid AS FacilityGid,
CityTaxRate AS CityTaxRate,
CountyTaxRate AS CountyTaxRate,
StateTaxRate AS StateTaxRate,
JurisdictionTax AS JurisdictionTax,
PSTRate AS PSTRate,
GSTRate AS GSTRate FROM EXP_StoreRetail_5""")
