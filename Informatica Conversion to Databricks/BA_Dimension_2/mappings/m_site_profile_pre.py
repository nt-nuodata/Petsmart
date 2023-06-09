# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, LOCATION_0

df_0=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        COMPANY_DESC AS COMPANY_DESC,
        COMPANY_ID AS COMPANY_ID,
        DATE_CLOSED AS DATE_CLOSED,
        DATE_OPEN AS DATE_OPEN,
        DATE_LOC_ADDED AS DATE_LOC_ADDED,
        DATE_LOC_DELETED AS DATE_LOC_DELETED,
        DATE_LOC_REFRESHED AS DATE_LOC_REFRESHED,
        DISTRICT_DESC AS DISTRICT_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        REGION_DESC AS REGION_DESC,
        REGION_ID AS REGION_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        STORE_CTRY AS STORE_CTRY,
        STORE_NAME AS STORE_NAME,
        STORE_NBR AS STORE_NBR,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        STORE_STATE_ABBR AS STORE_STATE_ABBR,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        STORE_TYPE_ID AS STORE_TYPE_ID,
        EQUINE_MERCH AS EQUINE_MERCH,
        DATE_GR_OPEN AS DATE_GR_OPEN,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        TP_LOC_FLAG AS TP_LOC_FLAG,
        TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
        TP_START_DT AS TP_START_DT,
        SITE_ADDRESS AS SITE_ADDRESS,
        SITE_CITY AS SITE_CITY,
        SITE_POSTAL_CD AS SITE_POSTAL_CD,
        SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
        SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LOCATION""")

df_0.createOrReplaceTempView("LOCATION_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_LOCATION_1

df_1=spark.sql("""
    SELECT
        SITE AS SITE,
        STORE_NAME AS STORE_NAME,
        STREET_ADDRESS AS STREET_ADDRESS,
        ZIP_CODE AS ZIP_CODE,
        CITY AS CITY,
        STORE_STATE_ABBR AS STORE_STATE_ABBR,
        SITE_CATEGORY AS SITE_CATEGORY,
        COUNTRY_KEY AS COUNTRY_KEY,
        OPEN_DATE AS OPEN_DATE,
        CLOSING_DATE AS CLOSING_DATE,
        SALES_AREA_SPACE AS SALES_AREA_SPACE,
        DATE_CREATED AS DATE_CREATED,
        STORE_COUNTRY AS STORE_COUNTRY,
        COMPANY_DESC AS COMPANY_DESC,
        COMPANY_ID AS COMPANY_ID,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        LOCATION_ID AS LOCATION_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        DISTRICT_ID AS DISTRICT_ID,
        REGION_DESC AS REGION_DESC,
        REGION_ID AS REGION_ID,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        DATE_LOC_DELETED AS DATE_LOC_DELETED,
        DATE_LOC_CREATED AS DATE_LOC_CREATED,
        SITE_PROFILE AS SITE_PROFILE,
        SITE_CATEGORY2 AS SITE_CATEGORY2,
        BANFIELD_FLAG AS BANFIELD_FLAG,
        EQUINE_MERCH AS EQUINE_MERCH,
        DATE_GR_OPEN AS DATE_GR_OPEN,
        DATE_SFT_OPEN AS DATE_SFT_OPEN,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        BROCKPORT_COMPANY_CD AS BROCKPORT_COMPANY_CD,
        BROCKPORT_SITE AS BROCKPORT_SITE,
        SITE_MAIN_TELE_NBR AS SITE_MAIN_TELE_NBR,
        GROOMING_TELE_NBR AS GROOMING_TELE_NBR,
        LABEL_INDICATOR AS LABEL_INDICATOR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LOCATION_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_LOCATION_1")

# COMMAND ----------

# DBTITLE 1, EXP_RTRIM_2

df_2=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        RTRIM(COMPANY_DESC) AS OUT_COMPANY_DESC,
        COMPANY_ID AS COMPANY_ID,
        DATE_LOC_ADDED AS DATE_LOC_ADDED,
        DATE_CLOSED AS DATE_CLOSED,
        DATE_LOC_DELETED AS DATE_LOC_DELETED,
        DATE_OPEN AS DATE_OPEN,
        RTRIM(INITCAP(DISTRICT_DESC)) AS OUT_DISTRICT_DESC,
        IFF(ISNULL(RTRIM(DISTRICT_ID)),
        'NULL',
        RTRIM(DISTRICT_ID)) AS OUT_DISTRICT_ID,
        RTRIM(PRICE_AD_ZONE_DESC) AS OUT_PRICE_AD_ZONE_DESC,
        RTRIM(PRICE_AD_ZONE_ID) AS OUT_PRICE_AD_ZONE_ID,
        RTRIM(PRICE_ZONE_DESC) AS OUT_PRICE_ZONE_DESC,
        RTRIM(PRICE_ZONE_ID) AS OUT_PRICE_ZONE_ID,
        RTRIM(INITCAP(REGION_DESC)) AS OUT_REGION_DESC,
        RTRIM(REGION_ID) AS OUT_REGION_ID,
        REPL_DC_NBR AS REPL_DC_NBR,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        RTRIM(STORE_COUNTRY) AS OUT_STORE_CTRY,
        RTRIM(INITCAP(STORE_NAME)) AS OUT_STORE_NAME,
        STORE_NBR AS STORE_NBR,
        IFF(CLOSING_DATE <> '99991231' 
        AND CLOSING_DATE <> '00000000',
        'C',
        'O') AS STORE_OPEN_CLOSE_FLAG,
        STORE_STATE_ABBR AS STORE_STATE_ABBR,
        RTRIM(STORE_TYPE_DESC) AS OUT_STORE_TYPE_DESC,
        RTRIM(SUBSTR(SITE_PROFILE,
        2,
        3)) AS OUT_STORE_TYPE_ID,
        RTRIM(EQUINE_MERCH) AS OUT_EQUINE_MERCH,
        DATE_GR_OPEN AS DATE_GR_OPEN,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        BP_GL_ACCT AS BP_GL_ACCT,
        RTRIM(STREET_ADDRESS) AS OUT_STREET_ADDRESS,
        RTRIM(ZIP_CODE) AS OUT_ZIP_CODE,
        RTRIM(CITY) AS OUT_CITY,
        RTRIM(SITE_MAIN_TELE_NBR) AS OUT_SITE_MAIN_TELE_NUMBER,
        RTRIM(GROOMING_TELE_NBR) AS OUT_GROOMING_TELE_NUMBER,
        LABEL_INDICATOR AS OUT_LABEL_INDICATOR,
        SITE_CATEGORY AS SITE_CATEGORY,
        SALES_AREA_SPACE AS SALES_AREA_SPACE,
        BANFIELD_FLAG AS BANFIELD_FLAG,
        DATE_SFT_OPEN AS DATE_SFT_OPEN,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_LOCATION_1""")

df_2.createOrReplaceTempView("EXP_RTRIM_2")

# COMMAND ----------

# DBTITLE 1, EXP_ORGANIZER_3

df_3=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        COMPANY_DESC AS COMPANY_DESC,
        STORE_NBR AS STORE_NBR,
        STORE_NAME AS STORE_NAME,
        IFF(STORE_NBR = 2940 
        OR STORE_NBR = 2941 
        OR STORE_NBR = 2801 
        OR STORE_NBR = 2859 
        OR STORE_NBR = 2877,
        'BRK',
        IFF(OUT_STORE_TYPE_ID = '130',
        '120',
        OUT_STORE_TYPE_ID)) AS OUT_STORE_TYPE_ID,
        COMPANY_ID AS COMPANY_ID,
        DISTRICT_DESC AS DISTRICT_DESC,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        REGION_ID AS REGION_ID,
        PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
        DISTRICT_ID AS DISTRICT_ID,
        PRICE_ZONE_ID AS PRICE_ZONE_ID,
        REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
        REPL_DC_NBR AS REPL_DC_NBR,
        PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
        REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
        PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
        REGION_DESC AS REGION_DESC,
        SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
        STORE_STATE_ABBR AS STORE_STATE_ABBR,
        STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
        ZIP_CODE AS ZIP_CODE,
        SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
        STREET_ADDRESS AS STREET_ADDRESS,
        CITY AS CITY,
        GROOMING_TELE_NUMBER AS GROOMING_TELE_NUMBER,
        SITE_MAIN_TELE_NUMBER AS SITE_MAIN_TELE_NUMBER,
        STORE_TYPE_DESC AS STORE_TYPE_DESC,
        STORE_CTRY AS STORE_CTRY,
        EQUINE_MERCH AS EQUINE_MERCH,
        IFF((DATE_GR_OPEN = '00000000'),
        to_date('12/31/9999',
        'mm/dd/yyyy'),
        TO_DATE(DATE_GR_OPEN,
        'YYYYMMDD')) AS O_DATE_GR_OPEN,
        IFF((DATE_OPEN = '00000000'),
        TO_DATE(DATE_LOC_ADDED,
        'YYYYMMDD'),
        TO_DATE(DATE_OPEN,
        'YYYYMMDD')) AS O_DATE_OPEN,
        BP_COMPANY_NBR AS BP_COMPANY_NBR,
        date_trunc('DAY',
        SESSSTARTTIME) AS O_DATE_LOC_REFRESHED,
        TO_DATE(DATE_LOC_ADDED,
        'YYYYMMDD') AS O_DATE_LOC_ADDED,
        BP_GL_ACCT AS BP_GL_ACCT,
        IFF((DATE_LOC_DELETED = '00000000'),
        to_date('12/31/9999',
        'mm/dd/yyyy'),
        TO_DATE(DATE_LOC_DELETED,
        'YYYYMMDD')) AS O_DATE_LOC_DELETED,
        IFF((DATE_CLOSED = '00000000'),
        to_date('12/31/9999',
        'mm/dd/yyyy'),
        TO_DATE(DATE_CLOSED,
        'YYYYMMDD')) AS O_DATE_CLOSED,
        LABEL_INDICATOR AS LABEL_INDICATOR,
        SITE_CATEGORY AS SITE_CATEGORY,
        SALES_AREA_SPACE AS SALES_AREA_SPACE,
        BANFIELD_FLAG AS BANFIELD_FLAG,
        IFF((DATE_SFT_OPEN = '00000000'),
        IFF((DATE_OPEN = '00000000'),
        TO_DATE(DATE_LOC_ADDED,
        'YYYYMMDD'),
        TO_DATE(DATE_OPEN,
        'YYYYMMDD')),
        TO_DATE(DATE_SFT_OPEN,
        'YYYYMMDD')) AS O_SFT_OPEN_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_RTRIM_2""")

df_3.createOrReplaceTempView("EXP_ORGANIZER_3")

# COMMAND ----------

# DBTITLE 1, SITE_PROFILE_PRE

spark.sql("""INSERT INTO SITE_PROFILE_PRE SELECT LOCATION_ID AS LOCATION_ID,
COMPANY_DESC AS COMPANY_DESC,
COMPANY_ID AS COMPANY_ID,
O_DATE_LOC_ADDED AS ADD_DT,
O_DATE_CLOSED AS CLOSE_DT,
O_DATE_LOC_DELETED AS DELETE_DT,
O_DATE_OPEN AS OPEN_DT,
DISTRICT_DESC AS DISTRICT_DESC,
DISTRICT_ID AS DISTRICT_ID,
PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
PRICE_ZONE_ID AS PRICE_ZONE_ID,
REGION_DESC AS REGION_DESC,
REGION_ID AS REGION_ID,
REPL_DC_NBR AS REPL_DC_NBR,
REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
STORE_CTRY_ABBR AS COUNTRY_CD,
STORE_CTRY AS COUNTRY_NAME,
STORE_NAME AS STORE_NAME,
STORE_NBR AS STORE_NBR,
STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
STORE_STATE_ABBR AS STATE_CD,
STORE_TYPE_DESC AS STORE_TYPE_DESC,
OUT_STORE_TYPE_ID AS STORE_TYPE_ID,
EQUINE_MERCH AS EQUINE_MERCH_DESC,
O_DATE_GR_OPEN AS GR_OPEN_DT,
SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
BP_COMPANY_NBR AS BP_COMPANY_NBR,
BP_GL_ACCT AS BP_GL_ACCT,
STREET_ADDRESS AS SITE_ADDRESS,
ZIP_CODE AS POSTAL_CD,
CITY AS SITE_CITY,
SITE_MAIN_TELE_NUMBER AS SITE_MAIN_TELE_NO,
GROOMING_TELE_NUMBER AS SITE_GROOM_TELE_NO,
O_DATE_LOC_REFRESHED AS LOAD_DT,
LABEL_INDICATOR AS PROMO_LABEL_CD,
O_SFT_OPEN_DT AS SFT_OPEN_DT,
BANFIELD_FLAG AS BANFIELD_FLAG,
SALES_AREA_SPACE AS SALES_AREA_FLR_SPC,
SITE_CATEGORY AS SITE_CATEGORY FROM EXP_ORGANIZER_3""")
