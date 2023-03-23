# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ZTB_LIST_HOLD_0


df_0=spark.sql("""
    SELECT
        CLIENT AS CLIENT,
        MATNR AS MATNR,
        WERKS AS WERKS,
        POGID AS POGID,
        REQUEST_TYPE AS REQUEST_TYPE,
        LIST_TYPE AS LIST_TYPE,
        PROMO_QTY AS PROMO_QTY,
        LIST_START_DATE AS LIST_START_DATE,
        LIST_END_DATE AS LIST_END_DATE,
        REPL_START_DATE AS REPL_START_DATE,
        REPL_END_DATE AS REPL_END_DATE,
        STATUS AS STATUS,
        BUYER AS BUYER,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_LIST_HOLD""")

df_0.createOrReplaceTempView("ZTB_LIST_HOLD_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTB_LIST_HOLD_1


df_1=spark.sql("""
    SELECT
        CLIENT AS CLIENT,
        MATNR AS MATNR,
        WERKS AS WERKS,
        POGID AS POGID,
        REQUEST_TYPE AS REQUEST_TYPE,
        LIST_TYPE AS LIST_TYPE,
        PROMO_QTY AS PROMO_QTY,
        LIST_START_DATE AS LIST_START_DATE,
        LIST_END_DATE AS LIST_END_DATE,
        REPL_START_DATE AS REPL_START_DATE,
        REPL_END_DATE AS REPL_END_DATE,
        STATUS AS STATUS,
        BUYER AS BUYER,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ZTB_LIST_HOLD_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_LIST_HOLD_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


df_2=spark.sql("""
    SELECT
        CLIENT AS CLIENT,
        (CAST(MATNR AS DECIMAL (38,
        0))) AS SKU_NBR,
        i_WERKS AS i_WERKS,
        (CAST(WERKS AS DECIMAL (38,
        0))) AS STORE_NBR,
        substr(POGID,
        1,
        8) AS POG_NBR,
        REQUEST_TYPE AS REQUEST_TYPE,
        LIST_TYPE AS LIST_TYPE,
        PROMO_QTY AS PROMO_QTY,
        to_date(LIST_START_DATE,
        'YYYYMMDD') AS LIST_START_DATE,
        to_date(LIST_END_DATE,
        'YYYYMMDD') AS LIST_END_DATE,
        to_date(REPL_START_DATE,
        'YYYYMMDD') AS REPL_START_DATE,
        to_date(REPL_END_DATE,
        'YYYYMMDD') AS REPL_END_DATE,
        STATUS AS STATUS,
        BUYER AS BUYER,
        current_timestamp AS CREATE_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ZTB_LIST_HOLD_1""")

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, LKP_SKuProfile_3


df_3=spark.sql("""
    SELECT
        EXPTRANS_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE 
    RIGHT OUTER JOIN
        EXPTRANS_2 
            ON SKU_PROFILE.SKU_NBR = EXPTRANS_2.SKU_NBR""")

df_3.createOrReplaceTempView("LKP_SKuProfile_3")

# COMMAND ----------
# DBTITLE 1, LKP_SiteProfile_4


df_4=spark.sql("""
    SELECT
        EXPTRANS_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE 
    RIGHT OUTER JOIN
        EXPTRANS_2 
            ON SITE_PROFILE.STORE_NBR = EXPTRANS_2.STORE_NBR""")

df_4.createOrReplaceTempView("LKP_SiteProfile_4")

# COMMAND ----------
# DBTITLE 1, FILTRANS_5


df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        POG_NBR AS POG_NBR,
        REPL_START_DATE AS REPL_START_DATE,
        REPL_END_DATE AS REPL_END_DATE,
        LIST_START_DATE AS LIST_START_DATE,
        LIST_END_DATE AS LIST_END_DATE,
        PROMO_QTY AS PROMO_QTY,
        CREATE_DT AS CREATE_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_SKuProfile_3 
    WHERE
        NOT isnull(PRODUCT_ID) 
        AND NOT isnull(LOCATION_ID) 
        AND NOT isnull(POG_NBR)""")

df_5.createOrReplaceTempView("FILTRANS_5")

df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        POG_NBR AS POG_NBR,
        REPL_START_DATE AS REPL_START_DATE,
        REPL_END_DATE AS REPL_END_DATE,
        LIST_START_DATE AS LIST_START_DATE,
        LIST_END_DATE AS LIST_END_DATE,
        PROMO_QTY AS PROMO_QTY,
        CREATE_DT AS CREATE_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_SiteProfile_4 
    WHERE
        NOT isnull(PRODUCT_ID) 
        AND NOT isnull(LOCATION_ID) 
        AND NOT isnull(POG_NBR)""")

df_5.createOrReplaceTempView("FILTRANS_5")

df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        POG_NBR AS POG_NBR,
        REPL_START_DATE AS REPL_START_DATE,
        REPL_END_DATE AS REPL_END_DATE,
        LIST_START_DATE AS LIST_START_DATE,
        LIST_END_DATE AS LIST_END_DATE,
        PROMO_QTY AS PROMO_QTY,
        CREATE_DT AS CREATE_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_2 
    WHERE
        NOT isnull(PRODUCT_ID) 
        AND NOT isnull(LOCATION_ID) 
        AND NOT isnull(POG_NBR)""")

df_5.createOrReplaceTempView("FILTRANS_5")

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
# DBTITLE 1, POG_PROMO_HOLD


spark.sql("""INSERT INTO POG_PROMO_HOLD SELECT PRODUCT_ID AS PRODUCT_ID,
LOCATION_ID AS LOCATION_ID,
POG_NBR AS POG_NBR,
REPL_START_DATE AS REPL_START_DT,
REPL_END_DATE AS REPL_END_DT,
LIST_START_DATE AS LIST_START_DT,
LIST_END_DATE AS LIST_END_DT,
PROMO_QTY AS PROMO_HOLD_QTY,
LAST_CHNG_DT AS LAST_CHNG_DT,
POG_STATUS_CD AS POG_STATUS_CD,
UPDATE_DT AS UPDATE_DT,
CREATE_DT AS LOAD_DT FROM FILTRANS_5""")