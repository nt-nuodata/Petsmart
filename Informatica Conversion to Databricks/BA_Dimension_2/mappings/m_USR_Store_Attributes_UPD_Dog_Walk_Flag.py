# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, USR_STORE_ATTRIBUTES_0


df_0=spark.sql("""
    SELECT
        STORE_NBR AS STORE_NBR,
        BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
        VET_TYPE_ID AS VET_TYPE_ID,
        MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
        ONP_DIST_FLAG AS ONP_DIST_FLAG,
        PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
        HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
        HOTEL_TIER_ID AS HOTEL_TIER_ID,
        STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
        STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
        FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
        STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
        MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
        NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
        MARKET_LEADER_ID AS MARKET_LEADER_ID,
        MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        USR_STORE_ATTRIBUTES""")

df_0.createOrReplaceTempView("USR_STORE_ATTRIBUTES_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1


df_1=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        DOG_WALK_FLAG AS DOG_WALK_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        USR_STORE_ATTRIBUTES_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1")

# COMMAND ----------
# DBTITLE 1, LKP_OPEN_CLOSE_FLAG_2


df_2=spark.sql("""
    SELECT
        SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1.LOCATION_ID AS i_LOCATION_ID,
        STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1.DOG_WALK_FLAG AS DOG_WALK_FLAG,
        SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE 
    RIGHT OUTER JOIN
        SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1 
            ON SITE_PROFILE.LOCATION_ID = SQ_Shortcut_to_USR_STORE_ATTRIBUTES_1.LOCATION_ID""")

df_2.createOrReplaceTempView("LKP_OPEN_CLOSE_FLAG_2")

# COMMAND ----------
# DBTITLE 1, SALES_DAY_SKU_STORE_3


df_3=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
        DATE_LOADED AS DATE_LOADED,
        VENDOR_ID AS VENDOR_ID,
        PROMO_FLAG AS PROMO_FLAG,
        WEEK_DT AS WEEK_DT,
        FISCAL_YR AS FISCAL_YR,
        SKU_VEND_TXN_CNT AS SKU_VEND_TXN_CNT,
        SALES_AMT AS SALES_AMT,
        SALES_COST AS SALES_COST,
        SALES_QTY AS SALES_QTY,
        RETURN_AMT AS RETURN_AMT,
        RETURN_COST AS RETURN_COST,
        RETURN_QTY AS RETURN_QTY,
        DISCOUNT_AMT AS DISCOUNT_AMT,
        DISCOUNT_QTY AS DISCOUNT_QTY,
        DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
        DISCOUNT_RETURN_QTY AS DISCOUNT_RETURN_QTY,
        POS_COUPON_AMT AS POS_COUPON_AMT,
        POS_COUPON_QTY AS POS_COUPON_QTY,
        SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
        SPECIAL_SALES_QTY AS SPECIAL_SALES_QTY,
        SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
        SPECIAL_RETURN_QTY AS SPECIAL_RETURN_QTY,
        SPECIAL_SRVC_AMT AS SPECIAL_SRVC_AMT,
        SPECIAL_SRVC_QTY AS SPECIAL_SRVC_QTY,
        EXCH_RATE_PCT AS EXCH_RATE_PCT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SALES_DAY_SKU_STORE""")

df_3.createOrReplaceTempView("SALES_DAY_SKU_STORE_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SALES_DAY_SKU_STORE_4


df_4=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        WEEK_DT AS WEEK_DT,
        SALES_AMT AS SALES_AMT,
        RETURN_AMT AS RETURN_AMT,
        DISCOUNT_AMT AS DISCOUNT_AMT,
        DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
        POS_COUPON_AMT AS POS_COUPON_AMT,
        SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
        SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SALES_DAY_SKU_STORE_3""")

df_4.createOrReplaceTempView("SQ_Shortcut_to_SALES_DAY_SKU_STORE_4")

# COMMAND ----------
# DBTITLE 1, SKU_PROFILE_5


df_5=spark.sql("""
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

df_5.createOrReplaceTempView("SKU_PROFILE_5")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_PROFILE_6


df_6=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_5""")

df_6.createOrReplaceTempView("SQ_Shortcut_To_SKU_PROFILE_6")

# COMMAND ----------
# DBTITLE 1, JNR_MERCH_HIERARCHY_7


df_7=spark.sql("""
    SELECT
        DETAIL.DAY_DT AS DAY_DT,
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.WEEK_DT AS WEEK_DT,
        DETAIL.SALES_AMT AS SALES_AMT,
        DETAIL.RETURN_AMT AS RETURN_AMT,
        DETAIL.DISCOUNT_AMT AS DISCOUNT_AMT,
        DETAIL.DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
        DETAIL.POS_COUPON_AMT AS POS_COUPON_AMT,
        DETAIL.SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
        DETAIL.SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
        MASTER.PRODUCT_ID AS PRODUCT_ID1,
        MASTER.SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        MASTER.SAP_CLASS_ID AS SAP_CLASS_ID,
        MASTER.SAP_DEPT_ID AS SAP_DEPT_ID,
        MASTER.SAP_DIVISION_ID AS SAP_DIVISION_ID,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_SKU_PROFILE_6 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_SALES_DAY_SKU_STORE_4 DETAIL 
            ON MASTER.PRODUCT_ID = DETAIL.PRODUCT_ID""")

df_7.createOrReplaceTempView("JNR_MERCH_HIERARCHY_7")

# COMMAND ----------
# DBTITLE 1, FIL_NEW_SALES_AND_CLASS_8


df_8=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        PRODUCT_ID AS PRODUCT_ID,
        LOCATION_ID AS LOCATION_ID,
        WEEK_DT AS WEEK_DT,
        SALES_AMT AS SALES_AMT,
        RETURN_AMT AS RETURN_AMT,
        DISCOUNT_AMT AS DISCOUNT_AMT,
        DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
        POS_COUPON_AMT AS POS_COUPON_AMT,
        SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
        SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
        PRODUCT_ID1 AS PRODUCT_ID1,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_MERCH_HIERARCHY_7 
    WHERE
        IFF(DAY_DT > (ADD_TO_DATE(current_timestamp, 'D', -41)) 
        AND SAP_CLASS_ID = 836, TRUE, FALSE)""")

df_8.createOrReplaceTempView("FIL_NEW_SALES_AND_CLASS_8")

# COMMAND ----------
# DBTITLE 1, AGG_SUM_OF_SALES_BY_WEEK_9


df_9=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        SUM(SALES_AMT) AS o_SALES_AMT,
        SUM(RETURN_AMT) AS o_RETURN_AMT,
        SUM(DISCOUNT_AMT) AS o_DISCOUNT_AMT,
        SUM(DISCOUNT_RETURN_AMT) AS o_DISCOUNT_RETURN_AMT,
        SUM(POS_COUPON_AMT) AS o_POS_COUPON_AMT,
        SUM(SPECIAL_SALES_AMT) AS o_SPECIAL_SALES_AMT,
        SUM(SPECIAL_RETURN_AMT) AS o_SPECIAL_RETURN_AMT 
    FROM
        FIL_NEW_SALES_AND_CLASS_8 
    GROUP BY
        LOCATION_ID,
        WEEK_DT""")

df_9.createOrReplaceTempView("AGG_SUM_OF_SALES_BY_WEEK_9")

# COMMAND ----------
# DBTITLE 1, AGG_MAX_OF_WEEKS_10


df_10=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        MAX(o_SALES_AMT - o_RETURN_AMT - o_DISCOUNT_AMT + o_DISCOUNT_RETURN_AMT - o_POS_COUPON_AMT - o_SPECIAL_SALES_AMT + o_SPECIAL_RETURN_AMT) AS MAX_SALES 
    FROM
        AGG_SUM_OF_SALES_BY_WEEK_9 
    GROUP BY
        LOCATION_ID""")

df_10.createOrReplaceTempView("AGG_MAX_OF_WEEKS_10")

# COMMAND ----------
# DBTITLE 1, EXP_DOG_WALK_FLAG_11


df_11=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        IFF(MAX_SALES >= 50,
        1,
        0) AS DOG_WALK_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        AGG_MAX_OF_WEEKS_10""")

df_11.createOrReplaceTempView("EXP_DOG_WALK_FLAG_11")

# COMMAND ----------
# DBTITLE 1, JNR_CLOSED_STORES_12


df_12=spark.sql("""
    SELECT
        DETAIL.LOCATION_ID AS LOCATION_ID,
        DETAIL.DOG_WALK_FLAG AS DOG_WALK_FLAG1,
        MASTER.i_LOCATION_ID AS i_LOCATION_ID,
        MASTER.STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
        MASTER.DOG_WALK_FLAG AS DOG_WALK_FLAG,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_OPEN_CLOSE_FLAG_2 MASTER 
    RIGHT JOIN
        EXP_DOG_WALK_FLAG_11 DETAIL 
            ON MASTER.i_LOCATION_ID = DETAIL.LOCATION_ID""")

df_12.createOrReplaceTempView("JNR_CLOSED_STORES_12")

# COMMAND ----------
# DBTITLE 1, EXP_CLOSED_STORES_13


df_13=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        IFF(ISNULL(LOCATION_ID),
        0,
        IFF(STORE_OPEN_CLOSE_FLAG = 'C',
        DOG_WALK_FLAG,
        DOG_WALK_FLAG1)) AS o_DOG_WALK_FLAG,
        IFF(STORE_OPEN_CLOSE_FLAG = 'C' 
        OR IFF(ISNULL(DOG_WALK_FLAG1),
        0,
        DOG_WALK_FLAG1) = IFF(ISNULL(DOG_WALK_FLAG),
        0,
        DOG_WALK_FLAG),
        DD_REJECT,
        DD_UPDATE) AS UPD_STRATEGY,
        current_timestamp AS UPD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_CLOSED_STORES_12""")

df_13.createOrReplaceTempView("EXP_CLOSED_STORES_13")

# COMMAND ----------
# DBTITLE 1, FIL_UPD_STRATEGY_14


df_14=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        DOG_WALK_FLAG AS DOG_WALK_FLAG,
        UPD_STRATEGY AS UPD_STRATEGY,
        UPD_TSTMP AS UPD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_CLOSED_STORES_13 
    WHERE
        UPD_STRATEGY = DD_UPDATE""")

df_14.createOrReplaceTempView("FIL_UPD_STRATEGY_14")

# COMMAND ----------
# DBTITLE 1, UPD_ONLY_CHANGES_15


df_15=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        DOG_WALK_FLAG AS DOG_WALK_FLAG,
        UPD_TSTMP AS UPD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_UPD_STRATEGY_14""")

df_15.createOrReplaceTempView("UPD_ONLY_CHANGES_15")

# COMMAND ----------
# DBTITLE 1, USR_STORE_ATTRIBUTES


spark.sql("""INSERT INTO USR_STORE_ATTRIBUTES SELECT LOCATION_ID AS LOCATION_ID,
STORE_NBR AS STORE_NBR,
BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
VET_TYPE_ID AS VET_TYPE_ID,
VET_TYPE_DESC AS VET_TYPE_DESC,
MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
ONP_DIST_FLAG AS ONP_DIST_FLAG,
PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
HOTEL_TIER_ID AS HOTEL_TIER_ID,
STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
SSDW_FLAG AS SSDW_FLAG,
DOG_WALK_FLAG AS DOG_WALK_FLAG,
GROOMERY_FLAG AS GROOMERY_FLAG,
STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
MARKET_LEADER_ID AS MARKET_LEADER_ID,
MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
UPD_TSTMP AS UPDATE_TSTMP,
current_timestamp() AS LOAD_TSTMP FROM UPD_ONLY_CHANGES_15""")