# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, PRODUCT_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        ALT_DESC AS ALT_DESC,
        ANIMAL AS ANIMAL,
        ANIMALSIZE AS ANIMALSIZE,
        BRAND_NAME AS BRAND_NAME,
        BUM_QTY AS BUM_QTY,
        BUYER_ID AS BUYER_ID,
        BUYER_NAME AS BUYER_NAME,
        CTRY_ORIGIN AS CTRY_ORIGIN,
        DATE_DISC_START AS DATE_DISC_START,
        DATE_DISC_END AS DATE_DISC_END,
        DATE_FIRST_INV AS DATE_FIRST_INV,
        DATE_FIRST_SALE AS DATE_FIRST_SALE,
        DATE_LAST_INV AS DATE_LAST_INV,
        DATE_LAST_SALE AS DATE_LAST_SALE,
        DATE_PROD_ADDED AS DATE_PROD_ADDED,
        DATE_PROD_DELETED AS DATE_PROD_DELETED,
        DATE_PROD_REFRESHED AS DATE_PROD_REFRESHED,
        DEPTH AS DEPTH,
        DIM_UNITS AS DIM_UNITS,
        FLAVOR AS FLAVOR,
        HEIGHT AS HEIGHT,
        HTS_CODE AS HTS_CODE,
        HTS_DESC AS HTS_DESC,
        IMPORT_FLAG AS IMPORT_FLAG,
        MFGREP_NAME AS MFGREP_NAME,
        OWNBRAND_FLAG AS OWNBRAND_FLAG,
        PRIMARY_UPC AS PRIMARY_UPC,
        PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        PURCH_GROUP_ID AS PURCH_GROUP_ID,
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
        SAP_CLASS_ID AS SAP_CLASS_ID,
        SAP_DEPT_ID AS SAP_DEPT_ID,
        SAP_DIVISION_ID AS SAP_DIVISION_ID,
        CONTENTS AS CONTENTS,
        CONTENTS_UNITS AS CONTENTS_UNITS,
        SKU_DESC AS SKU_DESC,
        SKU_NBR AS SKU_NBR,
        SKU_NBR_REF AS SKU_NBR_REF,
        STATELINE_FLAG AS STATELINE_FLAG,
        SUM_COST_NTL AS SUM_COST_NTL,
        SUM_RETAIL_NTL AS SUM_RETAIL_NTL,
        TUM_QTY AS TUM_QTY,
        VENDOR_SUB_RANGE AS VENDOR_SUB_RANGE,
        VOLUME AS VOLUME,
        VOLUME_UNITS AS VOLUME_UNITS,
        WEIGHT_GROSS AS WEIGHT_GROSS,
        WEIGHT_NET AS WEIGHT_NET,
        WEIGHT_UNITS AS WEIGHT_UNITS,
        WIDTH AS WIDTH,
        CATEGORY_DESC AS CATEGORY_DESC,
        CATEGORY_ID AS CATEGORY_ID,
        VENDOR_STYLE_NBR AS VENDOR_STYLE_NBR,
        PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
        COLOR AS COLOR,
        FLAVOR2 AS FLAVOR2,
        POND_FLAG AS POND_FLAG,
        PRODUCT_SIZE AS PRODUCT_SIZE,
        DATE_INIT_MKDN AS DATE_INIT_MKDN,
        ARTICLE_TYPE AS ARTICLE_TYPE,
        SAP_CLASS_DESC AS SAP_CLASS_DESC,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        SEASON_DESC AS SEASON_DESC,
        SEASON_ID AS SEASON_ID,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        PLAN_GROUP_DESC AS PLAN_GROUP_DESC,
        PLAN_GROUP_ID AS PLAN_GROUP_ID,
        STATUS_ID AS STATUS_ID,
        STATUS_NAME AS STATUS_NAME,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        TAX_CLASS_ID AS TAX_CLASS_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        PRODUCT""")

df_0.createOrReplaceTempView("PRODUCT_0")

# COMMAND ----------
# DBTITLE 1, ASQ_PRODUCT_1


df_1=spark.sql("""
    SELECT
        SAP.PRODUCT_ID,
        SAP.BUYER_ID,
        SAP.PRIMARY_UPC,
        SAP.SKU_NBR,
        SAP.SAP_CATEGORY_ID,
        SAP.OLD_ARTICLE_NBR,
        CASE 
            WHEN SAP.OLD_ARTICLE_NBR IS NULL THEN SAP.OLD_ARTICLE_NBR 
            ELSE substr(SAP.OLD_ARTICLE_NBR,
            1,
            6) || NVL(substr(DCLR_ID,
            1,
            3),
            '',
            substr(DCLR_ID,
            1,
            3)) || NVL(DSZE_ID,
            '',
            DSZE_ID) || NVL(DBRD_ID,
            '',
            DBRD_ID) || ' ' 
        END AS ITEM_CONCATENATED,
        SAP.PRIMARY_VENDOR_ID,
        SAP.PURCH_GROUP_ID 
    FROM
        (SELECT
            PRD.PRODUCT_ID,
            PRD.BUYER_ID,
            PRD.PRIMARY_UPC,
            PRD.PRIMARY_VENDOR_ID,
            PRD.PURCH_GROUP_ID,
            PRD.SKU_NBR,
            PRD.SAP_CATEGORY_ID,
            PRD.OLD_ARTICLE_NBR 
        FROM
            PRODUCT PRD 
        WHERE
            PRD.DATE_PROD_REFRESHED = CURRENT_DATE) SAP 
    LEFT OUTER JOIN
        (
            SELECT
                ATT.PRODUCT_ID,
                ATT.SAP_ATT_VALUE_ID AS DBRD_ID 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                ATT.SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'DBRD' 
                AND ATT.delete_flag <> 'X' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) DBRD 
            ON SAP.PRODUCT_ID = DBRD.PRODUCT_ID 
    LEFT OUTER JOIN
        (
            SELECT
                ATT.PRODUCT_ID,
                ATT.SAP_ATT_VALUE_ID AS DCLR_ID 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'DCLR' 
                AND ATT.delete_flag <> 'X' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) DCLR 
            ON SAP.PRODUCT_ID = DCLR.PRODUCT_ID 
    LEFT OUTER JOIN
        (
            SELECT
                ATT.PRODUCT_ID,
                ATT.SAP_ATT_VALUE_ID AS DSZE_ID 
            FROM
                SAP_ATTRIBUTE ATT,
                SAP_ATT_CODE COD,
                SAP_ATT_VALUE VAL 
            WHERE
                SAP_ATT_TYPE_ID = 'ATT' 
                AND ATT.SAP_ATT_CODE_ID = 'DSZE' 
                AND ATT.DELETE_FLAG <> 'X' 
                AND ATT.SAP_ATT_CODE_ID = COD.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_CODE_ID = VAL.SAP_ATT_CODE_ID 
                AND ATT.SAP_ATT_VALUE_ID = VAL.SAP_ATT_VALUE_ID
        ) DSZE 
            ON SAP.PRODUCT_ID = DSZE.PRODUCT_ID""")

df_1.createOrReplaceTempView("ASQ_PRODUCT_1")

# COMMAND ----------
# DBTITLE 1, PRODUCT_KEY_PRE


spark.sql("""INSERT INTO PRODUCT_KEY_PRE SELECT PRODUCT_ID AS PRODUCT_ID,
BUYER_ID AS BUYER_ID,
PRIMARY_UPC AS PRIMARY_UPC,
PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
PURCH_GROUP_ID AS PURCH_GROUP_ID,
SKU_NBR AS SKU_NBR,
SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
ITEM_CONCATENATED AS ITEM_CONCATENATED FROM ASQ_PRODUCT_1""")