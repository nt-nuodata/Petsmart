# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, STX_UPC_PRE_0

df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        TXN_KEY_GID AS TXN_KEY_GID,
        UPC_ID AS UPC_ID,
        SEQ_NBR AS SEQ_NBR,
        VOID_TYPE_CD AS VOID_TYPE_CD,
        TXN_TYPE_ID AS TXN_TYPE_ID,
        ADOPTION_GROUP_ID AS ADOPTION_GROUP_ID,
        COMBO_TYPE_CD AS COMBO_TYPE_CD,
        PARENT_UPC_ID AS PARENT_UPC_ID,
        SOURCE_VENDOR_ID AS SOURCE_VENDOR_ID,
        UPC_KEYED_FLAG AS UPC_KEYED_FLAG,
        UPC_NOTONFILE_FLAG AS UPC_NOTONFILE_FLAG,
        ADJ_REASON_ID AS ADJ_REASON_ID,
        ORIG_UPC_TAX_STATUS_ID AS ORIG_UPC_TAX_STATUS_ID,
        REGULATED_ANIMAL_PERMIT_NBR AS REGULATED_ANIMAL_PERMIT_NBR,
        CARE_SHEET_GIVEN_FLAG AS CARE_SHEET_GIVEN_FLAG,
        RETURN_REASON_ID AS RETURN_REASON_ID,
        RETURN_DESC AS RETURN_DESC,
        SPECIAL_ORDER_NBR AS SPECIAL_ORDER_NBR,
        TP_INVOICE_NBR AS TP_INVOICE_NBR,
        TP_MASTER_INVOICE_NBR AS TP_MASTER_INVOICE_NBR,
        TRAINING_START_DT AS TRAINING_START_DT,
        NON_TAX_FLAG AS NON_TAX_FLAG,
        NON_DISCOUNT_FLAG AS NON_DISCOUNT_FLAG,
        SPECIAL_SALES_FLAG AS SPECIAL_SALES_FLAG,
        UPC_SEQ_NBR AS UPC_SEQ_NBR,
        UNIT_PRICE_AMT AS UNIT_PRICE_AMT,
        SALES_AMT AS SALES_AMT,
        SALES_QTY AS SALES_QTY,
        SALES_COST AS SALES_COST,
        RETURN_AMT AS RETURN_AMT,
        RETURN_QTY AS RETURN_QTY,
        RETURN_COST AS RETURN_COST,
        SERVICE_AMT AS SERVICE_AMT,
        DROP_SHIP_FLAG AS DROP_SHIP_FLAG,
        PET_ID AS PET_ID,
        SERVICE_BULK_SKU_NBR AS SERVICE_BULK_SKU_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STX_UPC_PRE""")

df_0.createOrReplaceTempView("STX_UPC_PRE_0")

# COMMAND ----------

# DBTITLE 1, UPC_1

df_1=spark.sql("""
    SELECT
        UPC_ID AS UPC_ID,
        UPC_CD AS UPC_CD,
        UPC_ADD_DT AS UPC_ADD_DT,
        UPC_DELETE_DT AS UPC_DELETE_DT,
        UPC_REFRESH_DT AS UPC_REFRESH_DT,
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        UPC""")

df_1.createOrReplaceTempView("UPC_1")

# COMMAND ----------

# DBTITLE 1, ASQ_Join_STX_UPC_PRE_And_Upc_2

df_2=spark.sql("""
    SELECT
        MAX(SUP.DAY_DT) DAY_DT,
        UPC.PRODUCT_ID 
    FROM
        STX_UPC_PRE SUP,
        UPC 
    WHERE
        SUP.VOID_TYPE_CD = 'N' 
        AND SUP.UPC_ID = UPC.UPC_ID 
    GROUP BY
        UPC.PRODUCT_ID""")

df_2.createOrReplaceTempView("ASQ_Join_STX_UPC_PRE_And_Upc_2")

# COMMAND ----------

# DBTITLE 1, LKP_PRODUCT_SALES_DATE_3

df_3=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        DATE_FIRST_SALE AS DATE_FIRST_SALE,
        DATE_LAST_SALE AS DATE_LAST_SALE,
        ASQ_Join_STX_UPC_PRE_And_Upc_2.PRODUCT_ID AS IN_PRODUCT_ID,
        ASQ_Join_STX_UPC_PRE_And_Upc_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PRODUCT 
    RIGHT OUTER JOIN
        ASQ_Join_STX_UPC_PRE_And_Upc_2 
            ON PRODUCT_ID = ASQ_Join_STX_UPC_PRE_And_Upc_2.PRODUCT_ID""")

df_3.createOrReplaceTempView("LKP_PRODUCT_SALES_DATE_3")

# COMMAND ----------

# DBTITLE 1, EXP_CHECK_SALES_DATE_4

df_4=spark.sql("""
    SELECT
        LKP_PRODUCT_SALES_DATE_3.PRODUCT_ID AS PRODUCT_ID,
        IFF(ASQ_Join_STX_UPC_PRE_And_Upc_2.DAY_DT < LKP_PRODUCT_SALES_DATE_3.DATE_FIRST_SALE,
        ASQ_Join_STX_UPC_PRE_And_Upc_2.DAY_DT,
        LKP_PRODUCT_SALES_DATE_3.DATE_FIRST_SALE) AS OUT_DATE_FIRST_SALE,
        IFF(ASQ_Join_STX_UPC_PRE_And_Upc_2.DAY_DT > LKP_PRODUCT_SALES_DATE_3.DATE_LAST_SALE,
        ASQ_Join_STX_UPC_PRE_And_Upc_2.DAY_DT,
        LKP_PRODUCT_SALES_DATE_3.DATE_LAST_SALE) AS OUT_DATE_LAST_SALE,
        LKP_PRODUCT_SALES_DATE_3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_PRODUCT_SALES_DATE_3 
    INNER JOIN
        ASQ_Join_STX_UPC_PRE_And_Upc_2 
            ON LKP_PRODUCT_SALES_DATE_3.Monotonically_Increasing_Id = ASQ_Join_STX_UPC_PRE_And_Upc_2.Monotonically_Increasing_Id""")

df_4.createOrReplaceTempView("EXP_CHECK_SALES_DATE_4")

# COMMAND ----------

# DBTITLE 1, UPD_UPDATE_PRODUCT_SALES_DATE_5

df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        OUT_DATE_FIRST_SALE AS DATE_FIRST_SALE,
        OUT_DATE_LAST_SALE AS DATE_LAST_SALE,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_CHECK_SALES_DATE_4""")

df_5.createOrReplaceTempView("UPD_UPDATE_PRODUCT_SALES_DATE_5")

# COMMAND ----------

# DBTITLE 1, PRODUCT

spark.sql("""INSERT INTO PRODUCT SELECT PRODUCT_ID AS PRODUCT_ID,
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
OUT_DATE_FIRST_INV AS DATE_FIRST_INV,
DATE_FIRST_SALE AS DATE_FIRST_SALE,
OUT_DATE_LAST_INV AS DATE_LAST_INV,
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
TAX_CLASS_ID AS TAX_CLASS_ID FROM UPD_UPDATE_PRODUCT_SALES_DATE_5""")
