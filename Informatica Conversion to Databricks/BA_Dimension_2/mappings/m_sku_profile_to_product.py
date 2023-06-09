# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_0

df_0=spark.sql("""
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

df_0.createOrReplaceTempView("SKU_PROFILE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Sku_Profile_1

df_1=spark.sql("""
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
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_0""")

df_1.createOrReplaceTempView("SQ_Sku_Profile_1")

# COMMAND ----------

# DBTITLE 1, LKp_Status_2

df_2=spark.sql("""
    SELECT
        STATUS_NAME AS STATUS_NAME,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_STATUS 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON SKU_STATUS.STATUS_ID = SQ_Sku_Profile_1.STATUS_ID""")

df_2.createOrReplaceTempView("LKp_Status_2")

# COMMAND ----------

# DBTITLE 1, LKp_SAP_Division_3

df_3=spark.sql("""
    SELECT
        SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_DIVISION 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON SAP_DIVISION.SAP_DIVISION_ID = SQ_Sku_Profile_1.SAP_DIVISION_ID""")

df_3.createOrReplaceTempView("LKp_SAP_Division_3")

# COMMAND ----------

# DBTITLE 1, LKp_SAP_Department_4

df_4=spark.sql("""
    SELECT
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_DEPT 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON SAP_DEPT.SAP_DEPT_ID = SQ_Sku_Profile_1.SAP_DEPT_ID""")

df_4.createOrReplaceTempView("LKp_SAP_Department_4")

# COMMAND ----------

# DBTITLE 1, LKp_SAP_Class_5

df_5=spark.sql("""
    SELECT
        SAP_CLASS_DESC AS SAP_CLASS_DESC,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_CLASS 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON SAP_CLASS.SAP_CLASS_ID = SQ_Sku_Profile_1.SAP_CLASS_ID""")

df_5.createOrReplaceTempView("LKp_SAP_Class_5")

# COMMAND ----------

# DBTITLE 1, Lkp_Primary_vendor_6

df_6=spark.sql("""
    SELECT
        PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PRIMARY_VENDOR 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON PRIMARY_VENDOR.PRIMARY_VENDOR_ID = SQ_Sku_Profile_1.PRIMARY_VENDOR_ID""")

df_6.createOrReplaceTempView("Lkp_Primary_vendor_6")

# COMMAND ----------

# DBTITLE 1, Lkp_Sku_Uom_7

df_7=spark.sql("""
    SELECT
        WIDTH_AMT AS WIDTH_AMT,
        WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_UOM 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON SKU_UOM.PRODUCT_ID = SQ_Sku_Profile_1.PRODUCT_ID 
            AND SKU_UOM.UOM_CD = UOM_CD1 
            AND SKU_UOM.WEIGHT_UOM_CD = WEIGHT_UOM_CD1""")

df_7.createOrReplaceTempView("Lkp_Sku_Uom_7")

# COMMAND ----------

# DBTITLE 1, Lkp_SAP_Category_8

df_8=spark.sql("""
    SELECT
        SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_CATEGORY 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON SAP_CATEGORY.SAP_CATEGORY_ID = SQ_Sku_Profile_1.SAP_CATEGORY_ID""")

df_8.createOrReplaceTempView("Lkp_SAP_Category_8")

# COMMAND ----------

# DBTITLE 1, Lkp_Purch_Group_9

df_9=spark.sql("""
    SELECT
        PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        SQ_Sku_Profile_1.PURCH_GROUP_ID AS PURCH_GROUP_ID1,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        PURCH_GROUP 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON PURCH_GROUP.PURCH_GROUP_ID = SQ_Sku_Profile_1.PURCH_GROUP_ID""")

df_9.createOrReplaceTempView("Lkp_Purch_Group_9")

# COMMAND ----------

# DBTITLE 1, Lkp_HTS_10

df_10=spark.sql("""
    SELECT
        HTS_CODE_DESC AS HTS_CODE_DESC,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        HTS 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON HTS.HTS_CODE_ID = SQ_Sku_Profile_1.HTS_CODE_ID""")

df_10.createOrReplaceTempView("Lkp_HTS_10")

# COMMAND ----------

# DBTITLE 1, Lkp_Country_11

df_11=spark.sql("""
    SELECT
        COUNTRY_NAME AS COUNTRY_NAME,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        COUNTRY 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON COUNTRY.COUNTRY_CD = SQ_Sku_Profile_1.COUNTRY_CD""")

df_11.createOrReplaceTempView("Lkp_Country_11")

# COMMAND ----------

# DBTITLE 1, Lkp_Buyer_12

df_12=spark.sql("""
    SELECT
        BUYER_NAME AS BUYER_NAME,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        BUYER 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON BUYER.BUYER_ID = SQ_Sku_Profile_1.BUYER_ID""")

df_12.createOrReplaceTempView("Lkp_Buyer_12")

# COMMAND ----------

# DBTITLE 1, LKP_Brand_13

df_13=spark.sql("""
    SELECT
        BRAND_NAME AS BRAND_NAME,
        SQ_Sku_Profile_1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        BRAND 
    RIGHT OUTER JOIN
        SQ_Sku_Profile_1 
            ON BRAND.BRAND_CD = SQ_Sku_Profile_1.BRAND_CD""")

df_13.createOrReplaceTempView("LKP_Brand_13")

# COMMAND ----------

# DBTITLE 1, Exp_PRODUCT_14

df_14=spark.sql("""
    SELECT
        SQ_Sku_Profile_1.PRODUCT_ID AS PRODUCT_ID,
        SQ_Sku_Profile_1.ALT_DESC AS ALT_DESC,
        LKP_Brand_13.BRAND_NAME AS BRAND_NAME,
        SQ_Sku_Profile_1.BUM_QTY AS BUM_QTY,
        SQ_Sku_Profile_1.BUYER_ID AS BUYER_ID,
        Lkp_Buyer_12.BUYER_NAME AS BUYER_NAME,
        Lkp_Country_11.COUNTRY_NAME AS CTRY_ORIGIN,
        SQ_Sku_Profile_1.DISC_START_DT AS DATE_DISC_START,
        SQ_Sku_Profile_1.FIRST_INV_DT AS DATE_FIRST_INV,
        SQ_Sku_Profile_1.FIRST_SALE_DT AS DATE_FIRST_SALE,
        SQ_Sku_Profile_1.LAST_INV_DT AS DATE_LAST_INV,
        SQ_Sku_Profile_1.LAST_SALE_DT AS DATE_LAST_SALE,
        SQ_Sku_Profile_1.ADD_DT AS DATE_PROD_ADDED,
        SQ_Sku_Profile_1.DELETE_DT AS DATE_PROD_DELETED,
        SQ_Sku_Profile_1.UPDATE_DT AS DATE_PROD_REFRESHED,
        SQ_Sku_Profile_1.HTS_CODE_ID AS HTS_CODE,
        Lkp_HTS_10.HTS_CODE_DESC AS HTS_DESC,
        SQ_Sku_Profile_1.IMPORT_FLAG AS IMPORT_FLAG,
        SQ_Sku_Profile_1.OWNBRAND_FLAG AS OWNBRAND_FLAG,
        SQ_Sku_Profile_1.PRIMARY_UPC_ID AS PRIMARY_UPC,
        SQ_Sku_Profile_1.PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
        SQ_Sku_Profile_1.PURCH_GROUP_ID AS PURCH_GROUP_ID,
        Lkp_Purch_Group_9.PURCH_GROUP_NAME AS PURCH_GROUP_NAME,
        Lkp_SAP_Category_8.SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
        SQ_Sku_Profile_1.SAP_CLASS_ID AS SAP_CLASS_ID,
        SQ_Sku_Profile_1.SAP_DEPT_ID AS SAP_DEPT_ID,
        SQ_Sku_Profile_1.SAP_DIVISION_ID AS SAP_DIVISION_ID,
        SQ_Sku_Profile_1.CONTENTS AS CONTENTS,
        SQ_Sku_Profile_1.CONTENTS_UNITS AS CONTENTS_UNITS,
        SQ_Sku_Profile_1.SKU_DESC AS SKU_DESC,
        SQ_Sku_Profile_1.SKU_NBR AS SKU_NBR,
        SQ_Sku_Profile_1.STATELINE_FLAG AS STATELINE_FLAG,
        SQ_Sku_Profile_1.PURCH_COST_AMT AS SUM_COST_NTL,
        SQ_Sku_Profile_1.NAT_PRICE_CA_AMT AS SUM_RETAIL_NTL,
        IFF(Lkp_Sku_Uom_7.WEIGHT_GROSS_AMT > 999999.999,
        999999.999,
        Lkp_Sku_Uom_7.WEIGHT_GROSS_AMT) AS WEIGHT_GROSS,
        SQ_Sku_Profile_1.WEIGHT_NET_AMT AS WEIGHT_NET,
        SQ_Sku_Profile_1.WEIGHT_UOM_CD AS WEIGHT_UNITS,
        Lkp_Sku_Uom_7.WIDTH_AMT AS WIDTH,
        SQ_Sku_Profile_1.VENDOR_ARTICLE_NBR AS VENDOR_STYLE_NBR,
        Lkp_Primary_vendor_6.PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
        SQ_Sku_Profile_1.SIZE_DESC AS PRODUCT_SIZE,
        SQ_Sku_Profile_1.INIT_MKDN_DT AS DATE_INIT_MKDN,
        LKp_SAP_Class_5.SAP_CLASS_DESC AS SAP_CLASS_DESC,
        LKp_SAP_Department_4.SAP_DEPT_DESC AS SAP_DEPT_DESC,
        LKp_SAP_Division_3.SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        SQ_Sku_Profile_1.SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        SQ_Sku_Profile_1.STATUS_ID AS STATUS_ID,
        LKp_Status_2.STATUS_NAME AS STATUS_NAME,
        SQ_Sku_Profile_1.OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        SQ_Sku_Profile_1.TAX_CLASS_ID AS TAX_CLASS_ID,
        IFF(ISNULL(SQ_Sku_Profile_1.ARTICLE_CATEGORY_ID),
        0,
        SQ_Sku_Profile_1.ARTICLE_CATEGORY_ID) AS ARTICLE_CATEGORY_ID1,
        0 AS PLAN_GROUP_ID,
        LKP_Brand_13.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_Brand_13 
    INNER JOIN
        SQ_Sku_Profile_1 
            ON LKP_Brand_13.Monotonically_Increasing_Id = SQ_Sku_Profile_1.Monotonically_Increasing_Id 
    INNER JOIN
        Lkp_Buyer_12 
            ON LKP_Brand_13.Monotonically_Increasing_Id = Lkp_Buyer_12.Monotonically_Increasing_Id 
    INNER JOIN
        Lkp_Country_11 
            ON LKP_Brand_13.Monotonically_Increasing_Id = Lkp_Country_11.Monotonically_Increasing_Id 
    INNER JOIN
        Lkp_HTS_10 
            ON LKP_Brand_13.Monotonically_Increasing_Id = Lkp_HTS_10.Monotonically_Increasing_Id 
    INNER JOIN
        Lkp_Purch_Group_9 
            ON LKP_Brand_13.Monotonically_Increasing_Id = Lkp_Purch_Group_9.Monotonically_Increasing_Id 
    INNER JOIN
        Lkp_SAP_Category_8 
            ON LKP_Brand_13.Monotonically_Increasing_Id = Lkp_SAP_Category_8.Monotonically_Increasing_Id 
    INNER JOIN
        Lkp_Sku_Uom_7 
            ON LKP_Brand_13.Monotonically_Increasing_Id = Lkp_Sku_Uom_7.Monotonically_Increasing_Id 
    INNER JOIN
        Lkp_Primary_vendor_6 
            ON LKP_Brand_13.Monotonically_Increasing_Id = Lkp_Primary_vendor_6.Monotonically_Increasing_Id 
    INNER JOIN
        LKp_SAP_Class_5 
            ON LKP_Brand_13.Monotonically_Increasing_Id = LKp_SAP_Class_5.Monotonically_Increasing_Id 
    INNER JOIN
        LKp_SAP_Department_4 
            ON LKP_Brand_13.Monotonically_Increasing_Id = LKp_SAP_Department_4.Monotonically_Increasing_Id 
    INNER JOIN
        LKp_SAP_Division_3 
            ON LKP_Brand_13.Monotonically_Increasing_Id = LKp_SAP_Division_3.Monotonically_Increasing_Id 
    INNER JOIN
        LKp_Status_2 
            ON LKP_Brand_13.Monotonically_Increasing_Id = LKp_Status_2.Monotonically_Increasing_Id""")

df_14.createOrReplaceTempView("Exp_PRODUCT_14")

# COMMAND ----------

# DBTITLE 1, UPS_Product_15

df_15=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        ALT_DESC AS ALT_DESC,
        BRAND_NAME AS BRAND_NAME,
        BUM_QTY AS BUM_QTY,
        BUYER_ID AS BUYER_ID,
        BUYER_NAME AS BUYER_NAME,
        CTRY_ORIGIN AS CTRY_ORIGIN,
        DATE_DISC_START AS DATE_DISC_START,
        DATE_FIRST_INV AS DATE_FIRST_INV,
        DATE_FIRST_SALE AS DATE_FIRST_SALE,
        DATE_LAST_INV AS DATE_LAST_INV,
        DATE_LAST_SALE AS DATE_LAST_SALE,
        DATE_PROD_ADDED AS DATE_PROD_ADDED,
        DATE_PROD_DELETED AS DATE_PROD_DELETED,
        DATE_PROD_REFRESHED AS DATE_PROD_REFRESHED,
        HTS_CODE AS HTS_CODE,
        HTS_DESC AS HTS_DESC,
        IMPORT_FLAG AS IMPORT_FLAG,
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
        STATELINE_FLAG AS STATELINE_FLAG,
        SUM_COST_NTL AS SUM_COST_NTL,
        SUM_RETAIL_NTL AS SUM_RETAIL_NTL,
        WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
        WEIGHT_GROSS AS WEIGHT_GROSS,
        WEIGHT_NET AS WEIGHT_NET,
        WEIGHT_UNITS AS WEIGHT_UNITS,
        WIDTH AS WIDTH,
        VENDOR_STYLE_NBR AS VENDOR_STYLE_NBR,
        PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
        PRODUCT_SIZE AS PRODUCT_SIZE,
        DATE_INIT_MKDN AS DATE_INIT_MKDN,
        SAP_CLASS_DESC AS SAP_CLASS_DESC,
        SAP_DEPT_DESC AS SAP_DEPT_DESC,
        SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
        SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
        STATUS_ID AS STATUS_ID,
        STATUS_NAME AS STATUS_NAME,
        OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
        TAX_CLASS_ID AS TAX_CLASS_ID,
        ARTICLE_CATEGORY_ID1 AS ARTICLE_CATEGORY_ID,
        PLAN_GROUP_ID AS PLAN_GROUP_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_PRODUCT_14""")

df_15.createOrReplaceTempView("UPS_Product_15")

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
TAX_CLASS_ID AS TAX_CLASS_ID FROM UPS_Product_15""")
