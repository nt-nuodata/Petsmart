# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, UDH_SKU_STYLE_0

df_0=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        SKU_DESC AS SKU_DESC,
        STYLE_ID AS STYLE_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        UDH_SKU_STYLE""")

df_0.createOrReplaceTempView("UDH_SKU_STYLE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_UDH_SKU_STYLE_1

df_1=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STYLE_ID AS STYLE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UDH_SKU_STYLE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_SKU_STYLE_1")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_UDH_SKU_STYLE1_2

df_2=spark.sql("""
    SELECT
        SKU_NBR AS SKU_NBR,
        STYLE_ID AS STYLE_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        UDH_SKU_STYLE_0""")

df_2.createOrReplaceTempView("SQ_Shortcut_to_UDH_SKU_STYLE1_2")

# COMMAND ----------

# DBTITLE 1, Group_By_Style_Id_3

df_3=spark.sql("""
    SELECT
        STYLE_ID1 AS STYLE_ID1,
        MAX(SKU_NBR1) AS IMG_SKU_NBR 
    FROM
        SQ_Shortcut_to_UDH_SKU_STYLE1_2 
    GROUP BY
        STYLE_ID1""")

df_3.createOrReplaceTempView("Group_By_Style_Id_3")

# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_4

df_4=spark.sql("""
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

df_4.createOrReplaceTempView("SKU_PROFILE_4")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_To_SKU_PROFILE_5

df_5=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_DESC AS SKU_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_4""")

df_5.createOrReplaceTempView("SQ_Shortcut_To_SKU_PROFILE_5")

# COMMAND ----------

# DBTITLE 1, JNR_SKU_PROFILE_PRODUCT_ID_6

df_6=spark.sql("""
    SELECT
        MASTER.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        DETAIL.STYLE_ID AS STYLE_ID,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_SKU_PROFILE_5 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_UDH_SKU_STYLE_1 DETAIL 
            ON SKU_NBR1 = DETAIL.SKU_NBR""")

df_6.createOrReplaceTempView("JNR_SKU_PROFILE_PRODUCT_ID_6")

# COMMAND ----------

# DBTITLE 1, JNR_UDH_SKU_STYLE_IMG_SKU_NBR_7

df_7=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        DETAIL.STYLE_ID AS STYLE_ID,
        MASTER.STYLE_ID1 AS STYLE_ID1,
        MASTER.IMG_SKU_NBR AS IMG_SKU_NBR,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Group_By_Style_Id_3 MASTER 
    LEFT JOIN
        JNR_SKU_PROFILE_PRODUCT_ID_6 DETAIL 
            ON MASTER.STYLE_ID1 = DETAIL.STYLE_ID""")

df_7.createOrReplaceTempView("JNR_UDH_SKU_STYLE_IMG_SKU_NBR_7")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_To_SKU_PROFILE1_8

df_8=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        SKU_DESC AS SKU_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_4""")

df_8.createOrReplaceTempView("SQ_Shortcut_To_SKU_PROFILE1_8")

# COMMAND ----------

# DBTITLE 1, JNR_SKU_PROFILE_SKU_DESC_IMG_PRODUCT_ID_9

df_9=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID1,
        DETAIL.SKU_NBR AS SKU_NBR1,
        DETAIL.STYLE_ID AS STYLE_ID1,
        DETAIL.IMG_SKU_NBR AS IMG_SKU_NBR,
        MASTER.PRODUCT_ID AS PRODUCT_ID,
        MASTER.SKU_NBR AS SKU_NBR,
        MASTER.SKU_DESC AS SKU_DESC,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_SKU_PROFILE1_8 MASTER 
    LEFT JOIN
        JNR_UDH_SKU_STYLE_IMG_SKU_NBR_7 DETAIL 
            ON MASTER.SKU_NBR = DETAIL.IMG_SKU_NBR""")

df_9.createOrReplaceTempView("JNR_SKU_PROFILE_SKU_DESC_IMG_PRODUCT_ID_9")

# COMMAND ----------

# DBTITLE 1, EXP_LOAD_TSTMP_10

df_10=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        STYLE_ID AS STYLE_ID,
        STYLE_DESC AS STYLE_DESC,
        IMG_SKU_NBR AS IMG_SKU_NBR,
        IMG_PRODUCT_ID AS IMG_PRODUCT_ID,
        SYSTIMESTAMP() AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_SKU_PROFILE_SKU_DESC_IMG_PRODUCT_ID_9""")

df_10.createOrReplaceTempView("EXP_LOAD_TSTMP_10")

# COMMAND ----------

# DBTITLE 1, UDH_SKU_STYLE_PRE

spark.sql("""INSERT INTO UDH_SKU_STYLE_PRE SELECT PRODUCT_ID AS PRODUCT_ID,
SKU_NBR AS SKU_NBR,
STYLE_ID AS STYLE_ID,
STYLE_DESC AS STYLE_DESC,
IMG_SKU_NBR AS IMG_SKU_NBR,
IMG_PRODUCT_ID AS IMG_PRODUCT_ID,
LOAD_TSTMP AS LOAD_TSTMP FROM EXP_LOAD_TSTMP_10""")
