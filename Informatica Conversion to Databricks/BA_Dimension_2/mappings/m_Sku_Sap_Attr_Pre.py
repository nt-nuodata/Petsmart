# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ZTPIM_ART_ATTR_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        MATNR AS MATNR,
        ATTNUM AS ATTNUM,
        ATTVALNUM AS ATTVALNUM,
        DEL_IND AS DEL_IND,
        CHANGED_BY AS CHANGED_BY,
        CHANGED_ON AS CHANGED_ON,
        CHANGED_AT AS CHANGED_AT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTPIM_ART_ATTR""")

df_0.createOrReplaceTempView("ZTPIM_ART_ATTR_0")

# COMMAND ----------

# DBTITLE 1, Fil_ZTPIM_ART_ATTR


# COMMAND ----------

# DBTITLE 1, Exp_SAP_KOI_2

df_2=spark.sql("""SELECT TO_INTEGER(MATNR) AS o_MATNR,
TO_INTEGER(ATTVALNUM) AS SKU_SAP_KOI_ID,
IIF(TO_INTEGER(ATTVALNUM) = 1, 'Yes', 'No') AS SKU_SAP_KOI_DESC,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_2.createOrReplaceTempView("Exp_SAP_KOI_2")

# COMMAND ----------

# DBTITLE 1, ZTPIM_MAP_3

df_3=spark.sql("""
    SELECT
        MANDT AS MANDT,
        ARTICLE AS ARTICLE,
        DATAB AS DATAB,
        DATBI AS DATBI,
        MAP_PRICE AS MAP_PRICE,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTPIM_MAP""")

df_3.createOrReplaceTempView("ZTPIM_MAP_3")

# COMMAND ----------

# DBTITLE 1, FIL_DATE_IN_DATAB_DATBI


# COMMAND ----------

# DBTITLE 1, EXP_ZM_ARTICLE_CHANGE_TO_INTEGER_5

df_5=spark.sql("""SELECT TO_INTEGER(ARTICLE) AS o_ARTICLE,
TO_CHAR(MAP_PRICE) AS o_MAP_PRICE,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_5.createOrReplaceTempView("EXP_ZM_ARTICLE_CHANGE_TO_INTEGER_5")

# COMMAND ----------

# DBTITLE 1, ZTB_PVT_LBL_GAP_6

df_6=spark.sql("""
    SELECT
        MANDT AS MANDT,
        PLG_NUMBER AS PLG_NUMBER,
        MATNR AS MATNR,
        TIER AS TIER,
        PARENT_TIER AS PARENT_TIER,
        PLG_IDX AS PLG_IDX,
        MIN_PLG_IDX AS MIN_PLG_IDX,
        MAX_PLG_IDX AS MAX_PLG_IDX,
        CREATED_BY AS CREATED_BY,
        CREATED_ON AS CREATED_ON,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_PVT_LBL_GAP""")

df_6.createOrReplaceTempView("ZTB_PVT_LBL_GAP_6")

# COMMAND ----------

# DBTITLE 1, EXP_ZPLG_MATNR_CHANGE_TO_INTEGER_7

df_7=spark.sql("""SELECT PLG_NUMBER AS PLG_NUMBER,
TO_INTEGER(MATNR) AS o_MATNR,
TO_CHAR(TIER) AS o_TIER,
TO_CHAR(PARENT_TIER) AS o_PARENT_TIER,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_7.createOrReplaceTempView("EXP_ZPLG_MATNR_CHANGE_TO_INTEGER_7")

# COMMAND ----------

# DBTITLE 1, ZTB_PR_LABEL_8

df_8=spark.sql("""
    SELECT
        MANDT AS MANDT,
        PLG_NUMBER AS PLG_NUMBER,
        PLG_DESCRIPTION AS PLG_DESCRIPTION,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_PR_LABEL""")

df_8.createOrReplaceTempView("ZTB_PR_LABEL_8")

# COMMAND ----------

# DBTITLE 1, ZTB_SIZE_PARITY_9

df_9=spark.sql("""
    SELECT
        MANDT AS MANDT,
        MATNR AS MATNR,
        SP_GRP_ID AS SP_GRP_ID,
        SP_GRP_NAME AS SP_GRP_NAME,
        CREATED_BY AS CREATED_BY,
        CREATED_ON AS CREATED_ON,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_SIZE_PARITY""")

df_9.createOrReplaceTempView("ZTB_SIZE_PARITY_9")

# COMMAND ----------

# DBTITLE 1, EXP_ZSP_MATNR_CHANGE_TO_INTEGER_10

df_10=spark.sql("""SELECT TO_INTEGER(MATNR) AS o_MATNR,
SP_GRP_ID AS SP_GRP_ID,
SP_GRP_NAME AS SP_GRP_NAME,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_10.createOrReplaceTempView("EXP_ZSP_MATNR_CHANGE_TO_INTEGER_10")

# COMMAND ----------

# DBTITLE 1, ZTB_PR_FAM_ART_11

df_11=spark.sql("""
    SELECT
        MANDT AS MANDT,
        PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
        MATNR AS MATNR,
        PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
        MAKTX AS MAKTX,
        CREATED_BY AS CREATED_BY,
        CREATED_ON AS CREATED_ON,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_PR_FAM_ART""")

df_11.createOrReplaceTempView("ZTB_PR_FAM_ART_11")

# COMMAND ----------

# DBTITLE 1, EXP_ZPFA_MATNR_CHANGE_TO_INTEGER_12

df_12=spark.sql("""SELECT PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
TO_INTEGER(MATNR) AS o_MATNR,
PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_12.createOrReplaceTempView("EXP_ZPFA_MATNR_CHANGE_TO_INTEGER_12")

# COMMAND ----------

# DBTITLE 1, ZTB_PR_GRP_ART_13

df_13=spark.sql("""
    SELECT
        MANDT AS MANDT,
        KVI_CODE AS KVI_CODE,
        MATNR AS MATNR,
        ZONE_CODE AS ZONE_CODE,
        KVI_TYPE AS KVI_TYPE,
        KVI_NAME AS KVI_NAME,
        MAKTX AS MAKTX,
        CREATED_BY AS CREATED_BY,
        CREATED_ON AS CREATED_ON,
        CREATED_AT AS CREATED_AT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_PR_GRP_ART""")

df_13.createOrReplaceTempView("ZTB_PR_GRP_ART_13")

# COMMAND ----------

# DBTITLE 1, AGG_KVI_CODE_KVI_NAME


# COMMAND ----------

# DBTITLE 1, EXP_ZPGA_MATNR_CHANGE_TO_INTEGER_15

df_15=spark.sql("""SELECT KVI_CODE AS KVI_CODE,
KVI_NAME AS KVI_NAME,
TO_INTEGER(MATNR) AS o_MATNR,
Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null""")

df_15.createOrReplaceTempView("EXP_ZPGA_MATNR_CHANGE_TO_INTEGER_15")

# COMMAND ----------

# DBTITLE 1, SKU_PROFILE_16

df_16=spark.sql("""
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

df_16.createOrReplaceTempView("SKU_PROFILE_16")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_To_SKU_PROFILE_17

df_17=spark.sql("""
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
        SKU_PROFILE_16""")

df_17.createOrReplaceTempView("SQ_Shortcut_To_SKU_PROFILE_17")

# COMMAND ----------

# DBTITLE 1, JNR_ZTB_PR_GRP_ART_18

df_18=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        MASTER.KVI_CODE AS KVI_CODE,
        MASTER.KVI_NAME AS KVI_NAME,
        MASTER.o_MATNR AS MATNR,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_ZPGA_MATNR_CHANGE_TO_INTEGER_15 MASTER 
    LEFT JOIN
        SQ_Shortcut_To_SKU_PROFILE_17 DETAIL 
            ON MASTER.o_MATNR = DETAIL.SKU_NBR""")

df_18.createOrReplaceTempView("JNR_ZTB_PR_GRP_ART_18")

# COMMAND ----------

# DBTITLE 1, JNR_ZTB_PR_FAM_ART_19

df_19=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        DETAIL.KVI_CODE AS KVI_CODE,
        DETAIL.KVI_NAME AS KVI_NAME,
        MASTER.PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
        MASTER.o_MATNR AS MATNR,
        MASTER.PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_ZPFA_MATNR_CHANGE_TO_INTEGER_12 MASTER 
    LEFT JOIN
        JNR_ZTB_PR_GRP_ART_18 DETAIL 
            ON MASTER.o_MATNR = DETAIL.SKU_NBR""")

df_19.createOrReplaceTempView("JNR_ZTB_PR_FAM_ART_19")

# COMMAND ----------

# DBTITLE 1, JNR_ZTB_SIZE_PARITY_20

df_20=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        DETAIL.KVI_CODE AS KVI_CODE,
        DETAIL.KVI_NAME AS KVI_NAME,
        DETAIL.PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
        DETAIL.PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
        MASTER.o_MATNR AS MATNR,
        MASTER.SP_GRP_ID AS SP_GRP_ID,
        MASTER.SP_GRP_NAME AS SP_GRP_NAME,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_ZSP_MATNR_CHANGE_TO_INTEGER_10 MASTER 
    LEFT JOIN
        JNR_ZTB_PR_FAM_ART_19 DETAIL 
            ON MASTER.o_MATNR = DETAIL.SKU_NBR""")

df_20.createOrReplaceTempView("JNR_ZTB_SIZE_PARITY_20")

# COMMAND ----------

# DBTITLE 1, JNR_ZTB_PVT_LBL_GAP_21

df_21=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        DETAIL.KVI_CODE AS KVI_CODE,
        DETAIL.KVI_NAME AS KVI_NAME,
        DETAIL.PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
        DETAIL.PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
        DETAIL.SP_GRP_ID AS SP_GRP_ID,
        DETAIL.SP_GRP_NAME AS SP_GRP_NAME,
        MASTER.PLG_NUMBER AS PLG_NUMBER,
        MASTER.o_TIER AS TIER,
        MASTER.o_PARENT_TIER AS PARENT_TIER,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_ZPLG_MATNR_CHANGE_TO_INTEGER_7 MASTER 
    LEFT JOIN
        JNR_ZTB_SIZE_PARITY_20 DETAIL 
            ON MATNR = DETAIL.SKU_NBR""")

df_21.createOrReplaceTempView("JNR_ZTB_PVT_LBL_GAP_21")

# COMMAND ----------

# DBTITLE 1, JNR_ZTB_PR_LABEL_22

df_22=spark.sql("""SELECT DETAIL.PRODUCT_ID AS PRODUCT_ID,
DETAIL.SKU_NBR AS SKU_NBR,
DETAIL.KVI_CODE AS KVI_CODE,
DETAIL.KVI_NAME AS KVI_NAME,
DETAIL.PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
DETAIL.PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
DETAIL.SP_GRP_ID AS SP_GRP_ID,
DETAIL.SP_GRP_NAME AS SP_GRP_NAME,
DETAIL.PLG_NUMBER AS PLG_NUMBER,
DETAIL.TIER AS TIER,
DETAIL.PARENT_TIER AS PARENT_TIER,
MASTER.PLG_DESCRIPTION AS PLG_DESCRIPTION,
MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id FROM null MASTER  LEFT JOIN JNR_ZTB_PVT_LBL_GAP_21 DETAIL  ON PLG_NUMBER1 = DETAIL.PLG_NUMBER""")

df_22.createOrReplaceTempView("JNR_ZTB_PR_LABEL_22")

# COMMAND ----------

# DBTITLE 1, JNR_ZTPIM_MAP_23

df_23=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        DETAIL.KVI_CODE AS KVI_CODE,
        DETAIL.KVI_NAME AS KVI_NAME,
        DETAIL.PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
        DETAIL.PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
        DETAIL.SP_GRP_ID AS SP_GRP_ID,
        DETAIL.SP_GRP_NAME AS SP_GRP_NAME,
        DETAIL.PLG_NUMBER AS PLG_NUMBER,
        DETAIL.PLG_DESCRIPTION AS PLG_DESCRIPTION,
        DETAIL.TIER AS TIER,
        DETAIL.PARENT_TIER AS PARENT_TIER,
        MASTER.o_MAP_PRICE AS MAP_PRICE,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_ZM_ARTICLE_CHANGE_TO_INTEGER_5 MASTER 
    LEFT JOIN
        JNR_ZTB_PR_LABEL_22 DETAIL 
            ON ARTICLE = DETAIL.SKU_NBR""")

df_23.createOrReplaceTempView("JNR_ZTPIM_MAP_23")

# COMMAND ----------

# DBTITLE 1, Jnr_SAP_KOI_24

df_24=spark.sql("""
    SELECT
        DETAIL.PRODUCT_ID AS PRODUCT_ID,
        DETAIL.SKU_NBR AS SKU_NBR,
        DETAIL.KVI_CODE AS KVI_CODE,
        DETAIL.KVI_NAME AS KVI_NAME,
        DETAIL.PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
        DETAIL.PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
        DETAIL.SP_GRP_ID AS SP_GRP_ID,
        DETAIL.SP_GRP_NAME AS SP_GRP_NAME,
        DETAIL.PLG_NUMBER AS PLG_NUMBER,
        DETAIL.PLG_DESCRIPTION AS PLG_DESCRIPTION,
        DETAIL.TIER AS TIER,
        DETAIL.PARENT_TIER AS PARENT_TIER,
        DETAIL.MAP_PRICE AS MAP_PRICE,
        MASTER.o_MATNR AS o_MATNR,
        MASTER.SKU_SAP_KOI_ID AS SKU_SAP_KOI_ID,
        MASTER.SKU_SAP_KOI_DESC AS SKU_SAP_KOI_DESC,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_SAP_KOI_2 MASTER 
    LEFT JOIN
        JNR_ZTPIM_MAP_23 DETAIL 
            ON MASTER.o_MATNR = DETAIL.SKU_NBR""")

df_24.createOrReplaceTempView("Jnr_SAP_KOI_24")

# COMMAND ----------

# DBTITLE 1, EXP_LOAD_TSTMP_25

df_25=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        KVI_CODE AS KVI_CODE,
        KVI_NAME AS KVI_NAME,
        PRICE_FAMILY_NO AS PRICE_FAMILY_NO,
        PRICE_FAMILY_NAME AS PRICE_FAMILY_NAME,
        SP_GRP_ID AS SP_GRP_ID,
        SP_GRP_NAME AS SP_GRP_NAME,
        PLG_NUMBER AS PLG_NUMBER,
        PLG_DESCRIPTION AS PLG_DESCRIPTION,
        TIER AS TIER,
        PARENT_TIER AS PARENT_TIER,
        MAP_PRICE AS MAP_PRICE,
        SKU_SAP_KOI_ID AS SKU_SAP_KOI_ID,
        SKU_SAP_KOI_DESC AS SKU_SAP_KOI_DESC,
        SYSTIMESTAMP() AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_SAP_KOI_24""")

df_25.createOrReplaceTempView("EXP_LOAD_TSTMP_25")

# COMMAND ----------

# DBTITLE 1, SKU_SAP_ATTR_PRE

spark.sql("""INSERT INTO SKU_SAP_ATTR_PRE SELECT PRODUCT_ID AS PRODUCT_ID,
SKU_NBR AS SKU_NBR,
KVI_CODE AS SKU_SAP_PROD_GROUP_ID,
KVI_NAME AS SKU_SAP_PROD_GROUP_DESC,
PRICE_FAMILY_NO AS SKU_SAP_FAMILY_ID,
PRICE_FAMILY_NAME AS SKU_SAP_FAMILY_DESC,
SP_GRP_ID AS SKU_SAP_PPU_GROUP_ID,
SP_GRP_NAME AS SKU_SAP_PPU_GROUP_DESC,
PLG_NUMBER AS SKU_SAP_PRVT_LABEL_ID,
PLG_DESCRIPTION AS SKU_SAP_PRVT_LABEL_DESC,
SKU_SAP_PRVT_LABEL_TIER_ID AS SKU_SAP_PRVT_LABEL_TIER_ID,
TIER AS SKU_SAP_PRVT_LABEL_TIER_DESC,
SKU_SAP_PRVT_LABEL_PARENT_TIER_ID AS SKU_SAP_PRVT_LABEL_PARENT_TIER_ID,
PARENT_TIER AS SKU_SAP_PRVT_LABEL_PARENT_TIER_DESC,
SKU_SAP_MAP_ID AS SKU_SAP_MAP_ID,
MAP_PRICE AS SKU_SAP_MAP_DESC,
SKU_SAP_KOI_ID AS SKU_SAP_KOI_ID,
SKU_SAP_KOI_DESC AS SKU_SAP_KOI_DESC,
LOAD_TSTMP AS LOAD_TSTMP FROM EXP_LOAD_TSTMP_25""")