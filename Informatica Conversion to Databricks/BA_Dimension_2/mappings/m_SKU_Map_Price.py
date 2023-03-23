# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_MAP_PRICE_0


df_0=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        VALID_FROM_DT AS VALID_FROM_DT,
        VALID_TO_DT AS VALID_TO_DT,
        MAP_PRICE_AMT AS MAP_PRICE_AMT,
        DELETE_FLAG AS DELETE_FLAG,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_DT AS LAST_CHANGED_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_MAP_PRICE""")

df_0.createOrReplaceTempView("SKU_MAP_PRICE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_MAP_PRICE_1


df_1=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        VALID_FROM_DT AS VALID_FROM_DT,
        VALID_TO_DT AS VALID_TO_DT,
        MAP_PRICE_AMT AS MAP_PRICE_AMT,
        DELETE_FLAG AS DELETE_FLAG,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_DT AS LAST_CHANGED_DT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_MAP_PRICE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_MAP_PRICE_1")

# COMMAND ----------
# DBTITLE 1, SAP_ZTPIM_MAP_PRE_2


df_2=spark.sql("""
    SELECT
        MANDT AS MANDT,
        ARTICLE AS ARTICLE,
        MAP_PRICE AS MAP_PRICE,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
        DATAB AS DATAB,
        DATBI AS DATBI,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SAP_ZTPIM_MAP_PRE""")

df_2.createOrReplaceTempView("SAP_ZTPIM_MAP_PRE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SAP_ZTPIM_MAP_PRE_3


df_3=spark.sql("""
    SELECT
        MANDT AS MANDT,
        ARTICLE AS ARTICLE,
        MAP_PRICE AS MAP_PRICE,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
        DATAB AS DATAB,
        DATBI AS DATBI,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SAP_ZTPIM_MAP_PRE_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SAP_ZTPIM_MAP_PRE_3")

# COMMAND ----------
# DBTITLE 1, EXP_INT_Conversion_4


df_4=spark.sql("""
    SELECT
        MANDT AS MANDT,
        (CAST(ARTICLE AS DECIMAL (38,
        0))) AS o_ARTICLE,
        MAP_PRICE AS MAP_PRICE,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        date_trunc('DAY',
        TO_DATE(LAST_CHANGED_DATE,
        'MM/DD/YYYY')) AS o_LAST_CHANGED_DATE,
        date_trunc('DAY',
        to_date(DATAB,
        'MM/DD/YYYY')) AS o_DATAB,
        date_trunc('DAY',
        to_date(DATBI,
        'MM/DD/YYYY')) AS o_DATBI,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SAP_ZTPIM_MAP_PRE_3""")

df_4.createOrReplaceTempView("EXP_INT_Conversion_4")

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
        SKU_NBR AS SKU_NBR,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE_5""")

df_6.createOrReplaceTempView("SQ_Shortcut_To_SKU_PROFILE_6")

# COMMAND ----------
# DBTITLE 1, JNR_SKU_PROFILE_7


df_7=spark.sql("""
    SELECT
        DETAIL.MANDT AS MANDT,
        DETAIL.o_ARTICLE AS o_ARTICLE,
        DETAIL.MAP_PRICE AS MAP_PRICE,
        DETAIL.CREATED_BY AS CREATED_BY,
        DETAIL.LAST_CHANGED_BY AS LAST_CHANGED_BY,
        DETAIL.o_LAST_CHANGED_DATE AS o_LAST_CHANGED_DATE,
        DETAIL.o_DATAB AS o_DATAB,
        DETAIL.o_DATBI AS o_DATBI,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.PRODUCT_ID AS PRODUCT_ID,
        MASTER.SKU_NBR AS SKU_NBR,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_To_SKU_PROFILE_6 MASTER 
    INNER JOIN
        EXP_INT_Conversion_4 DETAIL 
            ON MASTER.SKU_NBR = DETAIL.o_ARTICLE""")

df_7.createOrReplaceTempView("JNR_SKU_PROFILE_7")

# COMMAND ----------
# DBTITLE 1, JNR_SKU_Map_Price_8


df_8=spark.sql("""
    SELECT
        DETAIL.MANDT AS in_MANDT,
        DETAIL.MAP_PRICE AS in_MAP_PRICE,
        DETAIL.CREATED_BY AS in_CREATED_BY,
        DETAIL.LAST_CHANGED_BY AS in_LAST_CHANGED_BY,
        DETAIL.o_LAST_CHANGED_DATE AS o_LAST_CHANGED_DATE,
        DETAIL.o_DATAB AS o_DATAB,
        DETAIL.o_DATBI AS o_DATBI,
        DETAIL.LOAD_TSTMP AS in_LOAD_TSTMP,
        DETAIL.PRODUCT_ID AS in_PRODUCT_ID,
        MASTER.PRODUCT_ID AS PRODUCT_ID,
        MASTER.VALID_FROM_DT AS VALID_FROM_DT,
        MASTER.VALID_TO_DT AS VALID_TO_DT,
        MASTER.MAP_PRICE_AMT AS MAP_PRICE_AMT,
        MASTER.DELETE_FLAG AS DELETE_FLAG,
        MASTER.CREATED_BY AS CREATED_BY,
        MASTER.LAST_CHANGED_BY AS LAST_CHANGED_BY,
        MASTER.LAST_CHANGED_DT AS LAST_CHANGED_DT,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_MAP_PRICE_1 MASTER 
    LEFT JOIN
        JNR_SKU_PROFILE_7 DETAIL 
            ON MASTER.PRODUCT_ID = in_PRODUCT_ID 
            AND VALID_FROM_DT = DETAIL.o_DATAB""")

df_8.createOrReplaceTempView("JNR_SKU_Map_Price_8")

# COMMAND ----------
# DBTITLE 1, FTR_No_Changed_Rec_9


df_9=spark.sql("""
    SELECT
        in_MANDT AS in_MANDT,
        in_MAP_PRICE AS in_MAP_PRICE,
        in_CREATED_BY AS in_CREATED_BY,
        in_LAST_CHANGED_BY AS in_LAST_CHANGED_BY,
        o_LAST_CHANGED_DATE AS o_LAST_CHANGED_DATE,
        o_DATAB AS o_DATAB,
        o_DATBI AS o_DATBI,
        in_LOAD_TSTMP AS in_LOAD_TSTMP,
        in_PRODUCT_ID AS in_PRODUCT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        VALID_FROM_DT AS VALID_FROM_DT,
        VALID_TO_DT AS VALID_TO_DT,
        MAP_PRICE_AMT AS MAP_PRICE_AMT,
        DELETE_FLAG AS DELETE_FLAG,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_DT AS LAST_CHANGED_DT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_SKU_Map_Price_8 
    WHERE
        ISNULL(in_PRODUCT_ID) 
        OR ISNULL(PRODUCT_ID) 
        OR (
            (
                NOT ISNULL(PRODUCT_ID)
            ) 
            AND (
                IFF(ISNULL(in_MAP_PRICE), 9, in_MAP_PRICE) <> IFF(ISNULL(MAP_PRICE_AMT), 9, MAP_PRICE_AMT) 
                OR IFF(ISNULL(LTRIM(RTRIM(in_CREATED_BY))), 'S', LTRIM(RTRIM(in_CREATED_BY))) <> IFF(ISNULL(LTRIM(RTRIM(CREATED_BY))), 'S', LTRIM(RTRIM(CREATED_BY))) 
                OR IFF(ISNULL(LTRIM(RTRIM(in_LAST_CHANGED_BY))), 'S', LTRIM(RTRIM(in_LAST_CHANGED_BY))) <> IFF(ISNULL(LTRIM(RTRIM(LAST_CHANGED_BY))), 'S', LTRIM(RTRIM(LAST_CHANGED_BY))) 
                OR IFF(ISNULL(o_LAST_CHANGED_DATE), TO_DATE('01/01/1900', 'MM/DD/YYYY'), o_LAST_CHANGED_DATE) <> IFF(ISNULL(LAST_CHANGED_DT), TO_DATE('01/01/1900', 'MM/DD/YYYY'), LAST_CHANGED_DT) 
                OR IFF(ISNULL(o_DATAB), TO_DATE('01/01/1900', 'MM/DD/YYYY'), o_DATAB) <> IFF(ISNULL(VALID_FROM_DT), TO_DATE('01/01/1900', 'MM/DD/YYYY'), VALID_FROM_DT) 
                OR IFF(ISNULL(o_DATBI), TO_DATE('01/01/1900', 'MM/DD/YYYY'), o_DATBI) <> IFF(ISNULL(VALID_TO_DT), TO_DATE('01/01/1900', 'MM/DD/YYYY'), VALID_TO_DT)
            )
        )""")

df_9.createOrReplaceTempView("FTR_No_Changed_Rec_9")

# COMMAND ----------
# DBTITLE 1, EXP_INS_UPD_FLAG_10


df_10=spark.sql("""
    SELECT
        in_MAP_PRICE AS in_MAP_PRICE,
        in_CREATED_BY AS in_CREATED_BY,
        in_LAST_CHANGED_BY AS in_LAST_CHANGED_BY,
        o_LAST_CHANGED_DATE AS o_LAST_CHANGED_DATE,
        o_DATAB AS o_DATAB,
        o_DATBI AS o_DATBI,
        in_LOAD_TSTMP AS in_LOAD_TSTMP,
        in_PRODUCT_ID AS in_PRODUCT_ID,
        PRODUCT_ID AS PRODUCT_ID,
        VALID_FROM_DT AS VALID_FROM_DT,
        VALID_TO_DT AS VALID_TO_DT,
        MAP_PRICE_AMT AS MAP_PRICE_AMT,
        DELETE_FLAG AS DELETE_FLAG,
        CREATED_BY AS CREATED_BY,
        LAST_CHANGED_BY AS LAST_CHANGED_BY,
        LAST_CHANGED_DT AS LAST_CHANGED_DT,
        IFF(ISNULL(in_PRODUCT_ID) 
        AND NOT ISNULL(PRODUCT_ID),
        1,
        0) AS exp_DELETE_FLAG,
        IFF(ISNULL(in_LOAD_TSTMP),
        current_timestamp,
        in_LOAD_TSTMP) AS LOAD_TSTMP,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(PRODUCT_ID) 
        AND NOT ISNULL(in_PRODUCT_ID),
        'INSERT',
        IFF(ISNULL(in_PRODUCT_ID) 
        AND NOT ISNULL(PRODUCT_ID),
        'DELETE',
        IIF())) AS UPD_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FTR_No_Changed_Rec_9""")

df_10.createOrReplaceTempView("EXP_INS_UPD_FLAG_10")

# COMMAND ----------
# DBTITLE 1, UPD_DELETE


# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD


# COMMAND ----------
# DBTITLE 1, SKU_MAP_PRICE


# COMMAND ----------
# DBTITLE 1, SKU_MAP_PRICE
