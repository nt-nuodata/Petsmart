# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ZTB_ADV_LBL_CHGS_PRE_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        EFFECTIVE_DATE AS EFFECTIVE_DATE,
        ARTICLE AS ARTICLE,
        SITE AS SITE,
        POG_TYPE AS POG_TYPE,
        LABEL_SIZE AS LABEL_SIZE,
        LABEL_TYPE AS LABEL_TYPE,
        EXP_LABEL_TYPE AS EXP_LABEL_TYPE,
        SUPPRESS_IND AS SUPPRESS_IND,
        NUM_LABELS AS NUM_LABELS,
        CREATE_DATE AS CREATE_DATE,
        ENH_LBL_ID AS ENH_LBL_ID,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_ADV_LBL_CHGS_PRE""")

df_0.createOrReplaceTempView("ZTB_ADV_LBL_CHGS_PRE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE_1

df_1=spark.sql("""
    SELECT
        MANDT AS MANDT,
        EFFECTIVE_DATE AS EFFECTIVE_DATE,
        ARTICLE AS ARTICLE,
        SITE AS SITE,
        POG_TYPE AS POG_TYPE,
        LABEL_SIZE AS LABEL_SIZE,
        LABEL_TYPE AS LABEL_TYPE,
        EXP_LABEL_TYPE AS EXP_LABEL_TYPE,
        SUPPRESS_IND AS SUPPRESS_IND,
        NUM_LABELS AS NUM_LABELS,
        CREATE_DATE AS CREATE_DATE,
        ENH_LBL_ID AS ENH_LBL_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ZTB_ADV_LBL_CHGS_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE_1")

# COMMAND ----------

# DBTITLE 1, EXP_LBL_FIELDS_CONVERSION_2

df_2=spark.sql("""
    SELECT
        TO_DATE(EFFECTIVE_DATE,
        'YYYYMMDD') AS LABEL_CHANGE_DT,
        (CAST(SITE AS DECIMAL (38,
        0))) AS STORE_NBR,
        (CAST(ARTICLE AS DECIMAL (38,
        0))) AS SKU_NBR,
        0 AS ACTUAL_FLAG,
        POG_TYPE AS POG_TYPE,
        LABEL_SIZE AS LABEL_SIZE,
        IFF(IS_NUMBER(LTRIM(RTRIM(ENH_LBL_ID))),
        (CAST(LTRIM(RTRIM(ENH_LBL_ID)) AS DECIMAL (38,
        0))),
        (CAST(LABEL_TYPE AS DECIMAL (38,
        0)))) AS LABEL_TYPE,
        IFF(EXP_LABEL_TYPE = 'X',
        1,
        0) AS EXPIRATION_FLAG,
        IFF(SUPPRESS_IND = 'X',
        1,
        0) AS SUPPRESSED_FLAG,
        (CAST(NUM_LABELS AS DECIMAL (38,
        0))) AS LABEL_CNT,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_PRE_1""")

df_2.createOrReplaceTempView("EXP_LBL_FIELDS_CONVERSION_2")

# COMMAND ----------

# DBTITLE 1, LKP_GET_LOCATION_ID_3

df_3=spark.sql("""
    SELECT
        LOCATION_ID AS LOCATION_ID,
        LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
        STORE_NBR AS STORE_NBR,
        EXP_LBL_FIELDS_CONVERSION_2.STORE_NBR AS STORE_NBR1,
        EXP_LBL_FIELDS_CONVERSION_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SITE_PROFILE 
    RIGHT OUTER JOIN
        EXP_LBL_FIELDS_CONVERSION_2 
            ON STORE_NBR = EXP_LBL_FIELDS_CONVERSION_2.STORE_NBR""")

df_3.createOrReplaceTempView("LKP_GET_LOCATION_ID_3")

# COMMAND ----------

# DBTITLE 1, LKP_GET_PRODUCT_ID_4

df_4=spark.sql("""
    SELECT
        PRODUCT_ID AS PRODUCT_ID,
        SKU_NBR AS SKU_NBR,
        EXP_LBL_FIELDS_CONVERSION_2.SKU_NBR AS SKU_NBR1,
        EXP_LBL_FIELDS_CONVERSION_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PROFILE 
    RIGHT OUTER JOIN
        EXP_LBL_FIELDS_CONVERSION_2 
            ON SKU_NBR = EXP_LBL_FIELDS_CONVERSION_2.SKU_NBR""")

df_4.createOrReplaceTempView("LKP_GET_PRODUCT_ID_4")

# COMMAND ----------

# DBTITLE 1, LKP_GET_TIME_DIMENSIONS_5

df_5=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_YR AS FISCAL_YR,
        WEEK_DT AS WEEK_DT,
        EXP_LBL_FIELDS_CONVERSION_2.LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
        EXP_LBL_FIELDS_CONVERSION_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        DAYS 
    RIGHT OUTER JOIN
        EXP_LBL_FIELDS_CONVERSION_2 
            ON DAY_DT = EXP_LBL_FIELDS_CONVERSION_2.LABEL_CHANGE_DT""")

df_5.createOrReplaceTempView("LKP_GET_TIME_DIMENSIONS_5")

# COMMAND ----------

# DBTITLE 1, EXP_LOAD_TSTMP_6

df_6=spark.sql("""
    SELECT
        EXP_LBL_FIELDS_CONVERSION_2.LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
        LKP_GET_LOCATION_ID_3.LOCATION_ID AS LOCATION_ID,
        LKP_GET_PRODUCT_ID_4.PRODUCT_ID AS PRODUCT_ID,
        EXP_LBL_FIELDS_CONVERSION_2.ACTUAL_FLAG AS ACTUAL_FLAG,
        EXP_LBL_FIELDS_CONVERSION_2.POG_TYPE AS POG_TYPE,
        EXP_LBL_FIELDS_CONVERSION_2.LABEL_SIZE AS LABEL_SIZE,
        EXP_LBL_FIELDS_CONVERSION_2.LABEL_TYPE AS LABEL_TYPE,
        EXP_LBL_FIELDS_CONVERSION_2.EXPIRATION_FLAG AS EXPIRATION_FLAG,
        EXP_LBL_FIELDS_CONVERSION_2.SKU_NBR AS SKU_NBR,
        EXP_LBL_FIELDS_CONVERSION_2.STORE_NBR AS STORE_NBR,
        LKP_GET_TIME_DIMENSIONS_5.WEEK_DT AS WEEK_DT,
        LKP_GET_TIME_DIMENSIONS_5.FISCAL_WK AS FISCAL_WK,
        LKP_GET_TIME_DIMENSIONS_5.FISCAL_MO AS FISCAL_MO,
        LKP_GET_TIME_DIMENSIONS_5.FISCAL_YR AS FISCAL_YR,
        EXP_LBL_FIELDS_CONVERSION_2.SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
        EXP_LBL_FIELDS_CONVERSION_2.LABEL_CNT AS LABEL_CNT,
        current_timestamp AS UPDATE_TSTMP,
        LKP_GET_TIME_DIMENSIONS_5.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LKP_GET_TIME_DIMENSIONS_5 
    INNER JOIN
        LKP_GET_PRODUCT_ID_4 
            ON LKP_GET_TIME_DIMENSIONS_5.Monotonically_Increasing_Id = LKP_GET_PRODUCT_ID_4.Monotonically_Increasing_Id 
    INNER JOIN
        LKP_GET_LOCATION_ID_3 
            ON LKP_GET_TIME_DIMENSIONS_5.Monotonically_Increasing_Id = LKP_GET_LOCATION_ID_3.Monotonically_Increasing_Id 
    INNER JOIN
        EXP_LBL_FIELDS_CONVERSION_2 
            ON LKP_GET_TIME_DIMENSIONS_5.Monotonically_Increasing_Id = EXP_LBL_FIELDS_CONVERSION_2.Monotonically_Increasing_Id""")

df_6.createOrReplaceTempView("EXP_LOAD_TSTMP_6")

# COMMAND ----------

# DBTITLE 1, LABEL_DAY_STORE_SKU_7

df_7=spark.sql("""
    SELECT
        LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
        LOCATION_ID AS LOCATION_ID,
        PRODUCT_ID AS PRODUCT_ID,
        ACTUAL_FLAG AS ACTUAL_FLAG,
        LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
        LABEL_SIZE_ID AS LABEL_SIZE_ID,
        LABEL_TYPE_ID AS LABEL_TYPE_ID,
        EXPIRATION_FLAG AS EXPIRATION_FLAG,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        WEEK_DT AS WEEK_DT,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_YR AS FISCAL_YR,
        SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
        LABEL_CNT AS LABEL_CNT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LABEL_DAY_STORE_SKU""")

df_7.createOrReplaceTempView("LABEL_DAY_STORE_SKU_7")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_LABEL_DAY_STORE_SKU_8

df_8=spark.sql("""
    SELECT
        LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
        LOCATION_ID AS LOCATION_ID,
        PRODUCT_ID AS PRODUCT_ID,
        ACTUAL_FLAG AS ACTUAL_FLAG,
        LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
        LABEL_SIZE_ID AS LABEL_SIZE_ID,
        LABEL_TYPE_ID AS LABEL_TYPE_ID,
        EXPIRATION_FLAG AS EXPIRATION_FLAG,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        WEEK_DT AS WEEK_DT,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_YR AS FISCAL_YR,
        SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
        LABEL_CNT AS LABEL_CNT,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LABEL_DAY_STORE_SKU_7 
    WHERE
        LABEL_DAY_STORE_SKU.ACTUAL_FLAG = 0 
        AND LABEL_DAY_STORE_SKU.LOAD_TSTMP > CURRENT_DATE - 15""")

df_8.createOrReplaceTempView("SQ_Shortcut_to_LABEL_DAY_STORE_SKU_8")

# COMMAND ----------

# DBTITLE 1, JNR_LABEL_DAY_STORE_SKU__ZTB_9

df_9=spark.sql("""
    SELECT
        MASTER.LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
        MASTER.LOCATION_ID AS LOCATION_ID,
        MASTER.PRODUCT_ID AS PRODUCT_ID,
        MASTER.ACTUAL_FLAG AS ACTUAL_FLAG,
        MASTER.POG_TYPE AS POG_TYPE,
        MASTER.LABEL_SIZE AS LABEL_SIZE,
        MASTER.LABEL_TYPE AS LABEL_TYPE,
        MASTER.EXPIRATION_FLAG AS EXPIRATION_FLAG,
        MASTER.SKU_NBR AS SKU_NBR,
        MASTER.STORE_NBR AS STORE_NBR,
        MASTER.WEEK_DT AS WEEK_DT,
        MASTER.FISCAL_WK AS FISCAL_WK,
        MASTER.FISCAL_MO AS FISCAL_MO,
        MASTER.FISCAL_YR AS FISCAL_YR,
        MASTER.SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
        MASTER.LABEL_CNT AS LABEL_CNT,
        MASTER.UPDATE_TSTMP AS UPDATE_TSTMP,
        DETAIL.LABEL_CHANGE_DT AS LABEL_CHANGE_DT_OLD,
        DETAIL.LOCATION_ID AS LOCATION_ID_OLD,
        DETAIL.PRODUCT_ID AS PRODUCT_ID_OLD,
        DETAIL.ACTUAL_FLAG AS ACTUAL_FLAG_OLD,
        DETAIL.LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD_OLD,
        DETAIL.LABEL_SIZE_ID AS LABEL_SIZE_ID_OLD,
        DETAIL.LABEL_TYPE_ID AS LABEL_TYPE_ID_OLD,
        DETAIL.EXPIRATION_FLAG AS EXPIRATION_FLAG_OLD,
        DETAIL.SKU_NBR AS SKU_NBR_OLD,
        DETAIL.STORE_NBR AS STORE_NBR_OLD,
        DETAIL.WEEK_DT AS WEEK_DT_OLD,
        DETAIL.FISCAL_WK AS FISCAL_WK_OLD,
        DETAIL.FISCAL_MO AS FISCAL_MO_OLD,
        DETAIL.FISCAL_YR AS FISCAL_YR_OLD,
        DETAIL.SUPPRESSED_FLAG AS SUPPRESSED_FLAG_OLD,
        DETAIL.LABEL_CNT AS LABEL_CNT_OLD,
        DETAIL.LOAD_TSTMP AS LOAD_TSTMP_OLD,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_LOAD_TSTMP_6 MASTER 
    RIGHT JOIN
        SQ_Shortcut_to_LABEL_DAY_STORE_SKU_8 DETAIL 
            ON MASTER.LABEL_CHANGE_DT = LABEL_CHANGE_DT_OLD 
            AND LOCATION_ID = LOCATION_ID_OLD 
            AND PRODUCT_ID = PRODUCT_ID_OLD 
            AND ACTUAL_FLAG = ACTUAL_FLAG_OLD 
            AND POG_TYPE = LABEL_POG_TYPE_CD_OLD 
            AND LABEL_SIZE = LABEL_SIZE_ID_OLD 
            AND LABEL_TYPE = LABEL_TYPE_ID_OLD 
            AND EXPIRATION_FLAG = DETAIL.EXPIRATION_FLAG""")

df_9.createOrReplaceTempView("JNR_LABEL_DAY_STORE_SKU__ZTB_9")

# COMMAND ----------

# DBTITLE 1, EXP_MD5_10

df_10=spark.sql("""
    SELECT
        LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
        LOCATION_ID AS LOCATION_ID,
        PRODUCT_ID AS PRODUCT_ID,
        ACTUAL_FLAG AS ACTUAL_FLAG,
        POG_TYPE AS POG_TYPE,
        LABEL_SIZE AS LABEL_SIZE,
        LABEL_TYPE AS LABEL_TYPE,
        EXPIRATION_FLAG AS EXPIRATION_FLAG,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        WEEK_DT AS WEEK_DT,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_YR AS FISCAL_YR,
        SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
        LABEL_CNT AS LABEL_CNT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        IFF(ISNULL(LABEL_CHANGE_DT_OLD),
        current_timestamp,
        LOAD_TSTMP_OLD) AS LOAD_TSTMP,
        IFF(ISNULL(LABEL_CHANGE_DT_OLD),
        DD_INSERT,
        IFF(MD5_PRE <> MD5_OLD,
        DD_UPDATE,
        DD_REJECT)) AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_LABEL_DAY_STORE_SKU__ZTB_9""")

df_10.createOrReplaceTempView("EXP_MD5_10")

# COMMAND ----------

# DBTITLE 1, FIL_INS_UPD_11

df_11=spark.sql("""
    SELECT
        LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
        LOCATION_ID AS LOCATION_ID,
        PRODUCT_ID AS PRODUCT_ID,
        ACTUAL_FLAG AS ACTUAL_FLAG,
        POG_TYPE AS POG_TYPE,
        LABEL_SIZE AS LABEL_SIZE,
        LABEL_TYPE AS LABEL_TYPE,
        EXPIRATION_FLAG AS EXPIRATION_FLAG,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        WEEK_DT AS WEEK_DT,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_YR AS FISCAL_YR,
        SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
        LABEL_CNT AS LABEL_CNT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        UPDATE_STRATEGY AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXP_MD5_10 
    WHERE
        UPDATE_STRATEGY <> DD_REJECT""")

df_11.createOrReplaceTempView("FIL_INS_UPD_11")

# COMMAND ----------

# DBTITLE 1, UPS_LABEL_DAY_STORE_SKU_12

df_12=spark.sql("""
    SELECT
        LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
        LOCATION_ID AS LOCATION_ID,
        PRODUCT_ID AS PRODUCT_ID,
        ACTUAL_FLAG AS ACTUAL_FLAG,
        POG_TYPE AS POG_TYPE,
        LABEL_SIZE AS LABEL_SIZE,
        LABEL_TYPE AS LABEL_TYPE,
        EXPIRATION_FLAG AS EXPIRATION_FLAG,
        SKU_NBR AS SKU_NBR,
        STORE_NBR AS STORE_NBR,
        WEEK_DT AS WEEK_DT,
        FISCAL_WK AS FISCAL_WK,
        FISCAL_MO AS FISCAL_MO,
        FISCAL_YR AS FISCAL_YR,
        SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
        LABEL_CNT AS LABEL_CNT,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        UPDATE_STRATEGY AS UPDATE_STRATEGY,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_INS_UPD_11""")

df_12.createOrReplaceTempView("UPS_LABEL_DAY_STORE_SKU_12")

# COMMAND ----------

# DBTITLE 1, LABEL_DAY_STORE_SKU

spark.sql("""INSERT INTO LABEL_DAY_STORE_SKU SELECT LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
LOCATION_ID AS LOCATION_ID,
PRODUCT_ID AS PRODUCT_ID,
ACTUAL_FLAG AS ACTUAL_FLAG,
POG_TYPE AS LABEL_POG_TYPE_CD,
LABEL_SIZE AS LABEL_SIZE_ID,
LABEL_TYPE AS LABEL_TYPE_ID,
EXPIRATION_FLAG AS EXPIRATION_FLAG,
SKU_NBR AS SKU_NBR,
STORE_NBR AS STORE_NBR,
WEEK_DT AS WEEK_DT,
FISCAL_WK AS FISCAL_WK,
FISCAL_MO AS FISCAL_MO,
FISCAL_YR AS FISCAL_YR,
SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
LABEL_CNT AS LABEL_CNT,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM UPS_LABEL_DAY_STORE_SKU_12""")
