# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, LABEL_POG_TYPE_0


df_0=spark.sql("""
    SELECT
        LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
        LABEL_POG_TYPE_DESC AS LABEL_POG_TYPE_DESC,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        LABEL_POG_TYPE""")

df_0.createOrReplaceTempView("LABEL_POG_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LABEL_POG_TYPE_1


df_1=spark.sql("""
    SELECT
        LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LABEL_POG_TYPE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_LABEL_POG_TYPE_1")

# COMMAND ----------
# DBTITLE 1, LABEL_DAY_STORE_SKU_2


df_2=spark.sql("""
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

df_2.createOrReplaceTempView("LABEL_DAY_STORE_SKU_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LABEL_DAY_STORE_SKU_3


df_3=spark.sql("""
    SELECT
        LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        LABEL_DAY_STORE_SKU_2 
    WHERE
        LABEL_DAY_STORE_SKU.LOAD_TSTMP > CURRENT_DATE""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_LABEL_DAY_STORE_SKU_3")

# COMMAND ----------
# DBTITLE 1, JNR_Label_Pog_Type_4


df_4=spark.sql("""
    SELECT
        DETAIL.LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
        MASTER.LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD1,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_LABEL_POG_TYPE_1 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_LABEL_DAY_STORE_SKU_3 DETAIL 
            ON MASTER.LABEL_POG_TYPE_CD = DETAIL.LABEL_POG_TYPE_CD""")

df_4.createOrReplaceTempView("JNR_Label_Pog_Type_4")

# COMMAND ----------
# DBTITLE 1, FIL_Label_Pog_Type_5


df_5=spark.sql("""
    SELECT
        LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
        LABEL_POG_TYPE_CD1 AS LABEL_POG_TYPE_CD1,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        JNR_Label_Pog_Type_4 
    WHERE
        ISNULL(LABEL_POG_TYPE_CD)""")

df_5.createOrReplaceTempView("FIL_Label_Pog_Type_5")

# COMMAND ----------
# DBTITLE 1, EXP_Set_Label_Pog_Type_6


df_6=spark.sql("""
    SELECT
        LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
        'Unavailable_' || LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_DESC,
        current_timestamp AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FIL_Label_Pog_Type_5""")

df_6.createOrReplaceTempView("EXP_Set_Label_Pog_Type_6")

# COMMAND ----------
# DBTITLE 1, LABEL_POG_TYPE


spark.sql("""INSERT INTO LABEL_POG_TYPE SELECT LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
LABEL_POG_TYPE_DESC AS LABEL_POG_TYPE_DESC,
LOAD_TSTMP AS LOAD_TSTMP FROM EXP_Set_Label_Pog_Type_6""")