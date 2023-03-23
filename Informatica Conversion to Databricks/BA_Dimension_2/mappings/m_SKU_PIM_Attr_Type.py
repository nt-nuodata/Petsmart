# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SKU_PIM_ATTR_TYPE_PRE_0

df_0=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_TAG AS PIM_ATTR_TAG,
        PIM_ATTR_NAME AS PIM_ATTR_NAME,
        PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
        SLICING_ATTR_IND AS SLICING_ATTR_IND,
        MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        SIZE_ATTR_IND AS SIZE_ATTR_IND,
        DEL_IND AS DEL_IND,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_TYPE_PRE""")

df_0.createOrReplaceTempView("SKU_PIM_ATTR_TYPE_PRE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_PRE_1

df_1=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_TAG AS PIM_ATTR_TAG,
        PIM_ATTR_NAME AS PIM_ATTR_NAME,
        PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
        SLICING_ATTR_IND AS SLICING_ATTR_IND,
        MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        SIZE_ATTR_IND AS SIZE_ATTR_IND,
        DEL_IND AS DEL_IND,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_TYPE_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_PRE_1")

# COMMAND ----------

# DBTITLE 1, SKU_PIM_ATTR_TYPE_2

df_2=spark.sql("""
    SELECT
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_TAG AS SKU_PIM_ATTR_TYPE_TAG,
        SKU_PIM_ATTR_TYPE_NAME AS SKU_PIM_ATTR_TYPE_NAME,
        SKU_PIM_ATTR_TYPE_DISPLAY_NAME AS SKU_PIM_ATTR_TYPE_DISPLAY_NAME,
        SLICING_ATTR_IND AS SLICING_ATTR_IND,
        MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        SIZE_ATTR_IND AS SIZE_ATTR_IND,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_TYPE""")

df_2.createOrReplaceTempView("SKU_PIM_ATTR_TYPE_2")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_3

df_3=spark.sql("""
    SELECT
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_TAG AS SKU_PIM_ATTR_TYPE_TAG,
        SKU_PIM_ATTR_TYPE_NAME AS SKU_PIM_ATTR_TYPE_NAME,
        SKU_PIM_ATTR_TYPE_DISPLAY_NAME AS SKU_PIM_ATTR_TYPE_DISPLAY_NAME,
        SLICING_ATTR_IND AS SLICING_ATTR_IND,
        MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        SIZE_ATTR_IND AS SIZE_ATTR_IND,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_TYPE_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_3")

# COMMAND ----------

# DBTITLE 1, Jnr_SKU_PIM_Attr_Type_4

df_4=spark.sql("""
    SELECT
        DETAIL.PIM_ATTR_ID AS PIM_ATTR_ID,
        DETAIL.PIM_ATTR_TAG AS PIM_ATTR_TAG,
        DETAIL.PIM_ATTR_NAME AS PIM_ATTR_NAME,
        DETAIL.PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
        DETAIL.SLICING_ATTR_IND AS SLICING_ATTR_IND,
        DETAIL.MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        DETAIL.INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        DETAIL.SIZE_ATTR_IND AS SIZE_ATTR_IND,
        DETAIL.DEL_IND AS DEL_IND,
        MASTER.SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        MASTER.SKU_PIM_ATTR_TYPE_TAG AS SKU_PIM_ATTR_TYPE_TAG,
        MASTER.SKU_PIM_ATTR_TYPE_NAME AS SKU_PIM_ATTR_TYPE_NAME,
        MASTER.SKU_PIM_ATTR_TYPE_DISPLAY_NAME AS SKU_PIM_ATTR_TYPE_DISPLAY_NAME,
        MASTER.SLICING_ATTR_IND AS SLICING_ATTR_IND1,
        MASTER.MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND1,
        MASTER.INTERNAL_AUD_IND AS INTERNAL_AUD_IND1,
        MASTER.SIZE_ATTR_IND AS SIZE_ATTR_IND1,
        MASTER.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_3 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_PRE_1 DETAIL 
            ON MASTER.SKU_PIM_ATTR_TYPE_ID = DETAIL.PIM_ATTR_ID""")

df_4.createOrReplaceTempView("Jnr_SKU_PIM_Attr_Type_4")

# COMMAND ----------

# DBTITLE 1, Fil_SKU_PIM_Attr_Type_5

df_5=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_TAG AS PIM_ATTR_TAG,
        PIM_ATTR_NAME AS PIM_ATTR_NAME,
        PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
        SLICING_ATTR_IND AS SLICING_ATTR_IND,
        MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        SIZE_ATTR_IND AS SIZE_ATTR_IND,
        DEL_IND AS DEL_IND,
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_TAG AS SKU_PIM_ATTR_TYPE_TAG,
        SKU_PIM_ATTR_TYPE_NAME AS SKU_PIM_ATTR_TYPE_NAME,
        SKU_PIM_ATTR_TYPE_DISPLAY_NAME AS SKU_PIM_ATTR_TYPE_DISPLAY_NAME,
        SLICING_ATTR_IND1 AS SLICING_ATTR_IND1,
        MULTI_VAL_ASSIGN_IND1 AS MULTI_VAL_ASSIGN_IND1,
        INTERNAL_AUD_IND1 AS INTERNAL_AUD_IND1,
        SIZE_ATTR_IND1 AS SIZE_ATTR_IND1,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_SKU_PIM_Attr_Type_4 
    WHERE
        (
            NOT ISNULL(PIM_ATTR_ID) 
            AND ISNULL(SKU_PIM_ATTR_TYPE_ID) 
            AND ISNULL(DEL_IND)
        ) 
        OR (
            NOT ISNULL(PIM_ATTR_ID) 
            AND NOT ISNULL(SKU_PIM_ATTR_TYPE_ID) 
            AND (
                PIM_ATTR_TAG <> SKU_PIM_ATTR_TYPE_TAG 
                OR PIM_ATTR_NAME <> SKU_PIM_ATTR_TYPE_NAME 
                OR PIM_ATTR_DISPLAY_NAME <> SKU_PIM_ATTR_TYPE_DISPLAY_NAME 
                OR SLICING_ATTR_IND <> SLICING_ATTR_IND1 
                OR MULTI_VAL_ASSIGN_IND <> MULTI_VAL_ASSIGN_IND1 
                OR INTERNAL_AUD_IND <> INTERNAL_AUD_IND1 
                OR SIZE_ATTR_IND <> SIZE_ATTR_IND1 
                OR DEL_IND = 'X'
            )
        )""")

df_5.createOrReplaceTempView("Fil_SKU_PIM_Attr_Type_5")

# COMMAND ----------

# DBTITLE 1, Exp_SKU_PIM_Attr_Type_6

df_6=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_TAG AS PIM_ATTR_TAG,
        PIM_ATTR_NAME AS PIM_ATTR_NAME,
        PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
        SLICING_ATTR_IND AS SLICING_ATTR_IND,
        MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        SIZE_ATTR_IND AS SIZE_ATTR_IND,
        current_timestamp AS UPDATE_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        current_timestamp,
        LOAD_TSTMP) AS LOAD_TSTMP,
        IFF(ISNULL(LOAD_TSTMP),
        DD_INSERT,
        IFF(DEL_IND = 'X',
        DD_DELETE,
        DD_UPDATE)) AS LoadStrategyFlag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Fil_SKU_PIM_Attr_Type_5""")

df_6.createOrReplaceTempView("Exp_SKU_PIM_Attr_Type_6")

# COMMAND ----------

# DBTITLE 1, Ups_SKU_PIM_Attr_Type_7

df_7=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_TAG AS PIM_ATTR_TAG,
        PIM_ATTR_NAME AS PIM_ATTR_NAME,
        PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
        SLICING_ATTR_IND AS SLICING_ATTR_IND,
        MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
        INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
        SIZE_ATTR_IND AS SIZE_ATTR_IND,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        LoadStrategyFlag AS LoadStrategyFlag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_SKU_PIM_Attr_Type_6""")

df_7.createOrReplaceTempView("Ups_SKU_PIM_Attr_Type_7")

# COMMAND ----------

# DBTITLE 1, SKU_PIM_ATTR_TYPE

spark.sql("""INSERT INTO SKU_PIM_ATTR_TYPE SELECT PIM_ATTR_ID AS SKU_PIM_ATTR_TYPE_ID,
PIM_ATTR_TAG AS SKU_PIM_ATTR_TYPE_TAG,
PIM_ATTR_NAME AS SKU_PIM_ATTR_TYPE_NAME,
PIM_ATTR_DISPLAY_NAME AS SKU_PIM_ATTR_TYPE_DISPLAY_NAME,
SLICING_ATTR_IND AS SLICING_ATTR_IND,
MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
SIZE_ATTR_IND AS SIZE_ATTR_IND,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM Ups_SKU_PIM_Attr_Type_7""")
