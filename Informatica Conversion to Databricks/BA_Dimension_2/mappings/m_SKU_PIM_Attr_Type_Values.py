# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR_TYPE_VALUES_PRE_0


df_0=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
        ENTRY_POSITION AS ENTRY_POSITION,
        DEL_IND AS DEL_IND,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_TYPE_VALUES_PRE""")

df_0.createOrReplaceTempView("SKU_PIM_ATTR_TYPE_VALUES_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_1


df_1=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
        ENTRY_POSITION AS ENTRY_POSITION,
        DEL_IND AS DEL_IND,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_TYPE_VALUES_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_1")

# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR_TYPE_VALUES_2


df_2=spark.sql("""
    SELECT
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
        SKU_PIM_ATTR_VALUE_DESC AS SKU_PIM_ATTR_VALUE_DESC,
        ENTRY_POSITION AS ENTRY_POSITION,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_TYPE_VALUES""")

df_2.createOrReplaceTempView("SKU_PIM_ATTR_TYPE_VALUES_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_3


df_3=spark.sql("""
    SELECT
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
        SKU_PIM_ATTR_VALUE_DESC AS SKU_PIM_ATTR_VALUE_DESC,
        ENTRY_POSITION AS ENTRY_POSITION,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_PIM_ATTR_TYPE_VALUES_2""")

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_3")

# COMMAND ----------
# DBTITLE 1, Jnr_SKU_PIM_Attr_Type_Values_4


df_4=spark.sql("""
    SELECT
        DETAIL.PIM_ATTR_ID AS PIM_ATTR_ID,
        DETAIL.PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        DETAIL.PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
        DETAIL.ENTRY_POSITION AS ENTRY_POSITION,
        DETAIL.DEL_IND AS DEL_IND,
        MASTER.SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        MASTER.SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
        MASTER.SKU_PIM_ATTR_VALUE_DESC AS SKU_PIM_ATTR_VALUE_DESC,
        MASTER.ENTRY_POSITION AS ENTRY_POSITION1,
        MASTER.LOAD_TSTMP AS LOAD_TSTMP,
        MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_3 MASTER 
    LEFT JOIN
        SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_1 DETAIL 
            ON MASTER.SKU_PIM_ATTR_TYPE_ID = PIM_ATTR_ID 
            AND SKU_PIM_ATTR_TYPE_VALUE_ID = DETAIL.PIM_ATTR_VAL_ID""")

df_4.createOrReplaceTempView("Jnr_SKU_PIM_Attr_Type_Values_4")

# COMMAND ----------
# DBTITLE 1, Fil_SKU_PIM_Attr_Type_Values_5


df_5=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
        ENTRY_POSITION AS ENTRY_POSITION,
        DEL_IND AS DEL_IND,
        SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
        SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
        SKU_PIM_ATTR_VALUE_DESC AS SKU_PIM_ATTR_VALUE_DESC,
        ENTRY_POSITION1 AS ENTRY_POSITION1,
        LOAD_TSTMP AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Jnr_SKU_PIM_Attr_Type_Values_4 
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
                PIM_ATTR_VAL_DESC <> SKU_PIM_ATTR_VALUE_DESC 
                OR ENTRY_POSITION <> ENTRY_POSITION1 
                OR DEL_IND = 'X'
            )
        )""")

df_5.createOrReplaceTempView("Fil_SKU_PIM_Attr_Type_Values_5")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_6


df_6=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
        ENTRY_POSITION AS ENTRY_POSITION,
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
        Fil_SKU_PIM_Attr_Type_Values_5""")

df_6.createOrReplaceTempView("EXPTRANS_6")

# COMMAND ----------
# DBTITLE 1, Ups_SKU_PIM_Attr_Type_Values_7


df_7=spark.sql("""
    SELECT
        PIM_ATTR_ID AS PIM_ATTR_ID,
        PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
        PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
        ENTRY_POSITION AS ENTRY_POSITION,
        UPDATE_TSTMP AS UPDATE_TSTMP,
        LOAD_TSTMP AS LOAD_TSTMP,
        LoadStrategyFlag AS LoadStrategyFlag,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        EXPTRANS_6""")

df_7.createOrReplaceTempView("Ups_SKU_PIM_Attr_Type_Values_7")

# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR_TYPE_VALUES


spark.sql("""INSERT INTO SKU_PIM_ATTR_TYPE_VALUES SELECT PIM_ATTR_ID AS SKU_PIM_ATTR_TYPE_ID,
PIM_ATTR_VAL_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
PIM_ATTR_VAL_DESC AS SKU_PIM_ATTR_VALUE_DESC,
ENTRY_POSITION AS ENTRY_POSITION,
UPDATE_TSTMP AS UPDATE_TSTMP,
LOAD_TSTMP AS LOAD_TSTMP FROM Ups_SKU_PIM_Attr_Type_Values_7""")