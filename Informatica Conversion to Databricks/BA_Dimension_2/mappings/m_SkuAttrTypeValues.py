# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, FF_sku_txs_attr_type_values_0

df_0=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        FF_sku_txs_attr_type_values""")

df_0.createOrReplaceTempView("FF_sku_txs_attr_type_values_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_FF_sku_txs_attr_type_values_1

df_1=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FF_sku_txs_attr_type_values_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_FF_sku_txs_attr_type_values_1")

# COMMAND ----------

# DBTITLE 1, Srt_Distinct_2

df_2=spark.sql("""
    SELECT
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_FF_sku_txs_attr_type_values_1 
    ORDER BY
        FF_SKU_TXS_ATTR_TYPE_ID ASC,
        FF_SKU_TXS_ATTR_TYPE_VALUE_DESC ASC""")

df_2.createOrReplaceTempView("Srt_Distinct_2")

# COMMAND ----------

# DBTITLE 1, Lkp_SkuAttrTypeValues_3

df_3=spark.sql("""
    SELECT
        SKUAttrTypeValueID AS SKUAttrTypeValueID,
        LoadDt AS LoadDt,
        Srt_Distinct_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKUAttrTypeValues 
    RIGHT OUTER JOIN
        Srt_Distinct_2 
            ON SKUAttrTypeValues.SKUAttrTypeID = Srt_Distinct_2.FF_SKU_TXS_ATTR_TYPE_ID 
            AND SKUAttrTypeValues.SKUAttrTypeValueDesc = FF_SKU_TXS_ATTR_TYPE_VALUE_DESC""")

df_3.createOrReplaceTempView("Lkp_SkuAttrTypeValues_3")

# COMMAND ----------

# DBTITLE 1, Fil_SkuAttrTypeValues_4

df_4=spark.sql("""
    SELECT
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKUAttrTypeValueID AS SKUAttrTypeValueID,
        LoadDt AS LoadDt,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Lkp_SkuAttrTypeValues_3 
    WHERE
        ISNULL(SKUAttrTypeValueID)""")

df_4.createOrReplaceTempView("Fil_SkuAttrTypeValues_4")

df_4=spark.sql("""
    SELECT
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
        SKUAttrTypeValueID AS SKUAttrTypeValueID,
        LoadDt AS LoadDt,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Srt_Distinct_2 
    WHERE
        ISNULL(SKUAttrTypeValueID)""")

df_4.createOrReplaceTempView("Fil_SkuAttrTypeValues_4")

# COMMAND ----------

# DBTITLE 1, Exp_SKUAttrTypeValues_5

df_5=spark.sql("""
    SELECT
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
        'ETL' AS LoadUser,
        current_timestamp AS UpdateDt,
        IFF(ISNULL(LoadDt),
        current_timestamp,
        LoadDt) AS LoadDt,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Fil_SkuAttrTypeValues_4""")

df_5.createOrReplaceTempView("Exp_SKUAttrTypeValues_5")

# COMMAND ----------

# DBTITLE 1, SKUAttrTypeValues

spark.sql("""INSERT INTO SKUAttrTypeValues SELECT FF_SKU_TXS_ATTR_TYPE_ID AS SKUAttrTypeID,
SKUAttrTypeValueID AS SKUAttrTypeValueID,
FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKUAttrTypeValueDesc,
DelInd AS DelInd,
UpdateUser AS UpdateUser,
UpdateDt AS UpdateDt,
LoadUser AS LoadUser,
LoadDt AS LoadDt FROM Exp_SKUAttrTypeValues_5""")
