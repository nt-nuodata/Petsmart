# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, FF_sku_txs_attr_0

df_0=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
        FF_DEL_IND AS FF_DEL_IND,
        FF_USER AS FF_USER,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        FF_sku_txs_attr""")

df_0.createOrReplaceTempView("FF_sku_txs_attr_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_FF_sku_txs_attr_1

df_1=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
        FF_DEL_IND AS FF_DEL_IND,
        FF_USER AS FF_USER,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        FF_sku_txs_attr_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_FF_sku_txs_attr_1")

# COMMAND ----------

# DBTITLE 1, Fil_FF_Source_2

df_2=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
        FF_DEL_IND AS FF_DEL_IND,
        FF_USER AS FF_USER,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_FF_sku_txs_attr_1 
    WHERE
        NOT ISNULL(FF_SKU_NBR) 
        AND NOT ISNULL(FF_SKU_TXS_ATTR_TYPE_ID) 
        AND NOT ISNULL(FF_SKU_TXS_ATTR_TYPE_VALUE_ID) 
        AND NOT ISNULL(FF_DEL_IND)""")

df_2.createOrReplaceTempView("Fil_FF_Source_2")

# COMMAND ----------

# DBTITLE 1, Lkp_SkuAttrTypeValues_3

df_3=spark.sql("""
    SELECT
        SKUAttrTypeID AS SKUAttrTypeID,
        Fil_FF_Source_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKUAttrTypeValues 
    RIGHT OUTER JOIN
        Fil_FF_Source_2 
            ON SKUAttrTypeID = Fil_FF_Source_2.FF_SKU_TXS_ATTR_TYPE_ID 
            AND SKUAttrTypeValues.SKUAttrTypeValueID = FF_SKU_TXS_ATTR_TYPE_VALUE_ID""")

df_3.createOrReplaceTempView("Lkp_SkuAttrTypeValues_3")

# COMMAND ----------

# DBTITLE 1, Lkp_SKuAttr_4

df_4=spark.sql("""
    SELECT
        LoadDt AS LoadDt,
        LoadUser AS LoadUser,
        Fil_FF_Source_2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKUAttr 
    RIGHT OUTER JOIN
        Fil_FF_Source_2 
            ON SKUAttr.SKUNbr = Fil_FF_Source_2.FF_SKU_NBR 
            AND SKUAttr.SKUAttrTypeID = FF_SKU_TXS_ATTR_TYPE_ID""")

df_4.createOrReplaceTempView("Lkp_SKuAttr_4")

# COMMAND ----------

# DBTITLE 1, Fil_SkuAttrTypeValues_5

df_5=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
        LoadDt AS LoadDt,
        SKUAttrTypeID AS SKUAttrTypeID,
        FF_DEL_IND AS FF_DEL_IND,
        LoadUser AS LoadUser,
        FF_USER AS FF_USER,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Fil_FF_Source_2 
    WHERE
        NOT ISNULL(SKUAttrTypeID)""")

df_5.createOrReplaceTempView("Fil_SkuAttrTypeValues_5")

df_5=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
        LoadDt AS LoadDt,
        SKUAttrTypeID AS SKUAttrTypeID,
        FF_DEL_IND AS FF_DEL_IND,
        LoadUser AS LoadUser,
        FF_USER AS FF_USER,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Lkp_SKuAttr_4 
    WHERE
        NOT ISNULL(SKUAttrTypeID)""")

df_5.createOrReplaceTempView("Fil_SkuAttrTypeValues_5")

df_5=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
        LoadDt AS LoadDt,
        SKUAttrTypeID AS SKUAttrTypeID,
        FF_DEL_IND AS FF_DEL_IND,
        LoadUser AS LoadUser,
        FF_USER AS FF_USER,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Lkp_SkuAttrTypeValues_3 
    WHERE
        NOT ISNULL(SKUAttrTypeID)""")

df_5.createOrReplaceTempView("Fil_SkuAttrTypeValues_5")

# COMMAND ----------

# DBTITLE 1, Exp_SkuAttr_6

df_6=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
        FF_DEL_IND AS FF_DEL_IND,
        IFF(ISNULL(LoadDt),
        current_timestamp,
        LoadDt) AS LoadDt,
        current_timestamp AS UpdateDt,
        IFF(ISNULL(LoadUser),
        RTRIM(LTRIM(FF_USER)),
        LoadUser) AS LoadUser,
        IFF(ISNULL(LoadUser),
        'ETL',
        FF_USER) AS UpdateUser,
        IFF(ISNULL(LoadDt),
        dd_insert,
        dd_update) AS UpdateStrategy,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Fil_SkuAttrTypeValues_5""")

df_6.createOrReplaceTempView("Exp_SkuAttr_6")

# COMMAND ----------

# DBTITLE 1, Ups_SkuAttr_7

df_7=spark.sql("""
    SELECT
        FF_SKU_NBR AS FF_SKU_NBR,
        FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
        FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
        FF_DEL_IND AS DelInd,
        LoadDt AS LoadDt,
        UpdateDt AS UpdateDt,
        LoadUser AS LoadUser,
        UpdateStrategy AS UpdateStrategy,
        UpdateUser AS UpdateUser,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        Exp_SkuAttr_6""")

df_7.createOrReplaceTempView("Ups_SkuAttr_7")

# COMMAND ----------

# DBTITLE 1, SKUAttr

spark.sql("""INSERT INTO SKUAttr SELECT FF_SKU_NBR AS SKUNbr,
FF_SKU_TXS_ATTR_TYPE_ID AS SKUAttrTypeID,
FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS SKUAttrTypeValueID,
DelInd AS DelInd,
UpdateUser AS UpdateUser,
UpdateDt AS UpdateDt,
LoadUser AS LoadUser,
LoadDt AS LoadDt FROM Ups_SkuAttr_7""")
