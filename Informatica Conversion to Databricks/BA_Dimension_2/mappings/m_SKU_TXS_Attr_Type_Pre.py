# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKUAttrType_0


df_0=spark.sql("""
    SELECT
        SKUAttrTypeID AS SKUAttrTypeID,
        SKUAttrTypeDesc AS SKUAttrTypeDesc,
        SKUAttrOwner AS SKUAttrOwner,
        DelInd AS DelInd,
        UpdateUser AS UpdateUser,
        UpdateDt AS UpdateDt,
        LoadUser AS LoadUser,
        LoadDt AS LoadDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKUAttrType""")

df_0.createOrReplaceTempView("SKUAttrType_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKUAttrType_1


df_1=spark.sql("""
    SELECT
        SKUAttrTypeID AS SKUAttrTypeID,
        SKUAttrTypeDesc AS SKUAttrTypeDesc,
        SKUAttrOwner AS SKUAttrOwner,
        DelInd AS DelInd,
        UpdateUser AS UpdateUser,
        LoadUser AS LoadUser,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKUAttrType_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKUAttrType_1")

# COMMAND ----------
# DBTITLE 1, Exp_Load_Tstmp_2


df_2=spark.sql("""
    SELECT
        LoadUser AS LoadUser,
        current_timestamp AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKUAttrType_1""")

df_2.createOrReplaceTempView("Exp_Load_Tstmp_2")

# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR_TYPE_PRE


spark.sql("""INSERT INTO SKU_TXS_ATTR_TYPE_PRE SELECT SKUAttrTypeID AS SKU_TXS_ATTR_TYPE_ID,
SKUAttrTypeDesc AS SKU_TXS_ATTR_TYPE_DESC,
SKUAttrOwner AS SKU_TXS_ATTR_OWNER,
DelInd AS DEL_IND,
UpdateUser AS UPDATE_USER,
LOAD_USER AS LOAD_USER,
current_timestamp() AS LOAD_TSTMP FROM SQ_Shortcut_to_SKUAttrType_1""")

spark.sql("""INSERT INTO SKU_TXS_ATTR_TYPE_PRE SELECT SKUAttrTypeID AS SKU_TXS_ATTR_TYPE_ID,
SKUAttrTypeDesc AS SKU_TXS_ATTR_TYPE_DESC,
SKUAttrOwner AS SKU_TXS_ATTR_OWNER,
DelInd AS DEL_IND,
UpdateUser AS UPDATE_USER,
LOAD_USER AS LOAD_USER,
current_timestamp() AS LOAD_TSTMP FROM Exp_Load_Tstmp_2""")