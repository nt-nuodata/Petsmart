# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, SKUAttr_0

df_0=spark.sql("""
    SELECT
        SKUNbr AS SKUNbr,
        SKUAttrTypeID AS SKUAttrTypeID,
        SKUAttrTypeValueID AS SKUAttrTypeValueID,
        DelInd AS DelInd,
        UpdateUser AS UpdateUser,
        UpdateDt AS UpdateDt,
        LoadUser AS LoadUser,
        LoadDt AS LoadDt,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKUAttr""")

df_0.createOrReplaceTempView("SKUAttr_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SKUAttr_1

df_1=spark.sql("""
    SELECT
        SKUNbr AS SKUNbr,
        SKUAttrTypeID AS SKUAttrTypeID,
        SKUAttrTypeValueID AS SKUAttrTypeValueID,
        DelInd AS DelInd,
        UpdateUser AS UpdateUser,
        LoadUser AS LoadUser,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKUAttr_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKUAttr_1")

# COMMAND ----------

# DBTITLE 1, Exp_Load_Tstmp_2

df_2=spark.sql("""
    SELECT
        LoadUser AS LoadUser,
        current_timestamp AS LOAD_TSTMP,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_Shortcut_to_SKUAttr_1""")

df_2.createOrReplaceTempView("Exp_Load_Tstmp_2")

# COMMAND ----------

# DBTITLE 1, SKU_TXS_ATTR_PRE

spark.sql("""INSERT INTO SKU_TXS_ATTR_PRE SELECT SKU_NBR AS SKU_NBR,
SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
DEL_IND AS DEL_IND,
UPDATE_USER AS UPDATE_USER,
LoadUser AS LOAD_USER,
LOAD_TSTMP AS LOAD_TSTMP FROM Exp_Load_Tstmp_2""")

spark.sql("""INSERT INTO SKU_TXS_ATTR_PRE SELECT SKU_NBR AS SKU_NBR,
SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
DEL_IND AS DEL_IND,
UPDATE_USER AS UPDATE_USER,
LoadUser AS LOAD_USER,
LOAD_TSTMP AS LOAD_TSTMP FROM SQ_Shortcut_to_SKUAttr_1""")
