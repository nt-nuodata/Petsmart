# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, SKU_HAZMAT_FLAT_0


df_0=spark.sql("""
    SELECT
        RTV_DEPT_CD AS RTV_DEPT_CD,
        RTV_DESC AS RTV_DESC,
        HAZ_FLAG AS HAZ_FLAG,
        AEROSOL_FLAG AS AEROSOL_FLAG,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        SKU_HAZMAT_FLAT""")

df_0.createOrReplaceTempView("SKU_HAZMAT_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_HAZMAT_FLAT_1


df_1=spark.sql("""
    SELECT
        RTV_DEPT_CD AS RTV_DEPT_CD,
        RTV_DESC AS RTV_DESC,
        HAZ_FLAG AS HAZ_FLAG,
        AEROSOL_FLAG AS AEROSOL_FLAG,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SKU_HAZMAT_FLAT_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_HAZMAT_FLAT_1")

# COMMAND ----------
# DBTITLE 1, SKU_HAZMAT_PRE


spark.sql("""INSERT INTO SKU_HAZMAT_PRE SELECT RTV_DEPT_CD AS RTV_DEPT_CD,
RTV_DESC AS RTV_DESC,
HAZ_FLAG AS HAZ_FLAG,
AEROSOL_FLAG AS AEROSOL_FLAG FROM SQ_Shortcut_to_SKU_HAZMAT_FLAT_1""")