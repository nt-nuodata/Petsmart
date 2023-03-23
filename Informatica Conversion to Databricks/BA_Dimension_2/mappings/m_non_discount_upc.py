# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, prm_415_FLAT_0


df_0=spark.sql("""
    SELECT
        UPC AS UPC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        prm_415_FLAT""")

df_0.createOrReplaceTempView("prm_415_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_prm_415_FLAT_1


df_1=spark.sql("""
    SELECT
        UPC_ID AS UPC_ID,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        prm_415_FLAT_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_prm_415_FLAT_1")

# COMMAND ----------
# DBTITLE 1, NON_DISCOUNT_UPC


spark.sql("""INSERT INTO NON_DISCOUNT_UPC SELECT UPC_ID AS UPC_ID FROM SQ_Shortcut_to_prm_415_FLAT_1""")