# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ZTPIM_ART_ATTR_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        MATNR AS MATNR,
        ATTNUM AS ATTNUM,
        ATTVALNUM AS ATTVALNUM,
        DEL_IND AS DEL_IND,
        CHANGED_BY AS CHANGED_BY,
        CHANGED_ON AS CHANGED_ON,
        CHANGED_AT AS CHANGED_AT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTPIM_ART_ATTR""")

df_0.createOrReplaceTempView("ZTPIM_ART_ATTR_0")

# COMMAND ----------
# DBTITLE 1, SAP_ZTPIM_ART_ATTR_FF
