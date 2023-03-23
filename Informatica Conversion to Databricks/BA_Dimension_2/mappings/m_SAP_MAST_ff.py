# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, MAST_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        MATNR AS MATNR,
        WERKS AS WERKS,
        STLAN AS STLAN,
        STLNR AS STLNR,
        STLAL AS STLAL,
        LOSVN AS LOSVN,
        LOSBS AS LOSBS,
        ANDAT AS ANDAT,
        ANNAM AS ANNAM,
        AEDAT AS AEDAT,
        AENAM AS AENAM,
        CSLTY AS CSLTY,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        MAST""")

df_0.createOrReplaceTempView("MAST_0")
