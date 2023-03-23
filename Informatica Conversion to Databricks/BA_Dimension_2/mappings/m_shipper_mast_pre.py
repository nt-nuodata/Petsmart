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

# COMMAND ----------
# DBTITLE 1, SQ_MAST_1


df_1=spark.sql("""
    SELECT
        MAST.MATNR,
        MAST.WERKS,
        MAST.STLAN,
        MAST.STLNR,
        MAST.STLAL,
        MAST.ANDAT,
        MAST.ANNAM 
    FROM
        SAPPR3.MAST""")

df_1.createOrReplaceTempView("SQ_MAST_1")

# COMMAND ----------
# DBTITLE 1, SHIPPER_MAST_PRE


spark.sql("""INSERT INTO SHIPPER_MAST_PRE SELECT MATNR AS ARTICLE_NBR,
WERKS AS SITE_NBR,
STLAN AS BOM_USAGE_IND,
STLNR AS BILL_OF_MATERIAL,
STLAL AS ALTERNATIVE_BOM,
ANDAT AS RECORD_CREATE_DT,
ANNAM AS RECORD_CREATE_USER FROM SQ_MAST_1""")