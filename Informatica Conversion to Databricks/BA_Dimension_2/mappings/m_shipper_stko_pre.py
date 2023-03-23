# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, STKO_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        STLTY AS STLTY,
        STLNR AS STLNR,
        STLAL AS STLAL,
        STKOZ AS STKOZ,
        DATUV AS DATUV,
        TECHV AS TECHV,
        AENNR AS AENNR,
        LKENZ AS LKENZ,
        LOEKZ AS LOEKZ,
        VGKZL AS VGKZL,
        ANDAT AS ANDAT,
        ANNAM AS ANNAM,
        AEDAT AS AEDAT,
        AENAM AS AENAM,
        BMEIN AS BMEIN,
        BMENG AS BMENG,
        CADKZ AS CADKZ,
        LABOR AS LABOR,
        LTXSP AS LTXSP,
        STKTX AS STKTX,
        STLST AS STLST,
        WRKAN AS WRKAN,
        DVDAT AS DVDAT,
        DVNAM AS DVNAM,
        AEHLP AS AEHLP,
        ALEKZ AS ALEKZ,
        GUIDX AS GUIDX,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        STKO""")

df_0.createOrReplaceTempView("STKO_0")

# COMMAND ----------

# DBTITLE 1, SQ_STKO_1

df_1=spark.sql("""
    SELECT
        STKO.STLTY,
        STKO.STLNR,
        STKO.STLAL,
        STKO.STKOZ,
        STKO.DATUV,
        STKO.ANDAT,
        STKO.ANNAM,
        STKO.ALEKZ,
        STKO.GUIDX 
    FROM
        SAPPR3.STKO""")

df_1.createOrReplaceTempView("SQ_STKO_1")

# COMMAND ----------

# DBTITLE 1, SHIPPER_STKO_PRE

spark.sql("""INSERT INTO SHIPPER_STKO_PRE SELECT STLTY AS BOM_CATEGORY_ID,
STLNR AS BILL_OF_MATERIAL,
STLAL AS ALTERNATIVE_BOM,
STKOZ AS INTERNAL_CNTR,
DATUV AS VALID_FROM_DT,
ANDAT AS RECORD_CREATE_DT,
ANNAM AS RECORD_CREATE_USER,
ALEKZ AS ALE_IND,
GUIDX AS BOM_HEADER_CHANGE_STATUS_ID FROM SQ_STKO_1""")
