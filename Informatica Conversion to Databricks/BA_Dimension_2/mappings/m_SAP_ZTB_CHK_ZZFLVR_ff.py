# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ZTB_CHK_ZZFLVR_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        VALUE AS VALUE,
        DESCR AS DESCR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_CHK_ZZFLVR""")

df_0.createOrReplaceTempView("ZTB_CHK_ZZFLVR_0")
