# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ZTB_CHK_ZZRTV_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        VALUE AS VALUE,
        DESCR AS DESCR,
        HAZ_MAT AS HAZ_MAT,
        AEROSOL AS AEROSOL,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_CHK_ZZRTV""")

df_0.createOrReplaceTempView("ZTB_CHK_ZZRTV_0")
