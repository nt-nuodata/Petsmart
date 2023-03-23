# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, TSKM_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        TATYP AS TATYP,
        TAXKM AS TAXKM,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TSKM""")

df_0.createOrReplaceTempView("TSKM_0")
