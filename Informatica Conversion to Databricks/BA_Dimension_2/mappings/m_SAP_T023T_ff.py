# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, T023T_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        SPRAS AS SPRAS,
        MATKL AS MATKL,
        WGBEZ AS WGBEZ,
        WGBEZ60 AS WGBEZ60,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T023T""")

df_0.createOrReplaceTempView("T023T_0")

# COMMAND ----------
# DBTITLE 1, T023T_ff
