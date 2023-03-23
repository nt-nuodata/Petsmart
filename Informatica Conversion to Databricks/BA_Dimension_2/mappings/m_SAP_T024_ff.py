# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, T024_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        EKGRP AS EKGRP,
        EKNAM AS EKNAM,
        EKTEL AS EKTEL,
        LDEST AS LDEST,
        TELFX AS TELFX,
        ZZBNAME AS ZZBNAME,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T024""")

df_0.createOrReplaceTempView("T024_0")

# COMMAND ----------
# DBTITLE 1, T024_ff
