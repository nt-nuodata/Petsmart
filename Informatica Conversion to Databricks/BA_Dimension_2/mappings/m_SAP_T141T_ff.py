# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, T141T_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        SPRAS AS SPRAS,
        MMSTA AS MMSTA,
        MTSTB AS MTSTB,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T141T""")

df_0.createOrReplaceTempView("T141T_0")

# COMMAND ----------
# DBTITLE 1, T141T_ff
