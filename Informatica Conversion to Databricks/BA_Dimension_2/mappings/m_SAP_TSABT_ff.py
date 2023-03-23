# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, TSABT_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        SPRAS AS SPRAS,
        ABTNR AS ABTNR,
        VTEXT AS VTEXT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        TSABT""")

df_0.createOrReplaceTempView("TSABT_0")
