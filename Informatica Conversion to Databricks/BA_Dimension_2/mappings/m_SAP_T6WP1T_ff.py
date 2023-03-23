# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, T6WP1T_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        BWVOR AS BWVOR,
        SPRAS AS SPRAS,
        VTEXT AS VTEXT,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        T6WP1T""")

df_0.createOrReplaceTempView("T6WP1T_0")

# COMMAND ----------
# DBTITLE 1, T6WP1T_ff
