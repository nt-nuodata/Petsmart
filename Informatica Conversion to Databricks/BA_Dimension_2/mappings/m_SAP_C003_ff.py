# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, C003_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        KAPPL AS KAPPL,
        KSCHL AS KSCHL,
        KTOPL AS KTOPL,
        VKORG AS VKORG,
        KTGRM AS KTGRM,
        KVSL1 AS KVSL1,
        SAKN1 AS SAKN1,
        SAKN2 AS SAKN2,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        C003""")

df_0.createOrReplaceTempView("C003_0")

# COMMAND ----------
# DBTITLE 1, FF_C003
