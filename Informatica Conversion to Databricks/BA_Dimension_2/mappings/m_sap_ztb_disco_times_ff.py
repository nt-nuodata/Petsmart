# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ZTB_DISCO_TIMES_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        SCHED_TYPE AS SCHED_TYPE,
        SCHED_CODE AS SCHED_CODE,
        SCHED_DESCR AS SCHED_DESCR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_DISCO_TIMES""")

df_0.createOrReplaceTempView("ZTB_DISCO_TIMES_0")
