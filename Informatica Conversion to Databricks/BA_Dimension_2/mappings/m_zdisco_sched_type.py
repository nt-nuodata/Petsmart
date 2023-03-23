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

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_ZTB_DISCO_TIMES_1

df_1=spark.sql("""
    SELECT
        ZTB_DISCO_TIMES.SCHED_TYPE,
        ZTB_DISCO_TIMES.SCHED_CODE,
        ZTB_DISCO_TIMES.SCHED_DESCR 
    FROM
        SAPPR3.ZTB_DISCO_TIMES""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_DISCO_TIMES_1")

# COMMAND ----------

# DBTITLE 1, ZDISCO_SCHED_TYPE

spark.sql("""INSERT INTO ZDISCO_SCHED_TYPE SELECT SCHED_TYPE AS ZDISCO_SCHED_TYPE_ID,
SCHED_CODE AS ZDISCO_SCHED_TYPE_CD,
SCHED_DESCR AS ZDISCO_SCHED_TYPE_DESC FROM SQ_Shortcut_to_ZTB_DISCO_TIMES_1""")
