# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, ZTB_DISCO_MD_0

df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        ZZMD_SCH_ID AS ZZMD_SCH_ID,
        MD_SCH_DESC AS MD_SCH_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ZTB_DISCO_MD""")

df_0.createOrReplaceTempView("ZTB_DISCO_MD_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_ZTB_DISCO_MD_1

df_1=spark.sql("""
    SELECT
        ZTB_DISCO_MD.ZZMD_SCH_ID,
        ZTB_DISCO_MD.MD_SCH_DESC 
    FROM
        SAPPR3.ZTB_DISCO_MD 
    WHERE
        ZTB_DISCO_MD.MANDT = '100'""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_DISCO_MD_1")

# COMMAND ----------

# DBTITLE 1, ZDISCO_MKDN_SCHED

spark.sql("""INSERT INTO ZDISCO_MKDN_SCHED SELECT ZZMD_SCH_ID AS ZDISCO_MKDN_SCHED_ID,
MD_SCH_DESC AS ZDISCO_MKDN_SCHED_DESC FROM SQ_Shortcut_to_ZTB_DISCO_MD_1""")
