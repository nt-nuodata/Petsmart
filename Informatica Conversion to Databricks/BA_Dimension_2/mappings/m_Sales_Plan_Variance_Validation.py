# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, BATCH_LOAD_AUD_LOG_0

df_0=spark.sql("""
    SELECT
        DAY_DT AS DAY_DT,
        BATCH_DATE AS BATCH_DATE,
        AOS AS AOS,
        ISPU AS ISPU,
        SFS AS SFS,
        STR AS STR,
        WEB AS WEB,
        STX_COUNT AS STX_COUNT,
        EDW_COUNT AS EDW_COUNT,
        EDW_SALES AS EDW_SALES,
        STX_SALES AS STX_SALES,
        PLAN_SALES AS PLAN_SALES,
        ACTUAL_SALES AS ACTUAL_SALES,
        SALES_VARIANCE AS SALES_VARIANCE,
        PLAN_VARIANCE AS PLAN_VARIANCE,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        BATCH_LOAD_AUD_LOG""")

df_0.createOrReplaceTempView("BATCH_LOAD_AUD_LOG_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_BATCH_LOAD_AUD_LOG_1

df_1=spark.sql("""select SALES_VARIANCE as VARIANCE, 1 as Title
FROM
 BATCH_LOAD_AUD_LOG 
WHERE
 BATCH_LOAD_AUD_LOG.BATCH_DATE= CURRENT_DATE
and (BATCH_LOAD_AUD_LOG.SALES_VARIANCE > $$Threshold_Variance_3 or BATCH_LOAD_AUD_LOG.SALES_VARIANCE < $$Threshold_Variance_4)
UNION
select PLAN_VARIANCE as VARIANCE, 2 as Title
FROM
 BATCH_LOAD_AUD_LOG 
WHERE
 BATCH_LOAD_AUD_LOG.BATCH_DATE= CURRENT_DATE
 AND  (BATCH_LOAD_AUD_LOG.PLAN_VARIANCE > $$Threshold_Variance_1 or BATCH_LOAD_AUD_LOG.PLAN_VARIANCE < $$Threshold_Variance_2)""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_BATCH_LOAD_AUD_LOG_1")

# COMMAND ----------

# DBTITLE 1, Variance_Validation

spark.sql("""INSERT INTO Variance_Validation SELECT SALES_VARIANCE AS VARIANCE,
PLAN_VARIANCE AS TITLE FROM SQ_Shortcut_to_BATCH_LOAD_AUD_LOG_1""")
