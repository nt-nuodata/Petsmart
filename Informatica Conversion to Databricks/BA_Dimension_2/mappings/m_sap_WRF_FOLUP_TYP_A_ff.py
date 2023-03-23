# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, WRF_FOLUP_TYP_A_0


df_0=spark.sql("""
    SELECT
        MANDT AS MANDT,
        ORIGINAL_ART_NR AS ORIGINAL_ART_NR,
        FOLUP_ART_NR AS FOLUP_ART_NR,
        ASORT AS ASORT,
        FOLLOWUP_TYP_NR AS FOLLOWUP_TYP_NR,
        DATE_FROM AS DATE_FROM,
        DATE_TO AS DATE_TO,
        PRIORITY_A AS PRIORITY_A,
        FOLLOWUP_ACTION AS FOLLOWUP_ACTION,
        TIMESTAMP AS TIMESTAMP,
        CREATE_TIMESTAMP AS CREATE_TIMESTAMP,
        ERNAM AS ERNAM,
        AENAM AS AENAM,
        ORG_ART_FACTOR AS ORG_ART_FACTOR,
        SUBST_ART_FACTOR AS SUBST_ART_FACTOR,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        WRF_FOLUP_TYP_A""")

df_0.createOrReplaceTempView("WRF_FOLUP_TYP_A_0")

# COMMAND ----------
# DBTITLE 1, sap_WRF_FOLUP_TYP_A_ff
