# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, ATT_TYPES_0


df_0=spark.sql("""
    SELECT
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC,
        monotonically_increasing_id() AS Monotonically_Increasing_Id 
    FROM
        ATT_TYPES""")

df_0.createOrReplaceTempView("ATT_TYPES_0")

# COMMAND ----------
# DBTITLE 1, SQ_ATT_TYPES_1


df_1=spark.sql("""
    SELECT
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        ATT_TYPES_0""")

df_1.createOrReplaceTempView("SQ_ATT_TYPES_1")

# COMMAND ----------
# DBTITLE 1, EXP_SAP_ATT_TYPE_2


df_2=spark.sql("""
    SELECT
        SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
        RTRIM(SAP_ATT_TYPE_DESC) AS OUT_SAP_ATT_TYPE_DESC,
        Monotonically_Increasing_Id AS Monotonically_Increasing_Id 
    FROM
        SQ_ATT_TYPES_1""")

df_2.createOrReplaceTempView("EXP_SAP_ATT_TYPE_2")

# COMMAND ----------
# DBTITLE 1, SAP_ATT_TYPE_PRE


spark.sql("""INSERT INTO SAP_ATT_TYPE_PRE SELECT SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
OUT_SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC FROM EXP_SAP_ATT_TYPE_2""")